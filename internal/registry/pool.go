package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

// speedLatencySlack 是「吞吐接近」时才开始比较延迟的门槛。
// 大镜像的耗时几乎全在传输上，几百毫秒的握手差异不值得为它换源。
const speedLatencySlack = 400.0 // 毫秒

// speedProbeTimeout 是一次吞吐探测的总预算。超过它的源基本可以认为
// 慢到没有使用价值，超时后仍会用已经读到的字节记一个偏低的样本。
const speedProbeTimeout = 10 * time.Second

// unknownLatencyMS 代表「还没有延迟样本」。取值远大于任何真实延迟，
// 这样未探测过的源不会因为 latency=0 而在延迟比较里白占便宜。
const unknownLatencyMS = 1e9

// latencyFor 返回用于比较的延迟；还没样本时给一个极大值。
func latencyFor(u *Upstream) float64 {
	if v := u.LatencyMS(); v > 0 {
		return v
	}
	return unknownLatencyMS
}

// Pool 管理一组互为镜像的上游，并负责健康探测与排序。
type Pool struct {
	upstreams     []*Upstream
	strategy      string
	health        config.HealthConfig
	library       string
	probeRepo     string
	probeTag      string
	group         string
	log           *slog.Logger
	trafficWindow time.Duration

	speedProbe     int64
	speedInterval  time.Duration
	speedTolerance float64

	// latest 等可变标签的新旧比较。
	preferFresh    bool
	freshTags      []string
	freshTolerance time.Duration
	fresh          *freshnessCache

	// 分组监控通知：健康镜像源跌破阈值时回调（由 Router 注入 notify.Manager）。
	notifyEnabled    bool
	notifyMinHealthy int
	notifyFn         func(group string, healthy, total, threshold int, failing []string)
}

// NewPool 构建上游池。
func NewPool(group config.GroupConfig, health config.HealthConfig, sel config.SelectionConfig, log *slog.Logger, m *metrics.Collector) (*Pool, error) {
	if log == nil {
		log = slog.Default()
	}
	p := &Pool{
		strategy:      sel.Strategy,
		health:        health,
		library:       group.LibraryNamespace,
		probeRepo:     NormalizeRepositoryName(group.ProbeRepo, group.LibraryNamespace),
		probeTag:      group.ProbeTag,
		group:         group.Name,
		log:           log,
		trafficWindow: health.TrafficWindow.D(),

		speedProbe:     sel.SpeedProbeBytes,
		speedInterval:  sel.SpeedProbeInterval.D(),
		speedTolerance: sel.SpeedTolerance,

		preferFresh:    sel.PreferFresh,
		freshTags:      sel.FreshTags,
		freshTolerance: sel.FreshnessTolerance.D(),

		notifyEnabled:    group.Notify.Enabled,
		notifyMinHealthy: group.Notify.MinHealthy,
	}
	if p.preferFresh {
		p.fresh = newFreshnessCache(sel.FreshnessTTL.D())
	}
	if len(p.freshTags) == 0 {
		p.freshTags = []string{"latest"}
	}
	if p.strategy == "" {
		p.strategy = "speed"
	}
	if p.probeTag == "" {
		p.probeTag = "latest"
	}
	if p.speedProbe <= 0 {
		p.speedProbe = 256 << 10
	}
	if p.speedInterval <= 0 {
		p.speedInterval = 30 * time.Minute
	}
	if p.speedTolerance <= 0 {
		p.speedTolerance = 0.25
	}
	for _, cfg := range group.Upstreams {
		u, err := NewUpstream(cfg, group.Name, health.FailThreshold, log, m)
		if err != nil {
			return nil, err
		}
		p.upstreams = append(p.upstreams, u)
	}
	if m != nil {
		m.GaugeFunc("mirror_gateway_upstreams_healthy", "健康的上游数量", map[string]string{"group": group.Name}, func() float64 {
			n := 0
			for _, u := range p.upstreams {
				if u.Healthy() {
					n++
				}
			}
			return float64(n)
		})
	}
	return p, nil
}

// HasHealthy 报告池中是否至少有一个健康上游。
func (p *Pool) HasHealthy() bool {
	for _, u := range p.upstreams {
		if u.Healthy() {
			return true
		}
	}
	return false
}

// Upstreams 返回配置顺序的上游列表。
func (p *Pool) Upstreams() []*Upstream {
	out := make([]*Upstream, len(p.upstreams))
	copy(out, p.upstreams)
	return out
}

// ByName 按名称查找上游。
func (p *Pool) ByName(name string) *Upstream {
	for _, u := range p.upstreams {
		if u.Name == name {
			return u
		}
	}
	return nil
}

// Candidates 返回按当前策略排序后的候选上游。
//
// 排序规则：
//  1. 健康的上游永远排在失效上游之前（失效源只作为最后兜底）。
//  2. speed 策略（默认）：吞吐高者优先。吞吐相差在 speedTolerance 以内视为
//     持平，此时才用延迟区分——而且延迟要差出 400ms 以上才算数，因为大镜像
//     的耗时几乎全在传输上，几百毫秒的握手差异可以忽略。仍持平时比 priority。
//  3. priority 策略：先比 priority（数值小者优先），再比实测延迟。
//  4. latency 策略：先比实测延迟（差值小于 5ms 视为持平），再比 priority。
//
// 还没有吞吐样本的上游排在已有样本的后面——主动探测会保证每个上游在
// speedProbeInterval 内至少产出一个样本，所以慢源不会被永久压在底部。
func (p *Pool) Candidates() []*Upstream {
	list := p.Upstreams()
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		ah, bh := a.Healthy(), b.Healthy()
		if ah != bh {
			return ah
		}
		if !ah {
			return a.Priority < b.Priority
		}
		switch p.strategy {
		case "latency":
			al, bl := latencyFor(a), latencyFor(b)
			if math.Abs(al-bl) > 5 {
				return al < bl
			}
			return a.Priority < b.Priority
		case "priority":
			if a.Priority != b.Priority {
				return a.Priority < b.Priority
			}
			return latencyFor(a) < latencyFor(b)
		default: // speed
			as, bs := a.SpeedBPS(), b.SpeedBPS()
			if as <= 0 || bs <= 0 {
				// 只有一边有数据时，有数据的优先；都没有则退回 priority。
				if as <= 0 && bs <= 0 {
					if a.Priority != b.Priority {
						return a.Priority < b.Priority
					}
					return latencyFor(a) < latencyFor(b)
				}
				return bs <= 0
			}
			hi, lo := math.Max(as, bs), math.Min(as, bs)
			if (hi-lo)/hi > p.speedTolerance {
				return as > bs
			}
			// 吞吐接近，再看延迟（差异要显著）。
			al, bl := latencyFor(a), latencyFor(b)
			if math.Abs(al-bl) > speedLatencySlack {
				return al < bl
			}
			if a.Priority != b.Priority {
				return a.Priority < b.Priority
			}
			return as > bs
		}
	})
	return list
}

// Statuses 返回带排名的上游状态列表。
func (p *Pool) Statuses() []Status {
	cands := p.Candidates()
	out := make([]Status, 0, len(cands))
	for i, u := range cands {
		st := u.Status()
		st.Rank = i + 1
		out = append(out, st)
	}
	return out
}

// Start 启动后台健康监测，直到 ctx 取消。
func (p *Pool) Start(ctx context.Context) {
	interval := p.health.Interval.D()
	if interval <= 0 {
		interval = 60 * time.Second
	}
	p.log.Info("健康监测已启动", "interval", interval.String(), "upstreams", len(p.upstreams), "strategy", p.strategy)
	p.ProbeAll(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.ProbeAll(ctx)
		}
	}
}

// ProbeAll 并发探测全部上游。
//
// 窗口内处理过真实业务流量的上游会被跳过：真实 pull 已经测出了延迟、
// 喂了吞吐样本、并由成功/失败在维护健康状态，再发探测请求纯属重复开销。
// 全部探测结束后评估一次分组监控通知。
func (p *Pool) ProbeAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, u := range p.upstreams {
		wg.Add(1)
		go func(u *Upstream) {
			defer wg.Done()
			before := u.Healthy()
			err := p.probeScheduled(ctx, u)
			after := u.Healthy()
			if before != after {
				if after {
					p.log.Info("上游恢复可用", "upstream", u.Name, "latencyMs", u.LatencyMS())
				} else {
					p.log.Warn("上游已被标记为不可用", "upstream", u.Name, "error", err)
				}
			}
		}(u)
	}
	wg.Wait()
	p.evaluateNotify()
}

// SetNotifier 注入分组监控通知的回调（由 Router 统一接线 notify.Manager）。
func (p *Pool) SetNotifier(fn func(group string, healthy, total, threshold int, failing []string)) {
	p.notifyFn = fn
}

// evaluateNotify 在每轮探测结束后检查健康镜像源数量是否跌破阈值。
// 具体发不发、多久发一次由 notify.Manager 的状态机决定，这里只如实上报。
func (p *Pool) evaluateNotify() {
	if p.notifyFn == nil || !p.notifyEnabled || p.notifyMinHealthy <= 0 {
		return
	}
	healthy := 0
	var failing []string
	for _, u := range p.upstreams {
		st := u.Status()
		if st.Healthy {
			healthy++
			continue
		}
		desc := st.Name
		if st.LastError != "" {
			desc += "（" + st.LastError + "）"
		}
		failing = append(failing, desc)
	}
	p.notifyFn(p.group, healthy, len(p.upstreams), p.notifyMinHealthy, failing)
}

// probeScheduled 是定时健康检查的入口，尊重 traffic window 的跳过逻辑。
func (p *Pool) probeScheduled(ctx context.Context, u *Upstream) error {
	if u.RecentTraffic(p.trafficWindow) {
		u.MarkProbeSkipped()
		p.log.Debug("上游最近有真实拉取，本轮跳过主动探测", "upstream", u.Name)
		return nil
	}
	return p.Probe(ctx, u)
}

// Probe 探测单个上游：先探 /v2/，再按配置 HEAD 一个探针镜像。
//
// 无论最近是否有真实流量都会执行——控制台的「探测」按钮走这里，
// 用户主动要求时必须给出当前的真实结果，而不是拿窗口内的旧数据搪塞。
func (p *Pool) Probe(ctx context.Context, u *Upstream) error {
	timeout := p.health.Timeout.D()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	if err := u.Ping(probeCtx); err != nil {
		u.ReportFailure(err)
		p.log.Debug("上游探测失败", "upstream", u.Name, "error", err)
		return err
	}
	if p.probeRepo != "" {
		repo := p.probeRepo
		ref := p.probeTag
		if ref == "" {
			ref = "latest"
		}
		if err := u.ProbeManifest(probeCtx, repo, ref); err != nil {
			u.ReportFailure(err)
			p.log.Debug("上游镜像探测失败", "upstream", u.Name, "repo", repo, "error", err)
			return err
		}
	}
	u.ReportSuccess(time.Since(start))
	p.log.Debug("上游探测成功", "upstream", u.Name, "latencyMs", u.LatencyMS())

	// 吞吐探测比健康探测重得多，单独节流。
	if p.strategy == "speed" {
		p.maybeProbeSpeed(ctx, u)
	}
	return nil
}

// maybeProbeSpeed 按 speedProbeInterval 节流地给上游补一个吞吐样本。
//
// 真实拉取也会喂数据，这里主要是让「一直没被选中」的源也能定期刷新，
// 否则慢源会被永远压在底部，配置里的调整也没有机会生效。
func (p *Pool) maybeProbeSpeed(parent context.Context, u *Upstream) {
	if p.probeRepo == "" {
		return
	}
	if !u.NeedSpeedProbe(p.speedInterval) {
		return
	}
	u.MarkSpeedProbe()
	// 单独给一个比健康探测更长的预算，但仍然要设上限：遇到极慢的源
	// （几百字节/秒）不能让一个探测连接挂在那里占着 goroutine 和连接。
	ctx, cancel := context.WithTimeout(parent, speedProbeTimeout)
	defer cancel()
	p.probeSpeed(ctx, u)
}

// probeSpeed 下载探针镜像的一段 layer 来估算吞吐。
func (p *Pool) probeSpeed(ctx context.Context, u *Upstream) {
	digest, size, err := p.probeLayer(ctx, u)
	if err != nil || digest == "" {
		p.log.Debug("吞吐探测跳过：拿不到 layer", "upstream", u.Name, "error", err)
		return
	}
	n := p.speedProbe
	if size > 0 && size < n {
		n = size
	}
	if n < minSpeedSampleBytes {
		// 层太小，测出来全是握手开销。
		return
	}

	header := http.Header{}
	header.Set("Range", fmt.Sprintf("bytes=0-%d", n-1))
	resp, err := u.Do(ctx, http.MethodGet, blobPath(p.probeRepo, digest), header, "repository:"+p.probeRepo+":pull")
	if err != nil {
		p.log.Debug("吞吐探测失败", "upstream", u.Name, "error", err)
		return
	}
	defer resp.Body.Close()
	// 不支持 Range 的源会回 200，这里读够 n 字节就停，剩下的丢掉。
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return
	}

	// 从拿到响应头之后才开始计时：建连、TLS、TTFB 属于延迟，不应该混进吞吐。
	start := time.Now()
	written, err := io.Copy(io.Discard, io.LimitReader(resp.Body, n))
	elapsed := time.Since(start)
	if err != nil && written < minSpeedSampleBytes {
		return
	}
	u.ReportSpeed(written, elapsed)
	p.log.Debug("吞吐探测完成", "upstream", u.Name, "bytes", written,
		"elapsed", elapsed.Round(time.Millisecond).String(), "kbPerSec", int(float64(written)/elapsed.Seconds()/1024))
}

// probeManifest 只解析吞吐探测需要的字段。
type probeManifest struct {
	Manifests []struct {
		Digest   string `json:"digest"`
		Platform struct {
			Architecture string `json:"architecture"`
			OS           string `json:"os"`
		} `json:"platform"`
	} `json:"manifests"`
	Layers []struct {
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	} `json:"layers"`
	Config struct {
		Digest string `json:"digest"`
	} `json:"config"`
}

// probeLayer 取探针镜像里最大的一个 layer，返回它的 digest 和大小。
// 取最大的层是因为小层测不出真实带宽。
func (p *Pool) probeLayer(ctx context.Context, u *Upstream) (string, int64, error) {
	header := http.Header{}
	header.Set("Accept", MediaTypeDockerManifestList+", "+MediaTypeOCIIndex+", "+MediaTypeDockerManifest+", "+MediaTypeOCIManifest)
	scope := "repository:" + p.probeRepo + ":pull"

	body, _, err := p.fetchUpstreamManifest(ctx, u, p.probeRepo, p.probeTag, header, scope)
	if err != nil {
		return "", 0, err
	}
	var mf probeManifest
	if err := json.Unmarshal(body, &mf); err != nil {
		return "", 0, fmt.Errorf("解析 manifest 失败: %w", err)
	}

	// 是 manifest list / OCI index 就下钻到具体平台。
	if len(mf.Manifests) > 0 {
		sub := pickPlatformDigest(mf.Manifests)
		if sub == "" {
			return "", 0, nil
		}
		body, _, err = p.fetchUpstreamManifest(ctx, u, p.probeRepo, sub, header, scope)
		if err != nil {
			return "", 0, err
		}
		mf = probeManifest{}
		if err := json.Unmarshal(body, &mf); err != nil {
			return "", 0, fmt.Errorf("解析子 manifest 失败: %w", err)
		}
	}

	var bestDigest string
	var bestSize int64 = -1
	for _, l := range mf.Layers {
		if l.Size > bestSize {
			bestDigest, bestSize = l.Digest, l.Size
		}
	}
	if bestSize < 0 {
		bestSize = 0
	}
	return bestDigest, bestSize, nil
}

func (p *Pool) fetchUpstreamManifest(ctx context.Context, u *Upstream, repo, ref string, header http.Header, scope string) ([]byte, string, error) {
	resp, err := u.Do(ctx, http.MethodGet, manifestPath(repo, ref), header, scope)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("manifest %s 返回 %d", ref, resp.StatusCode)
	}
	mediaType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	if !strings.HasPrefix(mediaType, "application/vnd.") {
		mediaType = GuessManifestMediaType(body)
	}
	return body, mediaType, nil
}

// pickPlatformDigest 优先挑 linux/amd64，否则退到第一个。
func pickPlatformDigest(items []struct {
	Digest   string `json:"digest"`
	Platform struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
	} `json:"platform"`
}) string {
	for _, m := range items {
		if m.Platform.Architecture == "amd64" && (m.Platform.OS == "" || m.Platform.OS == "linux") {
			return m.Digest
		}
	}
	if len(items) > 0 {
		return items[0].Digest
	}
	return ""
}
