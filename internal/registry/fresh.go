package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/cache"
)

// freshnessProbeTimeout 是一次「latest 谁更新」探测中，请求方愿意等待的预算。
// 探测本身顺带把 manifest 下载下来了，命中时可以直接返回，不再二次回源。
// 给 800ms 是权衡：正常镜像源 200~400ms 就能返回，足够拿到几个样本；
// 更慢的源（例如挂了代理的官方源）超时后不影响本次选源，探测转到后台继续跑。
const freshnessProbeTimeout = 800 * time.Millisecond

// freshnessProbeMax 是后台探测的硬上限。
// 慢源要 1~3 秒才能返回，如果整段探测都卡在 800ms 上，这类源永远拿不到样本，
// 也就永远无法在 latest 上胜出。超时后让探测继续跑完并写入缓存，
// 后续十分钟内的请求就能用上完整的新旧信息。
const freshnessProbeMax = 6 * time.Second

// maxConfigBlobBytes 是读取镜像配置（用于取 created 时间）的上限。
// 配置里主要是构建历史，正常几十 KB，给 2MB 已经很宽裕。
const maxConfigBlobBytes = 2 << 20

// freshnessEntry 是某个「仓库@标签」在新旧探测上的缓存。
type freshnessEntry struct {
	at     time.Time
	values map[string]time.Time
}

// freshnessCache 按 key 缓存探测结果。
type freshnessCache struct {
	ttl time.Duration

	mu       sync.Mutex
	entries  map[string]*freshnessEntry
	inflight map[string]bool // 正在探测的 key，避免并发重复探测
}

func newFreshnessCache(ttl time.Duration) *freshnessCache {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &freshnessCache{ttl: ttl, entries: make(map[string]*freshnessEntry), inflight: make(map[string]bool)}
}

// claim 尝试认领某个仓库的探测权：没有在跑的探测时返回 true。
func (c *freshnessCache) claim(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight[key] {
		return false
	}
	c.inflight[key] = true
	return true
}

// release 释放探测权。
func (c *freshnessCache) release(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, key)
}

// peek 返回仍在有效期内的缓存。
func (c *freshnessCache) peek(key string) (map[string]time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Since(e.at) >= c.ttl {
		return nil, false
	}
	return e.values, true
}

func (c *freshnessCache) put(key string, values map[string]time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 仓库数量没有上限，缓存跟着无限长没有意义；超过阈值就整体重置。
	if len(c.entries) > 4096 {
		c.entries = make(map[string]*freshnessEntry, 64)
	}
	c.entries[key] = &freshnessEntry{at: time.Now(), values: values}
}

// probeFreshnessAsync 在后台执行新旧探测，请求最多等 freshnessProbeTimeout。
//
// 超时后这一次先按原策略选源，探测继续跑完并把结果写入缓存，
// 这样慢源（例如挂了代理的官方源）也能在后续请求中参与新旧比较。
//
// 并发去重：同一个仓库的探测同时只允许一个在跑（claimFreshness），
// 其余请求立即返回 nil 走原顺序选源，避免缓存过期的瞬间触发请求风暴。
func (p *Pool) probeFreshnessAsync(ctx context.Context, key, repo, ref, accept string) (map[string]time.Time, map[string]*cache.Manifest) {
	if !p.fresh.claim(key) {
		// 已有同仓库的探测在跑：这次请求直接放弃探测，本次按速度选源。
		// 结果马上会进缓存，下一个请求就能用上。
		return nil, nil
	}
	// 客户端断开也要把探测跑完，否则慢源的结果永远进不了缓存。
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), freshnessProbeMax)

	var (
		mu        sync.Mutex
		values    map[string]time.Time
		manifests map[string]*cache.Manifest
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer p.fresh.release(key)
		v, m := p.probeFreshness(bg, repo, ref, accept)
		if bg.Err() != nil {
			cancel()
			return // 没跑完的半截结果不写缓存
		}
		mu.Lock()
		values, manifests = v, m
		mu.Unlock()
		// v == nil 表示各源 digest 一致（有效结论）；空 map 表示全部上游
		// 都失败了——后者不写缓存，下次请求会重新探测而不是拿 10 分钟的
		// 空结论把探测完全屏蔽掉。
		if v == nil || len(v) > 0 {
			p.fresh.put(key, v)
		}
		cancel()
	}()

	select {
	case <-done:
		mu.Lock()
		defer mu.Unlock()
		return values, manifests
	case <-time.After(freshnessProbeTimeout):
		if p.log != nil {
			p.log.Debug("latest 新旧探测超过预算，转后台继续", "repo", repo, "ref", ref)
		}
		return nil, nil
	case <-ctx.Done():
		return nil, nil
	}
}

// CandidatesForManifest 返回针对具体 <repo>:<ref> 请求排序后的候选上游，
// 以及探测阶段已经下载好的 manifest（键为上游名，可能为空）。
//
// 对 latest 这类可变标签，先比较各源上镜像的构建时间：更新的源排在前面，
// 构建时间一样（或只有它一个源有数据）时沿用 Candidates 的顺序。
// 这样即使某个源速度最快，只要它的 latest 已经落后好几天，也不会被选中。
//
// 探测时下载的顶层 manifest 会一并返回：它的 body 正是客户端要的内容，
// 选中之后可以直接返回，省掉一次重复回源。
func (p *Pool) CandidatesForManifest(ctx context.Context, repo, ref, accept string) ([]*Upstream, map[string]*cache.Manifest) {
	list := p.Candidates()
	if p.fresh == nil || !p.preferFresh || IsDigest(ref) || !p.freshTag(ref) || len(list) < 2 {
		return list, nil
	}

	key := repo + "@" + strings.ToLower(ref)
	values, cached := p.fresh.peek(key)
	var manifests map[string]*cache.Manifest
	if !cached {
		values, manifests = p.probeFreshnessAsync(ctx, key, repo, ref, accept)
	}

	known := 0
	for _, u := range list {
		if !values[u.Name].IsZero() {
			known++
		}
	}
	// 少于两个源有样本时没有可比性，保持原顺序。
	if known < 2 {
		if p.log != nil {
			p.log.Debug("latest 新旧样本不足，按原策略选源", "repo", repo, "ref", ref, "samples", known)
		}
		return list, manifests
	}

	base := make(map[*Upstream]int, len(list))
	for i, u := range list {
		base[u] = i
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		at, aok := values[a.Name]
		bt, bok := values[b.Name]
		aok = aok && !at.IsZero()
		bok = bok && !bt.IsZero()
		switch {
		case aok && bok:
			if at.Equal(bt) {
				return base[a] < base[b]
			}
			if p.freshTolerance > 0 && absDuration(at.Sub(bt)) < p.freshTolerance {
				// 差异小于容差，认为一样新，继续按速度/优先级选。
				return base[a] < base[b]
			}
			return at.After(bt)
		case aok != bok:
			// 探测失败的源视为未知，排在已知的后面。
			return aok
		default:
			return base[a] < base[b]
		}
	})
	if p.log != nil {
		p.log.Debug("latest 新旧比较完成", "repo", repo, "ref", ref, "samples", known,
			"chosen", list[0].Name, "created", values[list[0].Name].Format(time.RFC3339))
	}
	return list, manifests
}

// freshTag 判断该标签是否需要参与新旧比较。
func (p *Pool) freshTag(ref string) bool {
	ref = strings.ToLower(ref)
	for _, tag := range p.freshTags {
		if tag == ref {
			return true
		}
	}
	return false
}

// probeOutcome 是单个上游的新旧探测结果。
type probeOutcome struct {
	topDigest string
	cfgDigest string
	manifest  *cache.Manifest
	err       error
}

// probeFreshness 并发读取各上游上同一标签的清单与配置，解析出构建时间。
//
// 先比清单 digest：所有源完全一致时说明内容本来就一样，直接跳过配置下载。
// 不一致时才按 config digest 去重，每个不同的镜像只取一次配置。
//
// 这里不设超时，由调用方决定等多久。
func (p *Pool) probeFreshness(ctx context.Context, repo, ref, accept string) (map[string]time.Time, map[string]*cache.Manifest) {
	if strings.TrimSpace(accept) == "" {
		accept = DefaultManifestAccept
	}

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		top      = make(map[string]string)
		config   = make(map[string]string)
		manifest = make(map[string]*cache.Manifest)
	)
	for _, u := range p.upstreams {
		if !u.Healthy() {
			continue
		}
		wg.Add(1)
		go func(u *Upstream) {
			defer wg.Done()
			out := p.probeManifest(ctx, u, repo, ref, accept)
			if out.err != nil {
				return
			}
			mu.Lock()
			top[u.Name] = out.topDigest
			config[u.Name] = out.cfgDigest
			manifest[u.Name] = out.manifest
			mu.Unlock()
		}(u)
	}
	wg.Wait()

	// 一个上游都没探测成功：不是「内容一致」也不是「样本不足」，而是一次
	// 彻底失败的探测。返回非 nil 的空 map 让调用方跳过写缓存，
	// 下次请求重新探测（与 nil=内容一致 区分开）。
	if len(top) == 0 {
		return map[string]time.Time{}, manifest
	}

	// 所有清单一致（或只有一个样本）时没有新旧差异。
	var first string
	same := true
	for _, d := range top {
		if first == "" {
			first = d
			continue
		}
		if d != first {
			same = false
			break
		}
	}
	if same {
		return nil, manifest
	}

	// 同一个 config digest 只取一次，取到的时间对所有持有它的上游都成立。
	type configTask struct {
		upstream *Upstream
		digest   string
	}
	tasks := make(map[string]configTask, len(config))
	for name, digest := range config {
		if _, ok := tasks[digest]; ok {
			continue
		}
		u := p.ByName(name)
		if u == nil {
			continue
		}
		tasks[digest] = configTask{upstream: u, digest: digest}
	}

	created := make(map[string]time.Time, len(tasks))
	var cfgWG sync.WaitGroup
	for _, task := range tasks {
		task := task
		cfgWG.Add(1)
		go func() {
			defer cfgWG.Done()
			t, err := p.fetchConfigCreated(ctx, task.upstream, repo, task.digest)
			if err != nil {
				return
			}
			mu.Lock()
			created[task.digest] = t
			mu.Unlock()
		}()
	}
	cfgWG.Wait()

	values := make(map[string]time.Time, len(config))
	for name, digest := range config {
		if t, ok := created[digest]; ok && !t.IsZero() {
			values[name] = t
		}
	}
	return values, manifest
}

// probeManifest 返回顶层清单 digest、config digest 与可直接返回的 manifest。
func (p *Pool) probeManifest(ctx context.Context, u *Upstream, repo, ref, accept string) probeOutcome {
	header := http.Header{}
	header.Set("Accept", accept)
	scope := "repository:" + repo + ":pull"

	body, mediaType, err := p.fetchUpstreamManifest(ctx, u, repo, ref, header, scope)
	if err != nil {
		return probeOutcome{err: err}
	}
	sum := sha256.Sum256(body)
	topDigest := "sha256:" + hex.EncodeToString(sum[:])
	if mediaType == "" {
		mediaType = GuessManifestMediaType(body)
	}
	manifest := &cache.Manifest{Body: body, Digest: topDigest, MediaType: mediaType, Mutable: true}

	var mf probeManifest
	if err := json.Unmarshal(body, &mf); err != nil {
		return probeOutcome{err: fmt.Errorf("解析 manifest 失败: %w", err)}
	}
	if len(mf.Manifests) > 0 {
		sub := pickPlatformDigest(mf.Manifests)
		if sub == "" {
			return probeOutcome{err: fmt.Errorf("manifest list 中没有可用的平台")}
		}
		subBody, _, err := p.fetchUpstreamManifest(ctx, u, repo, sub, header, scope)
		if err != nil {
			return probeOutcome{err: err}
		}
		mf = probeManifest{}
		if err := json.Unmarshal(subBody, &mf); err != nil {
			return probeOutcome{err: fmt.Errorf("解析子 manifest 失败: %w", err)}
		}
	}
	if mf.Config.Digest == "" {
		return probeOutcome{err: fmt.Errorf("manifest 缺少 config digest")}
	}
	return probeOutcome{topDigest: topDigest, cfgDigest: mf.Config.Digest, manifest: manifest}
}

// fetchConfigCreated 读取镜像配置里的 created 时间。
func (p *Pool) fetchConfigCreated(ctx context.Context, u *Upstream, repo, digest string) (time.Time, error) {
	resp, err := u.Do(ctx, http.MethodGet, blobPath(repo, digest), nil, "repository:"+repo+":pull")
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
		return time.Time{}, fmt.Errorf("读取 config 返回 %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxConfigBlobBytes))
	if err != nil {
		return time.Time{}, err
	}
	var cfg struct {
		Created string `json:"created"`
		History []struct {
			Created string `json:"created"`
		} `json:"history"`
	}
	if err := json.Unmarshal(body, &cfg); err != nil {
		return time.Time{}, fmt.Errorf("解析 config 失败: %w", err)
	}
	if t := parseCreated(cfg.Created); !t.IsZero() {
		return t, nil
	}
	// 少数构建工具把顶层 created 清零，退到历史里最后一个非零时间。
	for i := len(cfg.History) - 1; i >= 0; i-- {
		if t := parseCreated(cfg.History[i].Created); !t.IsZero() {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("config 中没有有效的 created 时间")
}

// parseCreated 解析 created 字段，并过滤零值（0001-01-01）。
func parseCreated(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() < 2000 {
		return time.Time{}
	}
	return t
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
