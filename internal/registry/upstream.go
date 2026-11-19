package registry

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

const userAgent = "docker-mirror-gateway/1.0"

// 吞吐样本的门槛与平滑参数。
const (
	// 太小的样本主要反映握手和调度开销，噪声大到没有参考价值。
	// 这里只卡字节数、不卡耗时：本地回环传 256KB 只要几百微秒，
	// 用时间门槛会把真正快的源误判成噪声。
	minSpeedSampleBytes = 64 << 10
	// 吞吐 EWMA 中新样本的权重：约 3 个样本就能跟上新的速度水平，
	// 又不至于被单次网络抖动带偏。
	speedAlpha = 0.35
)

// Upstream 表示一个可以被聚合的上游镜像源。
type Upstream struct {
	Name     string
	Group    string
	BaseURL  string
	Priority int

	username    string
	password    string
	passthrough bool
	headers     map[string]string
	proxy       string
	client      *http.Client
	tokenClient *http.Client
	tokens      *tokenStore
	log         *slog.Logger

	failThreshold int
	reqTotal      atomic.Int64
	errTotal      atomic.Int64
	bytesServed   atomic.Int64

	mReq   *metrics.Counter
	mErr   *metrics.Counter
	mBytes *metrics.Counter

	mu          sync.RWMutex
	healthy     bool
	failures    int
	latencyMS   float64
	speedBPS    float64   // 下载吞吐的 EWMA（字节/秒），0 表示还没有样本
	speedN      int       // 已经累计的吞吐样本数
	speedUpAt   time.Time // 最后一个吞吐样本的时间
	speedTry    time.Time // 最后一次吞吐探测的尝试时间（成功与否都算）
	lastErr     string
	lastCheck   time.Time
	lastOK      time.Time
	lastTraffic time.Time // 最近一次真实业务流量（pull/tags）成功的时间
}

// Status 是对外暴露的上游状态快照。
type Status struct {
	Name        string     `json:"name"`
	Group       string     `json:"group"`
	URL         string     `json:"url"`
	Priority    int        `json:"priority"`
	Proxy       string     `json:"proxy"`
	Rank        int        `json:"rank"`
	Healthy     bool       `json:"healthy"`
	LatencyMS   float64    `json:"latencyMs"`
	SpeedBPS    float64    `json:"speedBps"`
	SpeedSample int        `json:"speedSamples"`
	Failures    int        `json:"consecutiveFailures"`
	LastError   string     `json:"lastError,omitempty"`
	LastChecked *time.Time `json:"lastChecked,omitempty"`
	LastOK      *time.Time `json:"lastOk,omitempty"`
	LastTraffic *time.Time `json:"lastTraffic,omitempty"`
	LastSpeedAt *time.Time `json:"lastSpeedAt,omitempty"`
	Requests    int64      `json:"requests"`
	Errors      int64      `json:"errors"`
	BytesServed int64      `json:"bytesServed"`
}

// NewUpstream 根据配置构建上游。
func NewUpstream(cfg config.UpstreamConfig, group string, failThreshold int, log *slog.Logger, m *metrics.Collector) (*Upstream, error) {
	if cfg.Name == "" || cfg.URL == "" {
		return nil, fmt.Errorf("upstream 名称与地址不能为空")
	}
	if log == nil {
		log = slog.Default()
	}
	timeout := cfg.Timeout.D()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	// 代理凭据可以写在地址里，也可以拆成独立字段，这里统一合并。
	proxyURL, err := config.EffectiveProxy(cfg.Proxy, cfg.ProxyUsername, cfg.ProxyPassword)
	if err != nil {
		return nil, fmt.Errorf("upstream %q 的代理配置无效: %w", cfg.Name, err)
	}
	proxyFunc, proxyLabel, err := buildProxy(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("upstream %q 的代理配置无效: %w", cfg.Name, err)
	}

	transport := &http.Transport{
		Proxy: proxyFunc,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
		// registry 的 blob 本身已是压缩层，禁止 Go 自动加 Accept-Encoding 并解压，
		// 否则内容不再是原始字节，digest 校验会失败。
		DisableCompression:    true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: timeout,
	}

	u := &Upstream{
		Name:          cfg.Name,
		Group:         group,
		BaseURL:       cfg.URL,
		Priority:      cfg.Priority,
		username:      cfg.Username,
		password:      cfg.Password,
		passthrough:   cfg.Passthrough(),
		headers:       cfg.Headers,
		proxy:         proxyLabel,
		client:        &http.Client{Transport: transport},
		tokenClient:   &http.Client{Transport: transport.Clone(), Timeout: 20 * time.Second},
		tokens:        newTokenStore(),
		log:           log.With("group", group, "upstream", cfg.Name),
		failThreshold: failThreshold,
		healthy:       true, // 启动时乐观假设可用，由健康探测纠正
	}
	if u.failThreshold <= 0 {
		u.failThreshold = 3
	}
	if m != nil {
		labels := map[string]string{"group": group, "upstream": cfg.Name}
		u.mReq = m.Counter("mirror_gateway_upstream_requests_total", "转发到上游的请求数", labels)
		u.mErr = m.Counter("mirror_gateway_upstream_errors_total", "上游请求失败数", labels)
		u.mBytes = m.Counter("mirror_gateway_upstream_bytes_total", "从上游读取的字节数", labels)
		m.GaugeFunc("mirror_gateway_upstream_healthy", "上游健康状态（1=健康）", labels, func() float64 {
			if u.Healthy() {
				return 1
			}
			return 0
		})
		m.GaugeFunc("mirror_gateway_upstream_latency_ms", "上游探测延迟（毫秒）", labels, u.LatencyMS)
		m.GaugeFunc("mirror_gateway_upstream_speed_bytes_per_second", "上游下载吞吐 EWMA（字节/秒，0=暂无样本）", labels, u.SpeedBPS)
		m.GaugeFunc("mirror_gateway_upstream_priority", "上游优先级（越小越优先）", labels, func() float64 {
			return float64(u.Priority)
		})
	}
	return u, nil
}

// Do 向上游发起请求，并在遇到 401 时自动完成 Bearer/Basic 认证后重试一次。
// scope 形如 repository:library/nginx:pull。
func (u *Upstream) Do(ctx context.Context, method, path string, header http.Header, scope string) (*http.Response, error) {
	resp, _, err := u.DoAuth(ctx, method, path, header, scope, "")
	if err != nil {
		return nil, err
	}
	if resp == nil {
		// 401 且带挑战的出口只发生在需要透传的场景；Do 的调用方不需要。
		return nil, fmt.Errorf("%w: %s 需要认证", ErrUnauthorized, u.Name)
	}
	return resp, nil
}

// Passthrough 报告该源的认证方式是否为客户端凭证透传。
func (u *Upstream) Passthrough() bool { return u.passthrough }

// DoAuth 在 Do 的基础上支持按源认证透传。
//
// clientAuth 是客户端原始 Authorization 头（可能为空），仅当该源配置
// auth=passthrough 时才使用；否则忽略它，按本源配置的账号自动认证。
//
//   - auth=passthrough：客户端凭证原样发给上游，不查网关令牌缓存、不用
//     配置账号；上游返回 401 且带挑战时返回 (nil, challenge, nil)，由上层
//     改写 realm 后交给客户端（docker 自己去 /token 换令牌）。
//   - auth=config（默认）：使用本源 username/password 自动完成 Bearer/Basic
//     认证；认证失败返回 err 并把原始挑战放在 challenge 里，不返回给客户端。
//
// challenge 非空时 resp 一定为 nil 且 body 已关闭。
func (u *Upstream) DoAuth(ctx context.Context, method, path string, header http.Header, scope, clientAuth string) (*http.Response, string, error) {
	u.reqTotal.Add(1)
	u.mReq.Inc()

	attempt := func(authorization string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, u.BaseURL+path, nil)
		if err != nil {
			return nil, err
		}
		for key, values := range header {
			for _, v := range values {
				req.Header.Add(key, v)
			}
		}
		for key, value := range u.headers {
			req.Header.Set(key, value)
		}
		req.Header.Set("User-Agent", userAgent)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		return u.client.Do(req)
	}
	// 读取 401 响应并返回原始挑战头。
	readChallenge := func(resp *http.Response) string {
		ch := resp.Header.Get("WWW-Authenticate")
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return ch
	}

	// 该源配置为透传：即使客户端未登录也走匿名请求，绝不回退到配置账号。
	if u.passthrough {
		resp, err := attempt(clientAuth)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode == http.StatusUnauthorized {
			ch := readChallenge(resp)
			if ch != "" {
				return nil, ch, nil
			}
			return nil, "", fmt.Errorf("%w: %s 返回 401 且未提供可用的认证挑战", ErrUnauthorized, u.Name)
		}
		return resp, "", nil
	}

	token := u.tokens.get(scope)
	auth := ""
	if token != "" {
		auth = "Bearer " + token
	} else if u.username != "" {
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(u.username+":"+u.password))
	}
	resp, err := attempt(auth)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, "", nil
	}

	rawChallenge := readChallenge(resp)
	ch := parseChallenge(rawChallenge)
	u.tokens.invalidate(scope)

	switch ch.Scheme {
	case "bearer":
		tokenStart := time.Now()
		newToken, ttl, err := u.fetchToken(ctx, ch, scope)
		u.log.Debug("换取上游令牌", "elapsed", time.Since(tokenStart).Round(time.Millisecond).String(), "err", err)
		if err != nil {
			// 配置账号换取失败：这是仓库级认证失败，交给上层按 denied 处理。
			return nil, rawChallenge, err
		}
		u.tokens.put(scope, newToken, ttl)
		resp, err := attempt("Bearer " + newToken)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode == http.StatusUnauthorized {
			ch2 := readChallenge(resp)
			if ch2 == "" {
				ch2 = rawChallenge
			}
			// 刚换的令牌被上游拒绝：清掉缓存，避免同 scope 的后续请求
			// 每次都先用坏令牌撞一次 401 再重新换取。
			u.tokens.invalidate(scope)
			return nil, ch2, fmt.Errorf("%w: %s 拒绝了配置账号换取的令牌", ErrUnauthorized, u.Name)
		}
		return resp, "", nil
	case "basic":
		if u.username == "" {
			return nil, rawChallenge, fmt.Errorf("%w: %s 需要 Basic 认证，但未配置账号", ErrUnauthorized, u.Name)
		}
		resp, err := attempt("Basic " + base64.StdEncoding.EncodeToString([]byte(u.username+":"+u.password)))
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode == http.StatusUnauthorized {
			ch2 := readChallenge(resp)
			if ch2 == "" {
				ch2 = rawChallenge
			}
			return nil, ch2, fmt.Errorf("%w: %s 拒绝了配置账号", ErrUnauthorized, u.Name)
		}
		return resp, "", nil
	default:
		return nil, rawChallenge, fmt.Errorf("%w: %s 返回 401 且未提供可用的认证挑战", ErrUnauthorized, u.Name)
	}
}

// Challenge 请求上游 /v2/ 并解析认证挑战。令牌代理（ServeToken）用它获得
// 上游真实的 realm 与 service，确保客户端凭证只发给该上游自己的令牌服务。
func (u *Upstream) Challenge(ctx context.Context) (challenge, error) {
	u.reqTotal.Add(1)
	u.mReq.Inc()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.BaseURL+"/v2/", nil)
	if err != nil {
		return challenge{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return challenge{}, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusOK {
		// 该源无需认证，客户端凭证无处可用。
		return challenge{}, fmt.Errorf("%w: %s 无需认证", ErrUnauthorized, u.Name)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return challenge{}, fmt.Errorf("%w: %s 返回 %d", ErrUnauthorized, u.Name, resp.StatusCode)
	}
	ch := parseChallenge(resp.Header.Get("WWW-Authenticate"))
	if ch.Realm == "" {
		return challenge{}, fmt.Errorf("%w: %s 认证挑战缺少 realm", ErrUnauthorized, u.Name)
	}
	return ch, nil
}

// ForwardToken 带客户端凭证请求上游令牌接口，返回原始 JSON 响应体。
// 与 fetchToken 的区别：认证用透传的客户端凭证而非网关配置账号，且结果
// 不进网关令牌缓存（令牌属于客户端，由客户端自己缓存与续期）。
func (u *Upstream) ForwardToken(ctx context.Context, ch challenge, scope, clientAuth string) ([]byte, error) {
	if ch.Realm == "" {
		return nil, fmt.Errorf("%w: 缺少认证 realm", ErrUnauthorized)
	}
	q := url.Values{}
	q.Set("client_id", "docker-mirror-gateway")
	if ch.Service != "" {
		q.Set("service", ch.Service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	realm := ch.Realm
	if strings.Contains(realm, "?") {
		realm += "&" + q.Encode()
	} else {
		realm += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if clientAuth != "" {
		req.Header.Set("Authorization", clientAuth)
	}
	resp, err := u.tokenClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求令牌失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: 令牌接口返回 %d: %s", ErrUnauthorized, resp.StatusCode, snippet(body))
	}
	return body, nil
}

// Ping 探测 /v2/ 端点。200 和 401 都说明服务在线。
func (u *Upstream) Ping(ctx context.Context) error {
	u.reqTotal.Add(1)
	u.mReq.Inc()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.BaseURL+"/v2/", nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	if u.username != "" {
		req.SetBasicAuth(u.username, u.password)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized {
		return nil
	}
	return fmt.Errorf("GET /v2/ 返回 %d", resp.StatusCode)
}

// ProbeManifest 通过 HEAD 一个真实镜像来验证认证与拉取链路。
func (u *Upstream) ProbeManifest(ctx context.Context, repo, ref string) error {
	header := http.Header{}
	header.Set("Accept", MediaTypeDockerManifestList+", "+MediaTypeOCIIndex+", "+MediaTypeDockerManifest)
	resp, err := u.Do(ctx, http.MethodHead, manifestPath(repo, ref), header, "repository:"+repo+":pull")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNotFound, http.StatusMethodNotAllowed:
		// 404 说明该镜像不存在，但服务本身是通的。
		return nil
	}
	return fmt.Errorf("HEAD %s 返回 %d", manifestPath(repo, ref), resp.StatusCode)
}

// ReportSuccess 记录一次成功（主动探测或真实流量的中间请求）。
func (u *Upstream) ReportSuccess(latency time.Duration) {
	u.report(latency, false)
}

// ReportTraffic 记录一次真实业务流量成功（客户端的 manifest/blob/tags 请求）。
//
// 与主动探测的成功同等对待——真实流量证明链路是通的，同样能清失败计数、
// 校准延迟；额外记下时间戳，供健康探测循环判断「最近有真实拉取，不必再探测」。
func (u *Upstream) ReportTraffic(latency time.Duration) {
	u.report(latency, true)
}

func (u *Upstream) report(latency time.Duration, traffic bool) {
	now := time.Now()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failures = 0
	u.healthy = true
	u.lastOK = now
	u.lastCheck = now
	if traffic {
		u.lastTraffic = now
	}
	if latency > 0 {
		ms := float64(latency.Microseconds()) / 1000.0
		if u.latencyMS <= 0 {
			u.latencyMS = ms
		} else {
			u.latencyMS = u.latencyMS*0.7 + ms*0.3
		}
	}
}

// RecentTraffic 报告该上游最近 window 内是否处理过成功的真实业务流量。
func (u *Upstream) RecentTraffic(window time.Duration) bool {
	if window <= 0 {
		return false
	}
	u.mu.RLock()
	defer u.mu.RUnlock()
	return !u.lastTraffic.IsZero() && time.Since(u.lastTraffic) < window
}

// MarkProbeSkipped 记录一轮被真实流量替代的健康检查。
//
// 只刷新「最后检查」时间，不碰失败计数与健康状态——窗口内真实流量的
// 成败已经在维护它们，这里若清零 failures 会掩盖正在累积的上游故障。
func (u *Upstream) MarkProbeSkipped() {
	u.mu.Lock()
	u.lastCheck = time.Now()
	u.mu.Unlock()
}

// ReportFailure 记录一次失败，连续失败达到阈值后标记为不可用。
func (u *Upstream) ReportFailure(err error) {
	if err == nil {
		return
	}
	u.errTotal.Add(1)
	u.mErr.Inc()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failures++
	u.lastErr = err.Error()
	u.lastCheck = time.Now()
	if u.failures >= u.failThreshold {
		u.healthy = false
	}
}

// AddBytes 记录从该上游读取的字节数。
func (u *Upstream) AddBytes(n int64) {
	if n <= 0 {
		return
	}
	u.bytesServed.Add(n)
	u.mBytes.Add(n)
}

// ReportSpeed 记录一次下载吞吐样本：bytes 字节一共用了 dur。
//
// 样本太小（握手开销占比太高）时直接丢弃。真实 blob 传输和主动探测都走这里。
func (u *Upstream) ReportSpeed(bytes int64, dur time.Duration) {
	if bytes < minSpeedSampleBytes || dur <= 0 {
		return
	}
	bps := float64(bytes) / dur.Seconds()
	if bps <= 0 || math.IsInf(bps, 0) || math.IsNaN(bps) {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.speedBPS <= 0 {
		u.speedBPS = bps
	} else {
		u.speedBPS = u.speedBPS*(1-speedAlpha) + bps*speedAlpha
	}
	u.speedN++
	u.speedUpAt = time.Now()
}

// Healthy 返回当前健康状态。
func (u *Upstream) Healthy() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.healthy
}

// LatencyMS 返回延迟的指数移动平均（毫秒）。
func (u *Upstream) LatencyMS() float64 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.latencyMS
}

// SpeedBPS 返回下载吞吐的指数移动平均（字节/秒）；0 表示还没有样本。
func (u *Upstream) SpeedBPS() float64 {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.speedBPS
}

// SpeedSample 返回吞吐样本数与最后一个样本的时间。
func (u *Upstream) SpeedSample() (int, time.Time) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.speedN, u.speedUpAt
}

// NeedSpeedProbe 判断是否该给这个上游补一个吞吐样本。
//
// 两个条件同时满足才测：样本已经过期（或还没有），且距上次「尝试」已经过了
// 一个 interval。用尝试时间而不是成功时间来节流，避免探测失败时每轮健康
// 检查都重试一遍。
func (u *Upstream) NeedSpeedProbe(interval time.Duration) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.speedN > 0 && time.Since(u.speedUpAt) < interval {
		return false
	}
	return time.Since(u.speedTry) >= interval
}

// MarkSpeedProbe 记录一次吞吐探测尝试（无论成功与否）。
func (u *Upstream) MarkSpeedProbe() {
	u.mu.Lock()
	u.speedTry = time.Now()
	u.mu.Unlock()
}

// Status 返回状态快照。
func (u *Upstream) Status() Status {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return Status{
		Name:        u.Name,
		Group:       u.Group,
		URL:         u.BaseURL,
		Priority:    u.Priority,
		Proxy:       u.proxy,
		Healthy:     u.healthy,
		LatencyMS:   round1(u.latencyMS),
		SpeedBPS:    math.Round(u.speedBPS),
		SpeedSample: u.speedN,
		Failures:    u.failures,
		LastError:   u.lastErr,
		LastChecked: timePtr(u.lastCheck),
		LastOK:      timePtr(u.lastOK),
		LastTraffic: timePtr(u.lastTraffic),
		LastSpeedAt: timePtr(u.speedUpAt),
		Requests:    u.reqTotal.Load(),
		Errors:      u.errTotal.Load(),
		BytesServed: u.bytesServed.Load(),
	}
}

// buildProxy 按配置生成 Transport 的代理函数。
//
// 返回的第二个值是用于日志/状态展示的脱敏描述（env / direct / 代理地址）。
func buildProxy(spec string) (func(*http.Request) (*url.URL, error), string, error) {
	switch spec {
	case "":
		return http.ProxyFromEnvironment, "env", nil
	case config.ProxyDirect:
		return nil, config.ProxyDirect, nil
	}
	u, err := url.Parse(spec)
	if err != nil {
		return nil, "", err
	}
	if u.Host == "" {
		return nil, "", fmt.Errorf("缺少主机名和端口")
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, "", fmt.Errorf("不支持的代理协议 %q", u.Scheme)
	}
	return http.ProxyURL(u), config.MaskProxy(spec), nil
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func round1(v float64) float64 {
	return float64(int64(v*10+0.5)) / 10
}

// HTTPClient 返回该上游使用的 HTTP 客户端，包含它自己的代理与超时设置。
// 供搜索等外部请求复用同一条出口链路。
func (u *Upstream) HTTPClient() *http.Client { return u.client }

// HasExplicitProxy 表示该上游显式配置了代理地址，
// 而不是沿用环境变量（env）或强制直连（direct）。
func (u *Upstream) HasExplicitProxy() bool {
	return u.proxy != "" && u.proxy != "env" && u.proxy != config.ProxyDirect
}
