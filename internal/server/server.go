// Package server 提供 HTTP 路由、管理接口和控制台页面。
package server

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/access"
	"github.com/midaug/docker-mirror-gateway/internal/activity"
	"github.com/midaug/docker-mirror-gateway/internal/cache"
	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
	"github.com/midaug/docker-mirror-gateway/internal/registry"
	"github.com/midaug/docker-mirror-gateway/internal/search"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Server 聚合所有 HTTP 处理逻辑。
type Server struct {
	cfg     *config.Config
	router  *registry.Router
	proxy   *registry.Proxy
	cache   cache.Store
	access  *access.Control
	search  *search.Service
	log     *slog.Logger
	metrics *metrics.Collector
	started time.Time
	version string

	reqTotal    *metrics.Counter
	deniedTotal *metrics.Counter

	// counterMu/counters 缓存「kind+method+status」对应的计数器。
	// 每个请求都要 Inc 一次，若每次都走 Collector.Counter 会产生
	// label map 分配、排序与写锁竞争；基数有限（状态码类别），缓存后
	// 热路径只剩一次短锁内的 map 查找。
	counterMu sync.Mutex
	counters  map[string]*metrics.Counter
}

// New 创建 Server。
// Options 汇总 New 所需的依赖。
type Options struct {
	Config  *config.Config
	Router  *registry.Router
	Proxy   *registry.Proxy
	Cache   cache.Store
	Search  *search.Service
	Logger  *slog.Logger
	Metrics *metrics.Collector
	Version string
}

func New(opts Options) (*Server, error) {
	cfg := opts.Config
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	c := opts.Cache
	m := opts.Metrics
	ctl, err := access.New(cfg.Access.Allow, cfg.Access.Deny, cfg.Access.TrustForwardedFor)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:      cfg,
		router:   opts.Router,
		proxy:    opts.Proxy,
		cache:    c,
		access:   ctl,
		search:   opts.Search,
		log:      log,
		metrics:  m,
		started:  time.Now(),
		version:  opts.Version,
		counters: make(map[string]*metrics.Counter),
	}
	if m != nil {
		m.GaugeFunc("mirror_gateway_cache_bytes", "缓存占用字节数", nil, func() float64 { return float64(c.Size()) })
		m.GaugeFunc("mirror_gateway_cache_objects", "缓存对象数量（blob + manifest）", nil, func() float64 { return float64(c.Objects()) })
		m.GaugeFunc("mirror_gateway_cache_max_bytes", "缓存容量上限（字节）", nil, func() float64 { return float64(c.MaxBytes()) })
		m.GaugeFunc("mirror_gateway_uptime_seconds", "运行时长（秒）", nil, func() float64 { return time.Since(s.started).Seconds() })
		s.reqTotal = m.Counter("mirror_gateway_requests_total", "收到的 HTTP 请求总数", nil)
		s.deniedTotal = m.Counter("mirror_gateway_access_denied_total", "因来源 IP 不在白名单而被拒绝的请求数", nil)
	}
	if ctl.Enabled() {
		allowed, denied, trust := ctl.Describe()
		log.Info("已启用 IP 访问控制", "allow", strings.Join(allowed, ","), "deny", strings.Join(denied, ","), "trustForwardedFor", trust)
	}
	return s, nil
}

// Handler 返回镜像拉取端口的处理器。
//
// 管理面使用独立端口（server.adminAddr）时，这里只保留 /v2/* 与健康检查，
// 控制台、管理接口、指标全部不对外暴露。
func (s *Server) Handler() http.Handler {
	if s.AdminSplit() {
		return s.wrap(s.servePull)
	}
	return s.wrap(s.route)
}

// AdminHandler 返回管理端口的处理器：控制台 + 管理接口 + 指标 + 健康检查。
func (s *Server) AdminHandler() http.Handler {
	return s.wrap(s.serveAdmin)
}

// AdminSplit 报告管理面是否使用独立端口。
func (s *Server) AdminSplit() bool {
	return strings.TrimSpace(s.cfg.Server.AdminAddr) != ""
}

func (s *Server) wrap(h http.HandlerFunc) http.Handler {
	// recoverer 必须在 accessLog 内层：这样 panic 时它拿到的 w 是
	// statusRecorder，可以判断响应头是否已写出——已写出就不再追加
	// 500 JSON（否则会把错误文本拼进正在流式传输的 blob 尾部）。
	return s.accessLog(s.recoverer(h))
}

// route 是单端口模式下的完整路由。
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case isV2Path(r.URL.Path) || isHealthPath(r.URL.Path) || isTokenPath(r.URL.Path):
		s.servePull(w, r)
	case isAdminPath(r.URL.Path):
		s.serveAdmin(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) servePull(w http.ResponseWriter, r *http.Request) {
	switch {
	case isV2Path(r.URL.Path):
		if !s.checkAccess(w, r) {
			return
		}
		s.handleV2(w, r)
	case isTokenPath(r.URL.Path):
		// 令牌换取端点：认证透传模式下 docker 从改写的挑战里拿到这里，
		// 受与 /v2 相同的访问控制（IP 白名单开启时同样受限）。
		if !s.checkAccess(w, r) {
			return
		}
		s.proxy.ServeToken(w, r)
	case isHealthPath(r.URL.Path):
		s.handleHealth(w, r)
	default:
		http.NotFound(w, r)
	}
}

// isTokenPath 报告是否为认证透传的令牌换取端点。
func isTokenPath(path string) bool {
	return path == "/token"
}

// checkAccess 在镜像拉取面执行 IP 白名单；健康检查不受限制，
// 否则外部探针会被一起挡掉。
func (s *Server) checkAccess(w http.ResponseWriter, r *http.Request) bool {
	if !s.access.Enabled() {
		return true
	}
	ok, ip := s.access.Allowed(r)
	if ok {
		return true
	}
	if s.deniedTotal != nil {
		s.deniedTotal.Inc()
	}
	s.log.Warn("拒绝拉取请求：来源 IP 不在白名单内", "ip", ip, "method", r.Method, "path", r.URL.Path)
	registry.WriteError(w, http.StatusForbidden, "DENIED", "来源 IP 不在允许范围内", map[string]string{"ip": ip})
	return false
}

func (s *Server) serveAdmin(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/":
		s.handleIndex(w, r)
	case path == "/healthz" || path == "/readyz":
		s.handleHealth(w, r)
	case path == "/metrics":
		s.requireAdmin(s.handleMetrics)(w, r)
	case path == "/api/v1/status":
		s.requireAdmin(s.handleStatus)(w, r)
	case path == "/api/v1/upstreams":
		s.requireAdmin(s.handleUpstreams)(w, r)
	case strings.HasPrefix(path, "/api/v1/upstreams/"):
		s.requireAdmin(s.handleProbe)(w, r)
	case path == "/api/v1/routes":
		s.requireAdmin(s.handleRoutes)(w, r)
	case path == "/api/v1/resolve":
		s.requireAdmin(s.handleResolve)(w, r)
	case path == "/api/v1/search":
		s.requireAdmin(s.handleSearch)(w, r)
	case path == "/api/v1/activity":
		s.requireAdmin(s.handleActivity)(w, r)
	case path == "/api/v1/cache/gc":
		s.requireAdmin(s.handleGC)(w, r)
	case path == "/api/v1/cache/purge":
		s.requireAdmin(s.handlePurge)(w, r)
	default:
		http.NotFound(w, r)
	}
}

func isV2Path(path string) bool {
	return path == "/v2" || path == "/v2/" || strings.HasPrefix(path, "/v2/")
}

func isHealthPath(path string) bool {
	return path == "/healthz" || path == "/readyz"
}

// isAllDigits 判断字符串是否全部由数字组成。
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isAdminPath(path string) bool {
	return path == "/" || path == "/metrics" || strings.HasPrefix(path, "/api/v1/")
}

// ---------- Docker Registry v2 ----------

func (s *Server) handleV2(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2" || r.URL.Path == "/v2/" {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte("{}"))
		}
		return
	}

	kind, name, ref := registry.ParseV2Path(strings.TrimPrefix(r.URL.Path, "/v2/"))
	switch kind {
	case registry.PathManifest:
		s.proxy.ServeManifest(w, r, name, ref)
	case registry.PathBlob:
		s.proxy.ServeBlob(w, r, name, ref)
	case registry.PathTags:
		s.proxy.ServeTags(w, r, name)
	case registry.PathUpload:
		w.Header().Set("Allow", "GET, HEAD")
		registry.WriteError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "docker-mirror-gateway 是只读镜像代理，不支持推送", nil)
	default:
		registry.WriteError(w, http.StatusNotFound, "UNSUPPORTED", "不支持的接口路径", map[string]string{"path": r.URL.Path})
	}
}

// ---------- 管理接口 ----------

type routingInfo struct {
	AutoDetect    bool   `json:"autoDetect"`
	PathAliases   bool   `json:"pathAliases"`
	Strategy      string `json:"strategy"`
	DefaultGroup  string `json:"defaultGroup"`
	DetectTimeout string `json:"detectTimeout"`
	RouteTTL      string `json:"routeTtl"`
	NegativeTTL   string `json:"negativeTtl"`
	CachedRoutes  int    `json:"cachedRoutes"`
	Detections    int64  `json:"detections"`
}

type selectionInfo struct {
	Strategy           string  `json:"strategy"`
	SpeedProbeBytes    int64   `json:"speedProbeBytes,omitempty"`
	SpeedProbeInterval string  `json:"speedProbeInterval,omitempty"`
	SpeedTolerance     float64 `json:"speedTolerance,omitempty"`
}

// listenInfo 描述端口分离情况，供控制台生成正确的接入示例。
type listenInfo struct {
	PullAddr   string `json:"pullAddr"`
	AdminAddr  string `json:"adminAddr"`
	AdminSplit bool   `json:"adminSplit"`
	AdminAuth  bool   `json:"adminAuth"`
	PullURL    string `json:"pullUrl,omitempty"`
}

type cacheInfo struct {
	Backend   string `json:"backend"`
	Enabled   bool   `json:"enabled"`
	Dir       string `json:"dir"`
	SizeBytes int64  `json:"sizeBytes"`
	MaxBytes  int64  `json:"maxBytes"`
	Objects   int64  `json:"objects"`
	TagTTL    string `json:"tagTtl"`
}

type accessInfo struct {
	Enabled           bool     `json:"enabled"`
	Allow             []string `json:"allow,omitempty"`
	Deny              []string `json:"deny,omitempty"`
	TrustForwardedFor bool     `json:"trustForwardedFor"`
}

type searchInfo struct {
	Enabled bool     `json:"enabled"`
	Sources []string `json:"sources"`
}

type totalsInfo struct {
	HealthyUpstreams int   `json:"healthyUpstreams"`
	TotalUpstreams   int   `json:"totalUpstreams"`
	Requests         int64 `json:"requests"`
	CacheHits        int64 `json:"cacheHits"`
	CacheMisses      int64 `json:"cacheMisses"`
	BytesServed      int64 `json:"bytesServed"`
}

type statusResponse struct {
	Version       string                 `json:"version"`
	StartedAt     time.Time              `json:"startedAt"`
	UptimeSeconds float64                `json:"uptimeSeconds"`
	Listen        listenInfo             `json:"listen"`
	Routing       routingInfo            `json:"routing"`
	Selection     selectionInfo          `json:"selection"`
	Cache         cacheInfo              `json:"cache"`
	Access        accessInfo             `json:"access"`
	Search        searchInfo             `json:"search"`
	Totals        totalsInfo             `json:"totals"`
	Groups        []registry.GroupStatus `json:"groups"`
	Upstreams     []registry.Status      `json:"upstreams"`
}

func (s *Server) buildStatus() statusResponse {
	upstreams := s.router.Statuses()
	groups := s.router.GroupStatuses()
	healthy := 0
	for _, u := range upstreams {
		if u.Healthy {
			healthy++
		}
	}
	st := s.proxy.Stats()
	def := ""
	if g := s.router.DefaultGroup(); g != nil {
		def = g.Name
	}
	adminAddr := s.cfg.Server.AdminAddr
	if adminAddr == "" {
		adminAddr = s.cfg.Server.Addr
	}
	return statusResponse{
		Version:       s.version,
		StartedAt:     s.started,
		UptimeSeconds: time.Since(s.started).Seconds(),
		Listen: listenInfo{
			PullAddr:   s.cfg.Server.Addr,
			AdminAddr:  adminAddr,
			AdminSplit: s.AdminSplit(),
			AdminAuth:  s.cfg.Server.AdminToken != "",
			PullURL:    s.cfg.Server.PublicURL,
		},
		Routing: routingInfo{
			AutoDetect:    s.cfg.Routing.AutoDetect,
			PathAliases:   s.cfg.Routing.PathAliases,
			Strategy:      s.cfg.Routing.Strategy,
			DefaultGroup:  def,
			DetectTimeout: s.cfg.Routing.DetectTimeout.D().String(),
			RouteTTL:      s.cfg.Routing.RouteTTL.D().String(),
			NegativeTTL:   s.cfg.Routing.NegativeTTL.D().String(),
			CachedRoutes:  len(s.router.RouteInfos()),
			Detections:    s.router.Detections(),
		},
		Selection: selectionInfo{
			Strategy:           s.cfg.Selection.Strategy,
			SpeedProbeBytes:    s.cfg.Selection.SpeedProbeBytes,
			SpeedProbeInterval: s.cfg.Selection.SpeedProbeInterval.D().String(),
			SpeedTolerance:     s.cfg.Selection.SpeedTolerance,
		},
		Groups: groups,
		Cache: cacheInfo{
			Backend:   s.cache.Backend(),
			Enabled:   s.cache.Backend() != "none",
			Dir:       s.cache.Dir(),
			SizeBytes: s.cache.Size(),
			MaxBytes:  s.cache.MaxBytes(),
			Objects:   s.cache.Objects(),
			TagTTL:    s.cache.TagTTL().String(),
		},
		Access: s.accessInfo(),
		Search: searchInfo{Enabled: s.cfg.Search.Enabled, Sources: s.cfg.Search.Sources},
		Totals: totalsInfo{
			HealthyUpstreams: healthy,
			TotalUpstreams:   len(upstreams),
			Requests:         s.reqTotal.Value(),
			CacheHits:        st.CacheHits,
			CacheMisses:      st.CacheMisses,
			BytesServed:      st.BytesServed,
		},
		Upstreams: upstreams,
	}
}

// handleActivity 返回最近的镜像拉取记录（含正在传输的），用于控制台实时监控。
func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET"})
		return
	}
	limit := activity.DefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	if s.proxy == nil {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "summary": activity.Summary{}})
		return
	}
	t := s.proxy.Activity()
	writeJSON(w, http.StatusOK, map[string]any{
		"items":   t.List(limit),
		"summary": t.Summary(),
	})
}

// handleSearch 代理公共仓库的搜索接口，结果附带建议走的分组。
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET")
		return
	}
	if s.search == nil || !s.cfg.Search.Enabled {
		writeJSONError(w, http.StatusNotFound, "搜索未启用")
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeJSONError(w, http.StatusBadRequest, "缺少查询参数 q")
		return
	}
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	resp := s.search.Search(r.Context(), query, limit)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) accessInfo() accessInfo {
	allow, deny, trust := s.access.Describe()
	return accessInfo{Enabled: s.access.Enabled(), Allow: allow, Deny: deny, TrustForwardedFor: trust}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET"})
		return
	}
	writeJSON(w, http.StatusOK, s.buildStatus())
}

func (s *Server) handleUpstreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"upstreams": s.router.Statuses(), "groups": s.router.GroupStatuses()})
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 POST"})
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/upstreams/")
	name, suffix, ok := strings.Cut(rest, "/")
	if !ok || name == "" || suffix != "probe" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "接口不存在，正确格式为 /api/v1/upstreams/{name}/probe"})
		return
	}
	u, g := s.router.ByName(name)
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "镜像源不存在: " + name})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.Health.Timeout.D()+time.Second)
	defer cancel()
	resp := map[string]any{}
	if err := s.router.Probe(ctx, g, u); err != nil {
		resp["error"] = err.Error()
	}
	for _, st := range g.Pool.Statuses() {
		if st.Name == name {
			st.Group = g.Name
			resp["upstream"] = st
			break
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleRoutes 查看或清空「仓库 -> 分组」的路由缓存。
func (s *Server) handleRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		writeJSON(w, http.StatusOK, map[string]any{
			"routes": s.router.RouteInfos(),
			"stats": map[string]any{
				"autoDetect":   s.router.AutoDetect(),
				"detections":   s.router.Detections(),
				"cachedRoutes": len(s.router.RouteInfos()),
				"ttl":          s.router.RouteTTL().String(),
			},
		})
	case http.MethodDelete:
		s.router.ClearRoutes()
		s.log.Info("路由缓存已被清空")
		writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
	default:
		w.Header().Set("Allow", "GET, HEAD, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET 或 DELETE"})
	}
}

// handleResolve 用于排查：展示某个仓库会被路由到哪个分组。
// 可选参数：host（模拟请求域名）、probe=1（并发探测各分组是否存在该仓库）。
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 GET"})
		return
	}
	repo := strings.TrimSpace(r.URL.Query().Get("repo"))
	if repo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "缺少 repo 参数，例如 /api/v1/resolve?repo=ghcr.io/owner/app:latest"})
		return
	}
	// 支持直接带 tag：nginx:latest、ghcr.io/owner/app:1.0。
	// 冒号后是纯数字且冒号前没有路径分隔符时（如 localhost:5000），
	// 视为带端口的地址而不是 tag，避免拆出假引用。
	ref := ""
	if i := strings.LastIndex(repo, ":"); i > 0 &&
		!strings.Contains(repo[i+1:], "/") &&
		(!isAllDigits(repo[i+1:]) || strings.Contains(repo[:i], "/")) {
		ref = repo[i+1:]
		repo = repo[:i]
	}
	timeout := s.cfg.Routing.DetectTimeout.D()
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout+5*time.Second)
	defer cancel()

	resp := map[string]any{"repo": repo}
	if ref != "" {
		resp["reference"] = ref
	}
	var (
		route *registry.Route
		err   error
	)
	if ref != "" {
		route, err = s.router.ResolveManifest(ctx, r.URL.Query().Get("host"), repo, ref)
	} else {
		route, err = s.router.Resolve(ctx, r.URL.Query().Get("host"), repo)
	}
	code := http.StatusOK
	if route != nil {
		resp["group"] = route.Group.Name
		resp["upstreamRepo"] = route.Repo
		resp["via"] = route.Via
	}
	if err != nil {
		resp["error"] = err.Error()
		switch {
		case errors.Is(err, registry.ErrRepoNotFound):
			code = http.StatusNotFound
		case errors.Is(err, registry.ErrInvalidRepository):
			code = http.StatusBadRequest
		default:
			code = http.StatusBadGateway
		}
	}
	if v := r.URL.Query().Get("probe"); v == "1" || v == "true" {
		resp["probe"] = s.router.ProbeRepo(ctx, repo)
	}
	writeJSON(w, code, resp)
}

func (s *Server) handleGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 POST"})
		return
	}
	res, err := s.cache.EnforceLimit()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handlePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 POST"})
		return
	}
	if err := s.cache.Purge(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.log.Warn("缓存已被手动清空")
	writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if s.metrics == nil {
		_, _ = w.Write([]byte("# metrics disabled\n"))
		return
	}
	_, _ = w.Write([]byte("# Docker Mirror Gateway metrics\n"))
	if err := s.metrics.WritePrometheus(w); err != nil {
		s.log.Debug("输出指标失败", "error", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	upstreams := s.router.Statuses()
	healthy := 0
	for _, u := range upstreams {
		if u.Healthy {
			healthy++
		}
	}
	status := "ok"
	code := http.StatusOK
	if healthy == 0 {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	if r.URL.Path == "/healthz" {
		// 进程存活即可，不因上游全部失效而重启容器。
		status, code = "ok", http.StatusOK
	}
	writeJSON(w, code, map[string]any{
		"status":           status,
		"healthyUpstreams": healthy,
		"totalUpstreams":   len(upstreams),
		"uptimeSeconds":    time.Since(s.started).Seconds(),
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(dashboardHTML)
}

// ---------- 中间件 ----------

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	token := s.cfg.Server.AdminToken
	if token == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Admin-Token")
		if got == "" {
			if v := r.Header.Get("Authorization"); strings.HasPrefix(v, "Bearer ") {
				got = strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
			}
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="docker-mirror-gateway"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "需要管理令牌"})
			return
		}
		next(w, r)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.wroteHeader {
		return
	}
	s.status = code
	s.wroteHeader = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(p []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	n, err := s.ResponseWriter.Write(p)
	s.bytes += int64(n)
	return n, err
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// counterFor 返回（或创建）某组标签对应的请求计数器。
func (s *Server) counterFor(kind, method string, status int) *metrics.Counter {
	key := kind + "\x00" + method + "\x00" + strconv.Itoa(status)
	s.counterMu.Lock()
	defer s.counterMu.Unlock()
	if c, ok := s.counters[key]; ok {
		return c
	}
	c := s.metrics.Counter("mirror_gateway_http_requests_total", "按类型统计的 HTTP 请求数", map[string]string{
		"kind":   kind,
		"method": method,
		"status": strconv.Itoa(status),
	})
	s.counters[key] = c
	return c
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		kind := requestKind(r.URL.Path)
		duration := time.Since(start)
		if s.reqTotal != nil {
			s.reqTotal.Inc()
		}
		if s.metrics != nil {
			s.counterFor(kind, r.Method, rec.status).Inc()
		}

		level := slog.LevelInfo
		switch {
		case kind == "blob" || kind == "dashboard" || kind == "health":
			level = slog.LevelDebug
		case rec.status >= 500:
			level = slog.LevelWarn
		}
		s.log.Log(r.Context(), level, "http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"durationMs", float64(duration.Microseconds())/1000.0,
			"remote", r.RemoteAddr,
		)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("请求处理发生 panic",
					"panic", rec,
					"path", r.URL.Path,
					"stack", string(debug.Stack()),
				)
				if sr, ok := w.(*statusRecorder); ok && sr.wroteHeader {
					return
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "内部错误"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func requestKind(path string) string {
	switch {
	case strings.HasPrefix(path, "/v2/"):
		switch {
		case strings.Contains(path, "/manifests/"):
			return "manifest"
		case strings.Contains(path, "/blobs/"):
			return "blob"
		case strings.HasSuffix(path, "/tags/list"):
			return "tags"
		default:
			return "registry"
		}
	case strings.HasPrefix(path, "/api/"):
		return "admin"
	case path == "/metrics":
		return "metrics"
	case path == "/":
		return "dashboard"
	case strings.HasPrefix(path, "/health"):
		return "health"
	}
	return "other"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// writeJSONError 返回统一格式的错误响应。
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
