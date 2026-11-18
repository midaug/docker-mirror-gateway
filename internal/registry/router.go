package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
	"github.com/midaug/docker-mirror-gateway/internal/notify"
)

var (
	// ErrRepoNotFound 表示所有分组里都没有这个仓库（上游明确返回 404）。
	ErrRepoNotFound = errors.New("所有分组中都不存在该仓库")
	// ErrNoRoute 表示无法确定仓库归属（网络故障、全部分组不可用等）。
	ErrNoRoute = errors.New("无法确定仓库所属的分组")
	// ErrInvalidRepository 表示仓库名不合法。
	ErrInvalidRepository = errors.New("无效的仓库名")

	errAccessDenied = errors.New("上游拒绝访问该仓库")
)

// Group 是一个仓库命名空间及其上游池。
type Group struct {
	Name     string
	Priority int
	Aliases  []string
	Hosts    []string
	Default  bool
	Library  string
	Probe    string
	Pool     *Pool
}

// RepoFor 按本分组的命名空间规则补全仓库名。
func (g *Group) RepoFor(name string) string {
	return NormalizeRepositoryName(name, g.Library)
}

// Route 是一次路由判定的结果。
type Route struct {
	Group *Group
	// Repo 是发往上游的规范化仓库名（已剥离路径别名）。
	Repo string
	// Via 记录判定方式：alias / host / cache / detect / default。
	Via string
}

// GroupStatus 是对外暴露的分组状态。
type GroupStatus struct {
	Name             string   `json:"name"`
	Priority         int      `json:"priority"`
	Default          bool     `json:"default"`
	LibraryNamespace string   `json:"libraryNamespace"`
	Hosts            []string `json:"hosts"`
	Aliases          []string `json:"aliases"`
	ProbeRepo        string   `json:"probeRepo,omitempty"`
	ProbeTag         string   `json:"probeTag,omitempty"`
	HealthyUpstreams int      `json:"healthyUpstreams"`
	TotalUpstreams   int      `json:"totalUpstreams"`
	Upstreams        []Status `json:"upstreams"`
}

// RouteInfo 是路由缓存的对外快照。
type RouteInfo struct {
	Repo      string    `json:"repo"`
	Group     string    `json:"group,omitempty"`
	Via       string    `json:"via"`
	ExpiresAt time.Time `json:"expiresAt"`
	Negative  bool      `json:"negative,omitempty"`
}

// Router 负责把「主机名 + 仓库名」解析到某个分组，并管理所有分组的健康状态。
//
// 解析顺序：
//  1. 仓库路径首段命中别名（ghcr.io、quay、nvcr.io …）——客户端显式指定，优先级最高；
//  2. 请求 Host 命中分组域名（ghcr.mirror.example.com）；
//  3. 路由缓存（仓库 -> 分组）；
//  4. 自动探测：并发查询各分组的 tags/list，按策略选出唯一分组并缓存；
//  5. 兜底分组（关闭自动探测时）。
type Router struct {
	groups  []*Group
	byAlias map[string]*Group
	byHost  map[string]*Group
	def     *Group

	auto          bool
	pathAliases   bool
	strategy      string
	detectTimeout time.Duration
	routeTTL      time.Duration
	negativeTTL   time.Duration

	cache  *routeCache
	notify *notify.Manager
	log    *slog.Logger

	detections atomic.Int64
	cacheHits  atomic.Int64

	mRoutes  *metrics.Counter
	mDetects *metrics.Counter
}

// NewRouter 根据配置构建全部分组及其上游池。
func NewRouter(cfg *config.Config, log *slog.Logger, m *metrics.Collector) (*Router, error) {
	if log == nil {
		log = slog.Default()
	}
	r := &Router{
		byAlias:       make(map[string]*Group),
		byHost:        make(map[string]*Group),
		auto:          cfg.Routing.AutoDetect,
		pathAliases:   cfg.Routing.PathAliases,
		strategy:      cfg.Routing.Strategy,
		detectTimeout: cfg.Routing.DetectTimeout.D(),
		routeTTL:      cfg.Routing.RouteTTL.D(),
		negativeTTL:   cfg.Routing.NegativeTTL.D(),
		cache:         newRouteCache(8192),
		log:           log,
	}
	if r.strategy == "" {
		r.strategy = "priority"
	}
	if r.detectTimeout <= 0 {
		r.detectTimeout = 3 * time.Second
	}
	if r.routeTTL <= 0 {
		r.routeTTL = 30 * time.Minute
	}
	if r.negativeTTL <= 0 {
		r.negativeTTL = 2 * time.Minute
	}

	// 分组监控通知：未启用时为 nil，SetNotifier 的接线也会跳过。
	// webhook 模板非法等配置问题在这里直接启动失败，避免运行期静默丢消息。
	notifier, err := notify.New(cfg.Notify, log.With("component", "notify"))
	if err != nil {
		return nil, fmt.Errorf("初始化通知通道失败: %w", err)
	}

	for _, gc := range cfg.Groups {
		pool, err := NewPool(gc, cfg.Health, cfg.Selection, log, m)
		if err != nil {
			return nil, fmt.Errorf("初始化分组 %q 失败: %w", gc.Name, err)
		}
		g := &Group{
			Name:     gc.Name,
			Priority: gc.Priority,
			Aliases:  gc.Aliases,
			Hosts:    gc.Hosts,
			Default:  gc.Default,
			Library:  gc.LibraryNamespace,
			Probe:    gc.ProbeRepo,
			Pool:     pool,
		}
		if notifier != nil && gc.Notify.Enabled && gc.Notify.MinHealthy > 0 {
			pool.SetNotifier(notifier.Evaluate)
		}
		r.groups = append(r.groups, g)
		for _, alias := range gc.Aliases {
			key := strings.ToLower(alias)
			if _, dup := r.byAlias[key]; dup {
				log.Warn("路径别名重复，先注册的分组生效", "alias", key, "group", gc.Name)
				continue
			}
			r.byAlias[key] = g
		}
		for _, host := range gc.Hosts {
			key := hostOnly(host)
			if key == "" {
				continue
			}
			if _, dup := r.byHost[key]; dup {
				log.Warn("分组域名重复，先注册的分组生效", "host", key, "group", gc.Name)
				continue
			}
			r.byHost[key] = g
		}
		if cfg.Routing.DefaultGroup == gc.Name {
			r.def = g
		}
	}
	if len(r.groups) == 0 {
		return nil, errors.New("没有任何可用分组")
	}
	if r.def == nil {
		r.def = r.groups[0]
	}
	sort.SliceStable(r.groups, func(i, j int) bool { return r.groups[i].Priority < r.groups[j].Priority })

	if m != nil {
		r.mRoutes = m.Counter("mirror_gateway_route_cache_hits_total", "路由缓存命中次数", nil)
		r.mDetects = m.Counter("mirror_gateway_route_detections_total", "仓库归属自动探测次数", nil)
		m.GaugeFunc("mirror_gateway_route_cache_entries", "路由缓存条目数", nil, func() float64 {
			return float64(r.cache.len())
		})
	}
	return r, nil
}

// Groups 返回按分组优先级排序的分组列表。
func (r *Router) Groups() []*Group {
	out := make([]*Group, len(r.groups))
	copy(out, r.groups)
	return out
}

// DefaultGroup 返回兜底分组。
func (r *Router) DefaultGroup() *Group { return r.def }

// AutoDetect 返回是否开启了自动探测。
func (r *Router) AutoDetect() bool { return r.auto }

// RouteTTL 返回路由缓存有效期。
func (r *Router) RouteTTL() time.Duration { return r.routeTTL }

// Resolve 把仓库名解析到具体分组（用于 blob / tags 等没有具体引用的请求）。
func (r *Router) Resolve(ctx context.Context, host, repo string) (*Route, error) {
	return r.resolve(ctx, host, repo, "")
}

// ResolveManifest 用具体 tag/digest 校验仓库归属，判定比 tags/list 更准确。
func (r *Router) ResolveManifest(ctx context.Context, host, repo, ref string) (*Route, error) {
	return r.resolve(ctx, host, repo, ref)
}

func (r *Router) resolve(ctx context.Context, host, repo, ref string) (*Route, error) {
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	if !ValidRepositoryName(repo) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidRepository, repo)
	}

	// 1. 路径首段别名：mirror/ghcr.io/owner/repo、mirror/quay/org/repo
	if r.pathAliases {
		if i := strings.IndexByte(repo, '/'); i > 0 {
			if g, ok := r.byAlias[strings.ToLower(repo[:i])]; ok {
				rest := repo[i+1:]
				if rest != "" && ValidRepositoryName(rest) {
					return &Route{Group: g, Repo: g.RepoFor(rest), Via: "alias"}, nil
				}
			}
		}
	}

	// 2. 请求域名：ghcr.mirror.example.com/v2/...
	if host != "" {
		if g, ok := r.byHost[hostOnly(host)]; ok {
			return &Route{Group: g, Repo: g.RepoFor(repo), Via: "host"}, nil
		}
	}

	// 3. 路由缓存
	if entry, ok := r.cache.get(repo); ok {
		r.cacheHits.Add(1)
		if r.mRoutes != nil {
			r.mRoutes.Inc()
		}
		if entry.negative {
			return nil, ErrRepoNotFound
		}
		return &Route{Group: entry.group, Repo: entry.repo, Via: "cache"}, nil
	}

	// 4. 未开启自动探测时直接使用兜底分组。
	if !r.auto || r.def == nil {
		if r.def == nil {
			return nil, ErrNoRoute
		}
		return &Route{Group: r.def, Repo: r.def.RepoFor(repo), Via: "default"}, nil
	}

	return r.detect(ctx, repo, ref)
}

// detect 并发探测各分组，选出仓库归属。
func (r *Router) detect(ctx context.Context, repo, ref string) (*Route, error) {
	r.detections.Add(1)
	if r.mDetects != nil {
		r.mDetects.Inc()
	}

	cands := r.candidates(repo)
	if len(cands) == 0 {
		return nil, fmt.Errorf("%w: 没有健康的上游可用于探测", ErrNoRoute)
	}

	dctx, cancel := context.WithTimeout(ctx, r.detectTimeout)
	defer cancel()

	ch := make(chan detectResult, len(cands))
	perCand := r.detectTimeout
	if perCand > 3*time.Second {
		perCand = 3 * time.Second
	}
	for i, cand := range cands {
		go func(i int, cand detectCandidate) {
			cctx, ccancel := context.WithTimeout(dctx, perCand)
			defer ccancel()
			var err error
			if ref != "" {
				err = cand.group.probeManifest(cctx, cand.repo, ref)
			} else {
				err = cand.group.probeTags(cctx, cand.repo)
			}
			ch <- detectResult{idx: i, group: cand.group, repo: cand.repo, err: err}
		}(i, cand)
	}

	reported := make([]bool, len(cands))
	bestIdx := len(cands)
	var best *detectResult
	var lastErr error
	definitive, inconclusive := 0, 0
	timedOut := false

	for pending := len(cands); pending > 0; pending-- {
		select {
		case res := <-ch:
			reported[res.idx] = true
			switch {
			case res.err == nil:
				if res.idx < bestIdx {
					bestIdx = res.idx
					best = &res
				}
				if r.strategy == "fastest" {
					return r.cacheRoute(repo, best, "detect")
				}
				// priority 策略：等待所有优先级更高的分组明确失败后再落定。
				ready := true
				for j := 0; j < bestIdx; j++ {
					if !reported[j] {
						ready = false
						break
					}
				}
				if ready {
					return r.cacheRoute(repo, best, "detect")
				}
			case errors.Is(res.err, errNotFound), errors.Is(res.err, errAccessDenied):
				// 该分组明确没有这个仓库（404，或匿名无权访问）。
				definitive++
			case errors.Is(res.err, context.DeadlineExceeded), errors.Is(res.err, context.Canceled):
				inconclusive++
			default:
				inconclusive++
				lastErr = res.err
			}
		case <-dctx.Done():
			pending = -1
			timedOut = true
		}
	}

	if best != nil {
		if timedOut {
			// 超时兜底：本次直接使用但不写缓存——高优先级分组可能还没给出
			// 结论，把一个可能次优的判定固化下来，会让错误路由持续整个 TTL。
			r.log.Warn("仓库归属探测超时，本次按已有结论选组但不缓存", "repo", repo, "group", best.group.Name)
			return &Route{Group: best.group, Repo: best.repo, Via: "detect"}, nil
		}
		return r.cacheRoute(repo, best, "detect")
	}
	// 只要有一个分组给出了明确的否定结论，就按「仓库不存在」处理；
	// 全部都是超时/网络错误时才返回 502，避免把网络问题误报成 404。
	if definitive > 0 || inconclusive == 0 {
		// 按具体 tag 判定时结论只对本次引用有效（同一仓库可能有别的 tag），
		// 因此只有仓库级探测（tags/list）才写负缓存。
		if ref == "" {
			r.cache.setNegative(repo, r.negativeTTL)
		}
		return nil, ErrRepoNotFound
	}
	if lastErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoRoute, lastErr)
	}
	return nil, ErrNoRoute
}

type detectCandidate struct {
	group *Group
	repo  string
}

type detectResult struct {
	idx   int
	group *Group
	repo  string
	err   error
}

// candidates 按分组优先级生成探测目标；没有健康上游的分组跳过。
func (r *Router) candidates(repo string) []detectCandidate {
	out := make([]detectCandidate, 0, len(r.groups))
	for _, g := range r.groups {
		if g.Pool.HasHealthy() {
			out = append(out, detectCandidate{group: g, repo: g.RepoFor(repo)})
		}
	}
	if len(out) == 0 {
		// 所有分组都被标记为不可用时，仍然探测一轮，给恢复留机会。
		for _, g := range r.groups {
			out = append(out, detectCandidate{group: g, repo: g.RepoFor(repo)})
		}
	}
	return out
}

func (r *Router) cacheRoute(repo string, res *detectResult, via string) (*Route, error) {
	r.cache.set(repo, routeEntry{
		group: res.group,
		repo:  res.repo,
		via:   via,
		exp:   time.Now().Add(r.routeTTL),
	})
	r.log.Debug("仓库归属已确定", "repo", repo, "group", res.group.Name, "upstreamRepo", res.repo)
	return &Route{Group: res.group, Repo: res.repo, Via: via}, nil
}

// ProbeRepo 探测某个仓库在各分组中的存在情况，用于控制台排查。
func (r *Router) ProbeRepo(ctx context.Context, repo string) map[string]string {
	ctx, cancel := context.WithTimeout(ctx, r.detectTimeout)
	defer cancel()
	out := make(map[string]string, len(r.groups))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, g := range r.groups {
		wg.Add(1)
		go func(g *Group) {
			defer wg.Done()
			err := g.probeTags(ctx, g.RepoFor(repo))
			state := "ok"
			switch {
			case errors.Is(err, errNotFound):
				state = "not-found"
			case err != nil:
				state = "error: " + err.Error()
			}
			mu.Lock()
			out[g.Name] = state
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	return out
}

// RouteInfos 返回路由缓存快照（按仓库名排序）。
func (r *Router) RouteInfos() []RouteInfo {
	entries := r.cache.snapshot()
	out := make([]RouteInfo, 0, len(entries))
	for repo, e := range entries {
		info := RouteInfo{Repo: repo, Via: e.via, ExpiresAt: e.exp, Negative: e.negative}
		if e.group != nil {
			info.Group = e.group.Name
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Repo < out[j].Repo })
	return out
}

// ClearRoutes 清空路由缓存。
func (r *Router) ClearRoutes() { r.cache.clear() }

// Detections 返回自动探测次数。
func (r *Router) Detections() int64 { return r.detections.Load() }

// Statuses 返回全部上游的扁平状态（按分组优先级 + 组内排名）。
func (r *Router) Statuses() []Status {
	out := make([]Status, 0, 16)
	for _, g := range r.groups {
		for _, st := range g.Pool.Statuses() {
			st.Group = g.Name
			out = append(out, st)
		}
	}
	return out
}

// GroupStatuses 返回分组及其上游状态。
func (r *Router) GroupStatuses() []GroupStatus {
	out := make([]GroupStatus, 0, len(r.groups))
	for _, g := range r.groups {
		upstreams := g.Pool.Statuses()
		healthy := 0
		for i := range upstreams {
			upstreams[i].Group = g.Name
			if upstreams[i].Healthy {
				healthy++
			}
		}
		out = append(out, GroupStatus{
			Name:             g.Name,
			Priority:         g.Priority,
			Default:          g.Default,
			LibraryNamespace: g.Library,
			Hosts:            g.Hosts,
			Aliases:          g.Aliases,
			ProbeRepo:        g.Probe,
			ProbeTag:         g.Pool.probeTag,
			HealthyUpstreams: healthy,
			TotalUpstreams:   len(upstreams),
			Upstreams:        upstreams,
		})
	}
	return out
}

// ByName 按上游名称查找（名称在配置中保证全局唯一）。
func (r *Router) ByName(name string) (*Upstream, *Group) {
	for _, g := range r.groups {
		if u := g.Pool.ByName(name); u != nil {
			return u, g
		}
	}
	return nil, nil
}

// Probe 探测某个上游（使用其所属分组的探针配置）。
func (r *Router) Probe(ctx context.Context, g *Group, u *Upstream) error {
	return g.Pool.Probe(ctx, u)
}

// Start 启动所有分组的健康监测与路由缓存清理，直到 ctx 取消。
func (r *Router) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for _, g := range r.groups {
		wg.Add(1)
		go func(g *Group) {
			defer wg.Done()
			g.Pool.Start(ctx)
		}(g)
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.cache.prune()
			}
		}
	}()
	wg.Wait()
}

// probeTags 通过 tags/list 判断分组内是否存在该仓库。
//
// 组内的上游互为镜像，因此 404/401/403 已经是整个分组的确定结论，
// 不再换下一个镜像源重试；只有网络错误或 5xx 才值得换源。
func (g *Group) probeTags(ctx context.Context, repo string) error {
	cands := g.Pool.Candidates()
	healthy := make([]*Upstream, 0, len(cands))
	for _, u := range cands {
		if u.Healthy() {
			healthy = append(healthy, u)
		}
	}
	if len(healthy) > 0 {
		cands = healthy
	}

	var lastErr error
	for _, u := range cands {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		resp, err := u.Do(ctx, http.MethodGet, tagsPath(repo)+"?n=1", nil, "repository:"+repo+":pull")
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if err != nil {
			// 读取失败（连接中断/客户端断开）不算仓库结论，换下一个源。
			lastErr = err
			continue
		}
		switch resp.StatusCode {
		case http.StatusOK:
			// 必须成功解析出「非空 tags」才算存在：部分源忽略 n=1 返回全量
			// 列表会被 64KB 截断、CDN 可能回 HTML 错误页，这些都不能当成
			// 「仓库存在」写进 30 分钟的路由缓存。
			var parsed struct {
				Tags []string `json:"tags"`
			}
			if json.Unmarshal(body, &parsed) != nil {
				lastErr = fmt.Errorf("%s 的 tags 响应无法解析", u.Name)
				continue
			}
			if len(parsed.Tags) == 0 {
				// 某些仓库「存在但没有任何 tag」（例如被清空的 GHCR 包），
				// tags 为空时无法拉取，按不存在处理。
				return errNotFound
			}
			return nil
		case http.StatusNotFound:
			return errNotFound
		case http.StatusUnauthorized, http.StatusForbidden:
			return errAccessDenied
		default:
			lastErr = fmt.Errorf("%s 返回 %d", u.Name, resp.StatusCode)
		}
	}
	if lastErr == nil {
		lastErr = errNotFound
	}
	return lastErr
}

// probeManifest 用具体 tag/digest 判断分组内是否存在该镜像，比 tags/list 更准确。
func (g *Group) probeManifest(ctx context.Context, repo, ref string) error {
	cands := g.Pool.Candidates()
	healthy := make([]*Upstream, 0, len(cands))
	for _, u := range cands {
		if u.Healthy() {
			healthy = append(healthy, u)
		}
	}
	if len(healthy) > 0 {
		cands = healthy
	}

	header := http.Header{}
	header.Set("Accept", DefaultManifestAccept)
	var lastErr error
	for _, u := range cands {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		resp, err := u.Do(ctx, http.MethodGet, manifestPath(repo, ref), header, "repository:"+repo+":pull")
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			return nil
		case http.StatusNotFound:
			return errNotFound
		case http.StatusUnauthorized, http.StatusForbidden:
			return errAccessDenied
		default:
			lastErr = fmt.Errorf("%s 返回 %d", u.Name, resp.StatusCode)
		}
	}
	if lastErr == nil {
		lastErr = errNotFound
	}
	return lastErr
}

// ---------- 路由缓存 ----------

type routeEntry struct {
	group    *Group
	repo     string
	via      string
	exp      time.Time
	negative bool
}

type routeCache struct {
	mu      sync.RWMutex
	entries map[string]routeEntry
	max     int
	hits    atomic.Int64
	misses  atomic.Int64
}

func newRouteCache(max int) *routeCache {
	return &routeCache{entries: make(map[string]routeEntry), max: max}
}

func (c *routeCache) get(repo string) (routeEntry, bool) {
	c.mu.RLock()
	e, ok := c.entries[repo]
	c.mu.RUnlock()
	if !ok {
		c.misses.Add(1)
		return routeEntry{}, false
	}
	if time.Now().After(e.exp) {
		c.mu.Lock()
		delete(c.entries, repo)
		c.mu.Unlock()
		c.misses.Add(1)
		return routeEntry{}, false
	}
	c.hits.Add(1)
	return e, true
}

func (c *routeCache) set(repo string, e routeEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		c.pruneLocked()
		if len(c.entries) >= c.max {
			// 仍然超限时随机丢弃一批，保证内存有上界。
			target := c.max * 3 / 4
			for k := range c.entries {
				if len(c.entries) <= target {
					break
				}
				delete(c.entries, k)
			}
		}
	}
	c.entries[repo] = e
}

func (c *routeCache) setNegative(repo string, ttl time.Duration) {
	c.set(repo, routeEntry{negative: true, via: "detect", exp: time.Now().Add(ttl)})
}

func (c *routeCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]routeEntry)
}

func (c *routeCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *routeCache) prune() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked()
}

func (c *routeCache) pruneLocked() {
	now := time.Now()
	for k, e := range c.entries {
		if now.After(e.exp) {
			delete(c.entries, k)
		}
	}
}

func (c *routeCache) snapshot() map[string]routeEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]routeEntry, len(c.entries))
	for k, e := range c.entries {
		out[k] = e
	}
	return out
}

// hostOnly 去掉端口并转小写，用于域名比对。
func hostOnly(hostport string) string {
	h := strings.ToLower(strings.TrimSpace(hostport))
	if h == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(h); err == nil {
		h = host
	}
	return strings.Trim(h, "[]")
}

// SearchClient 返回访问外部搜索接口用的 HTTP 客户端。
//
// 优先复用 dockerhub 分组里健康上游的客户端：这些源往往配了代理，
// 而 hub.docker.com 在国内通常直连不通。没有可用上游时返回 nil，
// 调用方退回默认客户端。
func (r *Router) SearchClient() *http.Client {
	for _, g := range r.groups {
		if g.Name != "dockerhub" && g.Name != "hub" {
			continue
		}
		if g.Pool == nil {
			continue
		}
		for _, u := range g.Pool.Candidates() {
			if u.Healthy() {
				return u.HTTPClient()
			}
		}
		if list := g.Pool.Upstreams(); len(list) > 0 {
			return list[0].HTTPClient()
		}
	}
	return nil
}

// ProxiedSearchClient 返回 dockerhub 分组里显式配置了代理的上游客户端。
//
// 镜像加速源只代理 registry 路径，访问 hub.docker.com 这类外部接口会直接超时；
// 想把 Hub 搜索走通，只能用挂在代理后面的那个上游。没有这样的上游时返回 nil。
func (r *Router) ProxiedSearchClient() *http.Client {
	for _, g := range r.groups {
		if g.Name != "dockerhub" && g.Name != "hub" {
			continue
		}
		if g.Pool == nil {
			continue
		}
		var fallback *http.Client
		for _, u := range g.Pool.Candidates() {
			if !u.HasExplicitProxy() {
				continue
			}
			if u.Healthy() {
				return u.HTTPClient()
			}
			if fallback == nil {
				fallback = u.HTTPClient()
			}
		}
		return fallback
	}
	return nil
}
