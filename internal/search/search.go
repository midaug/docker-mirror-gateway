// Package search 聚合公共镜像仓库的搜索接口。
//
// 目前接入 Docker Hub、Quay 与 MCR：前两者有公开的搜索 API，MCR 没有搜索
// 接口但开放了完整目录（/v2/_catalog），拉下来做本地匹配。
// GHCR、GCR、registry.k8s.io、Elastic、NVCR 既不开放目录也不提供匿名搜索，
// 只能按已知名字拉取，无法按关键字发现镜像。
//
// 搜索请求可以复用上游配置的代理：hub.docker.com 在国内经常不可达，
// 让搜索和拉取走同一条代理链路，行为才一致。
package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxLimit       = 50
	defaultTimeout = 8 * time.Second
	userAgent      = "docker-mirror-gateway/1.0"
	// sourceTimeout 是单个来源的超时。总超时往往给到 8 秒，
	// 但一个来源慢不该让用户干等——先返回能拿到的结果。
	sourceTimeout = 4 * time.Second
	// sourceCooldown 是来源失败后的默认冷却时间。hub.docker.com 在部分网络下
	// 根本不通，冷却期内直接跳过，避免每次搜索都白等一轮超时。
	sourceCooldown = 5 * time.Minute
	// rateLimitCooldown 是 401/403/429 这类限流/鉴权响应的冷却时间。
	// 这类响应说明来源可达、只是本次被拒，长冷却会让一个偶发 403
	// 把来源"静默下线"五分钟，用户看到的就是"一直没有结果"。
	rateLimitCooldown = 30 * time.Second
	// serverErrorCooldown 是 5xx 的冷却时间，介于限流与彻底不通之间。
	serverErrorCooldown = time.Minute
	// enrichTimeout 是补查标签信息的预算。查不到就留空，
	// 不能因为一个次要字段把搜索拖住。
	enrichTimeout = 5 * time.Second
	// enrichConcurrency 是补查标签的并发度。
	enrichConcurrency = 8
	// maxEnrichResults 限制补查的条数。每一条都要单独请求一次标签接口，
	// 只补当前真正展示出来的前几条，避免一次搜索发出几十个请求。
	maxEnrichResults = 20
	// tagInfoTTL 是「仓库 -> 最新标签」的缓存时长。同一个仓库短时间内
	// 不该重复补查，尤其是 Hub 还要走代理。
	tagInfoTTL = 10 * time.Minute
	// mcrCatalogTTL 是 MCR 目录的缓存时长。目录是 3000 多条的全量列表，
	// 每搜一次就拉一遍太重，而且它变化很慢。
	mcrCatalogTTL = 30 * time.Minute
	// maxMCRResults 是 MCR 结果的上限。目录匹配没有热度数据，
	// 放太多会把真正有用的结果挤出列表。
	maxMCRResults = 8
	// maxRecentTags 是版本号悬浮提示里展示的标签数。
	maxRecentTags = 10
	// maxStoredTags 是 tagInfo 里保留的标签上限。MCR 的 tags/list 是全量
	// 返回，个别仓库带几百个标签，全存进缓存没有必要。
	maxStoredTags = 200
)

// 搜索接口地址。变量而非常量，便于测试指向本地假服务。
var (
	hubSearchURL         = "https://hub.docker.com/v2/search/repositories/"
	hubTagURL            = "https://hub.docker.com/v2/repositories/"
	quaySearchURL        = "https://quay.io/api/v1/find/repositories"
	quayRepositoryTagURL = "https://quay.io/api/v1/repository/%s/%s/tag/?limit=50&onlyActiveTags=true&page=1"
	mcrCatalogURL        = "https://mcr.microsoft.com/v2/_catalog"
	mcrTagURL            = "https://mcr.microsoft.com/v2/%s/tags/list"
)

// maxDescriptionLen 是描述的最大字符数。
// Quay 有些仓库的 description 是整篇 README，不截断会把界面撑爆。
const maxDescriptionLen = 160

var (
	// 顺序有讲究：先去掉整张图片，再处理引用式链接，最后才是内联链接，
	// 否则 ![a][b] 会被链接规则拆成 "!a" 这样的残渣。
	mdImageInline = regexp.MustCompile(`!\[[^\]]*\]\([^)]*\)`)
	mdImageRef    = regexp.MustCompile(`!\[[^\]]*\]\[[^\]]*\]`)
	mdLinkRef     = regexp.MustCompile(`\[([^\]]*)\]\[[^\]]*\]`)
	mdLinkInline  = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
	mdHeading     = regexp.MustCompile(`(?m)^#{1,6}\s*`)
	htmlTag       = regexp.MustCompile(`<[^>]{0,300}>`)
)

// cleanDescription 折叠空白、去掉 Markdown 链接与 HTML 标签，并截断长度。
func cleanDescription(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = mdImageInline.ReplaceAllString(s, "")
	s = mdImageRef.ReplaceAllString(s, "")
	s = mdLinkRef.ReplaceAllString(s, "$1")
	s = mdLinkInline.ReplaceAllString(s, "$1")
	s = htmlTag.ReplaceAllString(s, "")
	s = mdHeading.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "`", "")
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) > maxDescriptionLen {
		return string(r[:maxDescriptionLen]) + "…"
	}
	return s
}

// Result 是一条搜索结果。
type Result struct {
	Source      string `json:"source"` // hub | quay | mcr
	Group       string `json:"group"`  // 建议走的分组名
	Name        string `json:"name"`   // library/nginx
	Description string `json:"description"`
	Stars       int    `json:"stars,omitempty"`
	Pulls       int64  `json:"pulls,omitempty"`
	Official    bool   `json:"official"`
	// Updated 是仓库最近一次更新的时间（RFC3339，UTC），便于判断 latest 的新旧。
	// MCR 的目录没有时间字段，这里为空。
	Updated string `json:"updated,omitempty"`
	// LatestTag 是除 latest 之外最新的版本号，例如 1.27.3。
	// 搜索接口只给仓库名，不带标签，需要按仓库补查一次。
	LatestTag string `json:"latestTag,omitempty"`
	// URL 是仓库在对应源上的页面，控制台里点击镜像名跳转。
	URL string `json:"url,omitempty"`
	// RecentTags 是最近更新的标签（不含 latest，最多 10 个），
	// 悬浮在版本号上时展示，方便直接挑一个可用的 tag。
	RecentTags []string `json:"recentTags,omitempty"`
	// sourceRank / order 记录结果来自哪个来源、在该来源里的原始顺序，
	// 用于在热度相同时保留各来源自己的相关性排序。
	sourceRank int
	order      int
}

// Response 是一次搜索的汇总结果。
type Response struct {
	Query   string   `json:"query"`
	Results []Result `json:"results"`
	Errors  []string `json:"errors,omitempty"`
	// Skipped 是处于失败冷却期、本次没有查询的来源。
	Skipped []string `json:"skipped,omitempty"`
}

// Service 并发查询各个来源并合并结果。
type Service struct {
	client        *http.Client
	hubClient     *http.Client      // 可选：Docker Hub 专用客户端（通常挂在代理后面）
	hubAuth       *hubTokenProvider // 可选：Docker Hub 账号认证（提升搜索 API 限额）
	hubAuthUser   string            // 构造期暂存凭据，New 末尾转成 hubAuth
	hubAuthPass   string
	timeout       time.Duration
	sources       []string
	enrichUpdated bool
	enrichTags    bool
	log           *slog.Logger

	mu       sync.Mutex
	down     map[string]time.Time     // 来源 -> 失败冷却到期时间
	downCool map[string]time.Duration // 来源 -> 本次实际冷却时长（用于日志/状态）

	tagMu    sync.Mutex
	tagCache map[string]tagInfo // 来源/仓库 -> 最新标签信息

	mcrMu    sync.Mutex
	mcrAt    time.Time
	mcrRepos []string
}

// tagInfo 是补查到的仓库标签信息。
type tagInfo struct {
	at      time.Time
	updated string
	latest  string
	// tags 是仓库的标签列表（可能截断到 maxStoredTags），用于
	// 「搜索带 tag 时验证仓库是否拥有该标签」与悬浮展示。
	tags []string
}

// Option 用于给搜索服务注入可选依赖。
type Option func(*Service)

// WithHubClient 单独指定访问 hub.docker.com 的客户端。
//
// 镜像加速源只代理 registry 路径，hub.docker.com 在国内通常只有走代理才通，
// 因此这里可以传入一个挂了代理的客户端，避免影响 Quay / MCR 的直连速度。
func WithHubClient(c *http.Client) Option {
	return func(s *Service) {
		if c != nil {
			s.hubClient = c
		}
	}
}

// WithTags 控制是否补查仓库的最新版本号。默认关闭，由调用方按配置开启。
func WithTags(enabled bool) Option {
	return func(s *Service) {
		s.enrichTags = enabled
	}
}

// WithHubAuth 用 Docker Hub 账号（密码或 PAT）为搜索 API 启用认证。
//
// 匿名的 hub.docker.com 搜索接口按出口 IP 限流，共享代理出口很容易返回
// 403/429；带上账号 token 可用认证用户的配额。换 token 失败时自动降级为
// 匿名，不影响搜索。凭据为空时不启用。
func WithHubAuth(username, password string) Option {
	return func(s *Service) {
		if strings.TrimSpace(username) == "" {
			return
		}
		s.hubAuthUser = username
		s.hubAuthPass = password
	}
}

// New 创建搜索服务。client 为 nil 时使用默认客户端。
func New(client *http.Client, timeout time.Duration, sources []string, enrichUpdated bool, log *slog.Logger, opts ...Option) *Service {
	if client == nil {
		client = &http.Client{}
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if log == nil {
		log = slog.Default()
	}
	if len(sources) == 0 {
		sources = []string{"hub", "quay", "mcr"}
	}
	s := &Service{
		client:        client,
		timeout:       timeout,
		sources:       sources,
		enrichUpdated: enrichUpdated,
		log:           log,
		down:          make(map[string]time.Time),
		downCool:      make(map[string]time.Duration),
		tagCache:      make(map[string]tagInfo),
	}
	for _, opt := range opts {
		opt(s)
	}
	// 有账号凭据时，用 Hub 客户端（含代理）换取 token，并把 Hub 请求
	// 包一层自动附加 Authorization 的 transport。登录请求本身用裸客户端，
	// 避免递归经过 auth transport。
	if s.hubAuthUser != "" {
		base := s.hubHTTPClient()
		s.hubAuth = newHubTokenProvider(s.hubAuthUser, s.hubAuthPass, base, log)
		transport := base.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		wrapped := *base
		wrapped.Transport = &hubAuthTransport{base: transport, provider: s.hubAuth}
		s.hubClient = &wrapped
	}
	return s
}

// hubHTTPClient 返回访问 Docker Hub 的客户端。
func (s *Service) hubHTTPClient() *http.Client {
	if s.hubClient != nil {
		return s.hubClient
	}
	return s.client
}

// Search 查询所有已启用的来源。单个来源失败不影响其他来源。
func (s *Service) Search(ctx context.Context, query string, limit int) Response {
	query = strings.TrimSpace(query)
	resp := Response{Query: query}
	if query == "" {
		return resp
	}
	// "nginx:1.31.8" 拆成名字 + 期望版本：名字照常搜索，版本用来
	// 过滤结果——只保留确实拥有这个标签的仓库。
	searchName, tagFilter := splitQueryTag(query)
	if limit <= 0 {
		limit = 20
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// 用总 ctx 的预算换取 Hub token（若启用认证），失败会自行降级匿名。
	// 放在并发查询之前，让登录时间不计入单个来源的 4 秒超时。
	if s.hubAuth != nil {
		s.hubAuth.refresh(ctx)
	}

	type outcome struct {
		results []Result
		err     error
		source  string
	}
	// 冷却期内的来源先跳过；如果全都在冷却，就再试一遍，
	// 否则网络恢复后永远等不到自动恢复。
	active := make([]string, 0, len(s.sources))
	for _, src := range s.sources {
		if s.coolingDown(src) {
			resp.Skipped = append(resp.Skipped, src)
			continue
		}
		active = append(active, src)
	}
	if len(active) == 0 {
		// 全在冷却里也要试一次，否则网络恢复后没有机会自愈。
		active = s.sources
		resp.Skipped = nil
	}

	ch := make(chan outcome, len(active))
	var wg sync.WaitGroup
	for _, src := range active {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			srcCtx, srcCancel := context.WithTimeout(ctx, sourceTimeout)
			defer srcCancel()
			var (
				res []Result
				err error
			)
			switch src {
			case "hub":
				res, err = s.searchHub(srcCtx, searchName, limit)
			case "quay":
				res, err = s.searchQuay(srcCtx, searchName, limit)
			case "mcr":
				res, err = s.searchMCR(srcCtx, searchName, limit)
			default:
				return
			}
			if err != nil {
				s.markDown(src, err)
				// Hub 用缓存的 token 被明确拒绝（401，如 PAT 被吊销）时
				// 清掉缓存，下次搜索重新登录，而不是带着失效 token 空转。
				if src == "hub" && s.hubAuth != nil {
					var se *statusError
					if errors.As(err, &se) && se.code == http.StatusUnauthorized {
						s.hubAuth.invalidate()
					}
				}
			} else {
				s.markUp(src)
			}
			ch <- outcome{results: res, err: err, source: src}
		}()
	}
	wg.Wait()
	close(ch)

	sourceRank := make(map[string]int, len(s.sources))
	for i, src := range s.sources {
		sourceRank[src] = i
	}
	for out := range ch {
		if out.err != nil {
			s.log.Debug("搜索来源失败", "source", out.source, "error", out.err)
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %v", out.source, out.err))
			continue
		}
		for i := range out.results {
			out.results[i].sourceRank = sourceRank[out.source]
			out.results[i].order = i
		}
		resp.Results = append(resp.Results, out.results...)
	}
	// 先按热度排序：官方、Star、拉取量，最后保留各来源自身的相关性顺序。
	sort.SliceStable(resp.Results, func(i, j int) bool {
		a, b := resp.Results[i], resp.Results[j]
		if a.Official != b.Official {
			return a.Official
		}
		if a.Stars != b.Stars {
			return a.Stars > b.Stars
		}
		if a.Pulls != b.Pulls {
			return a.Pulls > b.Pulls
		}
		// 没有热度数据时，保持各来源自己的相关性顺序（Hub/Quay/MCR 依次）。
		if a.sourceRank != b.sourceRank {
			return a.sourceRank < b.sourceRank
		}
		return a.order < b.order
	})
	resp.Results = truncateWithSourceFloor(resp.Results, limit)
	// 搜索接口只给仓库名，不带标签：补查更新时间与最新版本号。
	// 排序截断之后再补，避免为用不上的结果发请求；带 tag 搜索时
	// 还要在这一步验证仓库是否真的拥有该标签，验证不过的直接过滤。
	if s.enrichUpdated || s.enrichTags || tagFilter != "" {
		resp.Results = s.enrichResults(ctx, resp.Results, tagFilter)
	}
	return resp
}

// perSourceFloor 是截断时给每个来源的保底席位。
//
// 热度排序会让没有热度数据的来源（MCR 只有目录匹配度、Quay 部分结果没有
// Star）永远排在 Hub 的长尾仓库之后被整体挤出列表——Hub 上随便一个无人
// 问津的同名仓库都有拉取量。保底几个席位，多来源搜索才不会名存实亡。
const perSourceFloor = 3

// truncateWithSourceFloor 按热度截断到 limit 条，但保证每个来源至少露出
// perSourceFloor 条（如果它有这么多结果）。补位条目按各来源自身的相关性
// 顺序放在列表尾部，最多占一半名额，避免挤掉头部高热度结果。
func truncateWithSourceFloor(results []Result, limit int) []Result {
	if len(results) <= limit {
		return results
	}
	counts := make(map[string]int, 4)
	for _, r := range results[:limit] {
		counts[r.Source]++
	}
	var extra []Result
	for _, r := range results[limit:] {
		if counts[r.Source] >= perSourceFloor {
			continue
		}
		counts[r.Source]++
		extra = append(extra, r)
	}
	if len(extra) > limit/2 {
		extra = extra[:limit/2]
	}
	if len(extra) == 0 {
		return results[:limit]
	}
	keep := results[:limit-len(extra)]
	return append(keep, extra...)
}

// cooldownFor 按错误类型决定冷却时长。
//
// 限流/鉴权（401/403/429）说明来源本身是通的、只是这次被拒，用长冷却会让
// 一个偶发 403 把来源静默下线五分钟——用户看到的就是"一直没有结果"。
// 5xx 介于两者之间；网络错误（超时、连接失败）才用默认的长冷却。
func cooldownFor(err error) time.Duration {
	var se *statusError
	if errors.As(err, &se) {
		switch {
		case se.code == http.StatusUnauthorized || se.code == http.StatusForbidden || se.code == http.StatusTooManyRequests:
			return rateLimitCooldown
		case se.code >= 500 && se.code <= 599:
			return serverErrorCooldown
		}
	}
	return sourceCooldown
}

func (s *Service) coolingDown(src string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.down[src]
	return ok && time.Since(at) < s.downCool[src]
}

func (s *Service) markDown(src string, err error) {
	cool := cooldownFor(err)
	s.mu.Lock()
	_, existed := s.down[src]
	s.down[src] = time.Now()
	s.downCool[src] = cool
	s.mu.Unlock()
	if !existed {
		s.log.Warn("搜索来源不可用，暂时跳过", "source", src, "cooldown", cool.String(), "error", err)
	}
}

func (s *Service) markUp(src string) {
	s.mu.Lock()
	_, existed := s.down[src]
	delete(s.down, src)
	delete(s.downCool, src)
	s.mu.Unlock()
	if existed {
		s.log.Info("搜索来源已恢复", "source", src)
	}
}

// repoURL 返回仓库在对应源上的页面地址，控制台点击镜像名跳转。
func repoURL(source, name string, official bool) string {
	switch source {
	case "hub":
		if official || strings.HasPrefix(name, "library/") {
			return "https://hub.docker.com/_/" + strings.TrimPrefix(name, "library/")
		}
		return "https://hub.docker.com/r/" + name
	case "quay":
		return "https://quay.io/repository/" + name
	case "mcr":
		return "https://mcr.microsoft.com/en-us/product/" + name + "/about"
	}
	return ""
}

// ---------- Docker Hub ----------

type hubResponse struct {
	Count   int         `json:"count"`
	Results []hubResult `json:"results"`
}

type hubResult struct {
	RepoName         string `json:"repo_name"`
	Name             string `json:"name"`
	ShortDescription string `json:"short_description"`
	Description      string `json:"description"`
	StarCount        int    `json:"star_count"`
	PullCount        int64  `json:"pull_count"`
	IsOfficial       bool   `json:"is_official"`
	LastUpdated      string `json:"last_updated"`
}

func (s *Service) searchHub(ctx context.Context, query string, limit int) ([]Result, error) {
	q := url.Values{}
	q.Set("query", query)
	q.Set("page_size", strconv.Itoa(limit))
	endpoint := hubSearchURL + "?" + q.Encode()

	body, err := s.fetchWith(ctx, s.hubHTTPClient(), endpoint, "application/json")
	if err != nil {
		return nil, err
	}
	var payload hubResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 Docker Hub 响应失败: %w", err)
	}
	out := make([]Result, 0, len(payload.Results))
	for _, item := range payload.Results {
		name := item.RepoName
		if name == "" {
			name = item.Name
		}
		if name == "" {
			continue
		}
		if !strings.Contains(name, "/") {
			name = "library/" + name
		}
		desc := item.ShortDescription
		if desc == "" {
			desc = item.Description
		}
		out = append(out, Result{
			Source:      "hub",
			Group:       "dockerhub",
			Name:        name,
			Description: cleanDescription(desc),
			Stars:       item.StarCount,
			Pulls:       item.PullCount,
			Official:    item.IsOfficial,
			Updated:     normalizeTime(item.LastUpdated),
			URL:         repoURL("hub", name, item.IsOfficial),
		})
	}
	return out, nil
}

// ---------- Quay ----------

type quayResponse struct {
	Results []quayResult `json:"results"`
}

type quayResult struct {
	Namespace struct {
		Name string `json:"name"`
	} `json:"namespace"`
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	IsPublic     bool            `json:"is_public"`
	LastModified json.RawMessage `json:"last_modified"`
}

func (s *Service) searchQuay(ctx context.Context, query string, limit int) ([]Result, error) {
	q := url.Values{}
	q.Set("query", query)
	q.Set("page_size", strconv.Itoa(limit))
	endpoint := quaySearchURL + "?" + q.Encode()

	body, err := s.fetch(ctx, endpoint, "application/json")
	if err != nil {
		return nil, err
	}
	var payload quayResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 Quay 响应失败: %w", err)
	}
	out := make([]Result, 0, len(payload.Results))
	for _, item := range payload.Results {
		if item.Name == "" || !item.IsPublic {
			continue
		}
		name := item.Namespace.Name + "/" + item.Name
		out = append(out, Result{
			Source:      "quay",
			Group:       "quay",
			Name:        name,
			Description: cleanDescription(item.Description),
			Updated:     normalizeRawTime(item.LastModified),
			URL:         repoURL("quay", name, false),
		})
	}
	return out, nil
}

// enrichResults 给搜索结果补查「最近更新时间」「最新版本号」与最近标签。
//
// 三个来源的搜索接口都只返回仓库名，标签得按仓库再查一次：
//
//	hub  -> /v2/repositories/<name>/tags/
//	quay -> /api/v1/repository/<ns>/<repo>/tag/
//	mcr  -> /v2/<repo>/tags/list
//
// 并发压到 8，超出预算就留空，不影响搜索结果本身。tagFilter 非空时
// （搜索词带 :tag），会在这里验证仓库是否真的拥有该标签——查不到标签
// 或没有匹配标签的结果直接过滤掉，返回过滤后的列表。
func (s *Service) enrichResults(ctx context.Context, results []Result, tagFilter string) []Result {
	n := len(results)
	if n > maxEnrichResults {
		n = maxEnrichResults
	}
	if n == 0 {
		return results
	}
	ctx, cancel := context.WithTimeout(ctx, enrichTimeout)
	defer cancel()

	// 带 tag 过滤时标签是必需品，绕过 enrichTags 开关。
	wantTags := s.enrichTags || tagFilter != ""
	sem := make(chan struct{}, enrichConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// 预算耗尽还没轮到验证，不能让未过滤的结果混出去。
				if tagFilter != "" {
					results[i] = Result{}
				}
				return
			}
			defer func() { <-sem }()

			info, ok := s.tagInfoFor(ctx, results[i])
			if !ok {
				// 验证不了标签（仓库不存在 / 无标签）就别让
				// 「跟版本无关」的结果混进来。
				if tagFilter != "" {
					results[i] = Result{}
				}
				return
			}
			if tagFilter != "" && !tagMatches(info.tags, tagFilter) {
				results[i] = Result{}
				return
			}
			if s.enrichUpdated && info.updated != "" {
				results[i].Updated = info.updated
			}
			if wantTags {
				results[i].LatestTag = info.latest
				if tags := displayTags(info.tags, maxRecentTags); len(tags) > 0 {
					results[i].RecentTags = tags
				}
			}
		}()
	}
	wg.Wait()

	if tagFilter == "" {
		return results
	}
	kept := make([]Result, 0, len(results))
	for _, r := range results {
		if r.Source != "" {
			kept = append(kept, r)
		}
	}
	return kept
}

// tagInfoFor 返回仓库的标签信息，命中缓存就不再请求。
func (s *Service) tagInfoFor(ctx context.Context, r Result) (tagInfo, bool) {
	key := r.Source + "/" + r.Name
	if info, ok := s.tagCacheGet(key); ok {
		return info, true
	}
	var (
		info tagInfo
		err  error
	)
	switch r.Source {
	case "hub":
		info, err = s.fetchHubTags(ctx, r.Name)
	case "quay":
		info, err = s.fetchQuayTags(ctx, r.Name)
	case "mcr":
		info, err = s.fetchMCRTags(ctx, r.Name)
	default:
		return tagInfo{}, false
	}
	if err != nil {
		return tagInfo{}, false
	}
	s.tagCachePut(key, info)
	return info, true
}

func (s *Service) tagCacheGet(key string) (tagInfo, bool) {
	s.tagMu.Lock()
	defer s.tagMu.Unlock()
	info, ok := s.tagCache[key]
	if !ok || time.Since(info.at) >= tagInfoTTL {
		return tagInfo{}, false
	}
	return info, true
}

func (s *Service) tagCachePut(key string, info tagInfo) {
	s.tagMu.Lock()
	defer s.tagMu.Unlock()
	// 超过上限时随机剔除一半：整体清空会把 TTL 内仍然有效的条目全部丢掉，
	// 缓存命中率周期性崩塌；随机淘汰既控制内存又保留多数热条目。
	if len(s.tagCache) > 1024 {
		for k := range s.tagCache {
			if len(s.tagCache) <= 512 {
				break
			}
			delete(s.tagCache, k)
		}
	}
	info.at = time.Now()
	s.tagCache[key] = info
}

// fetchHubTags 查 Docker Hub 仓库的标签。
//
// 顺带把最近一个标签的时间当成仓库更新时间：v2 搜索接口已经不再返回
// last_updated，只有标签接口还带时间。
func (s *Service) fetchHubTags(ctx context.Context, name string) (tagInfo, error) {
	if !strings.Contains(name, "/") {
		name = "library/" + name
	}
	endpoint := hubTagURL + name + "/tags/?page_size=100"
	body, err := s.fetchWith(ctx, s.hubHTTPClient(), endpoint, "application/json")
	if err != nil {
		return tagInfo{}, err
	}
	var payload struct {
		Results []struct {
			Name        string `json:"name"`
			LastUpdated string `json:"last_updated"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return tagInfo{}, fmt.Errorf("解析 Docker Hub 标签失败: %w", err)
	}
	names := make([]string, 0, len(payload.Results))
	info := tagInfo{}
	for _, t := range payload.Results {
		names = append(names, t.Name)
		// 归一化后都是 RFC3339 UTC，字符串比较即时间先后。
		if ts := normalizeTime(t.LastUpdated); ts != "" && ts > info.updated {
			info.updated = ts
		}
	}
	if len(names) > maxStoredTags {
		names = names[:maxStoredTags]
	}
	info.tags = names
	info.latest = pickNewestTag(names)
	return info, nil
}

// fetchQuayTags 查 Quay 仓库的标签。列表按最近更新排序，第一条就是最新的。
func (s *Service) fetchQuayTags(ctx context.Context, name string) (tagInfo, error) {
	namespace, repo, ok := strings.Cut(name, "/")
	if !ok || namespace == "" || repo == "" {
		return tagInfo{}, fmt.Errorf("仓库名缺少命名空间")
	}
	endpoint := fmt.Sprintf(quayRepositoryTagURL, url.PathEscape(namespace), url.PathEscape(repo))
	body, err := s.fetch(ctx, endpoint, "application/json")
	if err != nil {
		return tagInfo{}, err
	}
	var payload struct {
		Tags []struct {
			Name         string          `json:"name"`
			LastModified json.RawMessage `json:"last_modified"`
		} `json:"tags"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return tagInfo{}, fmt.Errorf("解析 Quay 标签失败: %w", err)
	}
	names := make([]string, 0, len(payload.Tags))
	for _, t := range payload.Tags {
		names = append(names, t.Name)
	}
	if len(names) > maxStoredTags {
		names = names[:maxStoredTags]
	}
	info := tagInfo{latest: pickNewestTag(names), tags: names}
	if len(payload.Tags) > 0 {
		// 形如 "Mon, 16 Nov 2026 04:29:28 -0000"。
		info.updated = normalizeRawTime(payload.Tags[0].LastModified)
	}
	return info, nil
}

// fetchMCRTags 查 MCR 仓库的标签。目录接口没有时间字段，只取版本号；
// 返回的是全量字母序列表，重排成版本从新到旧再截断。
func (s *Service) fetchMCRTags(ctx context.Context, name string) (tagInfo, error) {
	endpoint := fmt.Sprintf(mcrTagURL, name)
	body, err := s.fetch(ctx, endpoint, "application/json")
	if err != nil {
		return tagInfo{}, err
	}
	var payload struct {
		Tags []string `json:"tags"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return tagInfo{}, fmt.Errorf("解析 MCR 标签失败: %w", err)
	}
	sortTagsByVersion(payload.Tags)
	if len(payload.Tags) > maxStoredTags {
		payload.Tags = payload.Tags[:maxStoredTags]
	}
	return tagInfo{latest: pickNewestTag(payload.Tags), tags: payload.Tags}, nil
}

// pickNewestTag 从标签列表里挑出最新的版本号。
//
// 只认以数字开头的版本形态（1.27.3、v2.1.0），至少两段，避免把 2024-01-01
// 这类日期标签当成版本；同一个版本号优先不带后缀的（1.27.3 优于 1.27.3-alpine）。
// stable、mainline、latest 这类浮动标签不参与。
func pickNewestTag(names []string) string {
	var (
		best     string
		bestVer  []int
		bestPure bool
	)
	for _, raw := range names {
		ver, rest, ok := parseVersionTag(raw)
		if !ok || len(ver) < 2 {
			continue
		}
		pure := rest == ""
		switch cmp := compareVersion(ver, bestVer); {
		case best == "" || cmp > 0 || (cmp == 0 && pure && !bestPure):
			best, bestVer, bestPure = raw, ver, pure
		}
	}
	return best
}

// parseVersionTag 把 1.27.3-alpine 拆成 ([1 27 3], "-alpine")。
func parseVersionTag(s string) ([]int, string, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil, "", false
	}
	if t[0] == 'v' || t[0] == 'V' {
		t = t[1:]
	}
	if t == "" || t[0] < '0' || t[0] > '9' {
		return nil, "", false
	}
	parts := make([]int, 0, 4)
	i := 0
	for {
		j := i
		for j < len(t) && t[j] >= '0' && t[j] <= '9' {
			j++
		}
		if j == i {
			break
		}
		if j-i > 9 { // 过长的数字串不是版本号
			return nil, "", false
		}
		n, err := strconv.Atoi(t[i:j])
		if err != nil {
			return nil, "", false
		}
		parts = append(parts, n)
		i = j
		if i < len(t) && t[i] == '.' {
			i++
			continue
		}
		break
	}
	if len(parts) == 0 {
		return nil, "", false
	}
	return parts, t[i:], true
}

// compareVersion 逐段比较版本号，缺的段按 0 处理。
func compareVersion(a, b []int) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		var x, y int
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			if x > y {
				return 1
			}
			return -1
		}
	}
	return 0
}

// splitQueryTag 把 "nginx:1.31.8" 拆成 ("nginx", "1.31.8")。
//
// 只有冒号后是合法标签形态（字母数字与 . _ -）时才拆，其余情况原样返回：
// 带端口的地址、普通关键字都不能误伤。
func splitQueryTag(q string) (name, tag string) {
	i := strings.LastIndex(q, ":")
	if i < 0 || i == len(q)-1 {
		return q, ""
	}
	if !validTag(q[i+1:]) {
		return q, ""
	}
	return q[:i], q[i+1:]
}

// validTag 判断是否是合法的镜像标签形态。
func validTag(tag string) bool {
	if tag == "" || len(tag) > 128 {
		return false
	}
	for _, c := range tag {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-':
		default:
			return false
		}
	}
	return true
}

// tagMatches 检查仓库的标签列表里是否有指定标签。
//
// 除了精确相等，还接受「前缀 + 分隔符」的变体：搜 1.31.8 也能命中
// 1.31.8-alpine3.24、1.31.8-perl 这类同版本变体标签，否则变体仓库
// 会被全部过滤掉。
func tagMatches(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
		if strings.HasPrefix(t, want+"-") || strings.HasPrefix(t, want+"_") ||
			strings.HasPrefix(t, want+".") {
			return true
		}
	}
	return false
}

// displayTags 从标签列表里挑出用于悬浮展示的最近 n 个。
//
// latest 与 stable、mainline 这类滚动别名（含 stable-alpine 之类的组合
// 形态）对挑版本没有帮助，跳过；Hub / Quay 的列表本来就按最近更新排序，
// 原样取前 n 个即可，MCR 的全量字母序列表在 fetch 时已按版本降序重排。
func displayTags(tags []string, n int) []string {
	out := make([]string, 0, n)
	for _, t := range tags {
		if isRollingTag(t) {
			continue
		}
		out = append(out, t)
		if len(out) >= n {
			break
		}
	}
	return out
}

// isRollingTag 判断是否是滚动别名标签：latest、stable、mainline 等，
// 包括 stable-alpine3.24、mainline-perl 这类组合形态。
func isRollingTag(t string) bool {
	t = strings.ToLower(t)
	for _, alias := range []string{"latest", "stable", "edge", "mainline", "nightly", "master", "dev", "head"} {
		if t == alias || strings.HasPrefix(t, alias+"-") || strings.HasPrefix(t, alias+"_") {
			return true
		}
	}
	return false
}

// sortTagsByVersion 把标签按版本从新到旧排（字母序的 MCR 全量列表用）。
// 非版本形态的标签保持相对顺序排到后面。
func sortTagsByVersion(names []string) {
	sort.SliceStable(names, func(i, j int) bool {
		a, _, aok := parseVersionTag(names[i])
		b, _, bok := parseVersionTag(names[j])
		switch {
		case aok && bok:
			if cmp := compareVersion(a, b); cmp != 0 {
				return cmp > 0
			}
			// 同版本号时短的在前（1.31.8 优先于 1.31.8-alpine3.24）。
			return len(names[i]) < len(names[j])
		case aok:
			return true
		default:
			return false
		}
	})
}

// ---------- MCR ----------

// searchMCR 在 MCR 的完整目录里做关键字匹配。
// MCR 没有搜索 API，但 /v2/_catalog 会返回全部仓库名（约 3700 条）。
func (s *Service) searchMCR(ctx context.Context, query string, limit int) ([]Result, error) {
	repos, err := s.mcrCatalog(ctx)
	if err != nil {
		return nil, err
	}
	q := strings.ToLower(strings.TrimSpace(query))
	tokens := strings.Fields(q)
	if q == "" || len(tokens) == 0 {
		return nil, nil
	}

	type match struct {
		name  string
		score int
	}
	matches := make([]match, 0, 16)
	for _, repo := range repos {
		lower := strings.ToLower(repo)
		ok := true
		for _, token := range tokens {
			if !strings.Contains(lower, token) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		score := 1
		switch {
		case lower == q:
			score = 4
		case strings.HasSuffix(lower, "/"+q):
			score = 3
		case strings.HasPrefix(lower, q):
			score = 2
		}
		matches = append(matches, match{name: repo, score: score})
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		if len(matches[i].name) != len(matches[j].name) {
			return len(matches[i].name) < len(matches[j].name)
		}
		return matches[i].name < matches[j].name
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	if len(matches) > maxMCRResults {
		matches = matches[:maxMCRResults]
	}
	out := make([]Result, 0, len(matches))
	for _, m := range matches {
		out = append(out, Result{Source: "mcr", Group: "mcr", Name: m.name, URL: repoURL("mcr", m.name, false)})
	}
	return out, nil
}

// mcrCatalog 返回缓存的 MCR 仓库名列表。
func (s *Service) mcrCatalog(ctx context.Context) ([]string, error) {
	s.mcrMu.Lock()
	if len(s.mcrRepos) > 0 && time.Since(s.mcrAt) < mcrCatalogTTL {
		repos := s.mcrRepos
		s.mcrMu.Unlock()
		return repos, nil
	}
	s.mcrMu.Unlock()

	body, err := s.fetch(ctx, mcrCatalogURL, "application/json")
	if err != nil {
		return nil, err
	}
	var payload struct {
		Repositories []string `json:"repositories"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 MCR 目录失败: %w", err)
	}
	if len(payload.Repositories) == 0 {
		return nil, fmt.Errorf("MCR 目录为空")
	}
	s.mcrMu.Lock()
	s.mcrRepos = payload.Repositories
	s.mcrAt = time.Now()
	s.mcrMu.Unlock()
	return payload.Repositories, nil
}

// normalizeTime 把各种时间写法统一成 RFC3339（UTC），无法解析时返回空。
func normalizeTime(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}
	// 少数接口直接给 Unix 秒。
	if sec, err := strconv.ParseInt(raw, 10, 64); err == nil && sec > 0 {
		return time.Unix(sec, 0).UTC().Format(time.RFC3339)
	}
	return ""
}

// normalizeRawTime 处理「字符串或数字」两种 JSON 时间。
func normalizeRawTime(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	return normalizeTime(strings.Trim(strings.TrimSpace(string(raw)), `"`))
}

// fetch 发起 GET 并限制响应体积。
func (s *Service) fetch(ctx context.Context, endpoint, accept string) ([]byte, error) {
	return s.fetchWith(ctx, s.client, endpoint, accept)
}

// statusError 携带上游响应码，供冷却策略按错误类型分级。
type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("上游返回 HTTP %d: %s", e.code, e.msg)
	}
	return fmt.Sprintf("上游返回 HTTP %d", e.code)
}

// fetchWith 用指定客户端发起 GET 并限制响应体积。
func (s *Service) fetchWith(ctx context.Context, client *http.Client, endpoint, accept string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", accept)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// 读一点响应体：403/429 通常带限流说明（如 Docker Hub 的 rate limit），
		// 写进错误里才能在日志/接口上一眼看出原因，而不是只看到裸状态码。
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &statusError{code: resp.StatusCode, msg: strings.TrimSpace(string(body))}
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}
