package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/activity"
	"github.com/midaug/docker-mirror-gateway/internal/cache"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

// errNotFound 表示当前上游没有这个镜像（不代表上游故障）。
var errNotFound = errors.New("上游不存在该对象")

// deniedError 表示上游按仓库白名单/权限策略拒绝了整个仓库（如 DaoCloud
// public-image-mirror 对白名单外镜像的 403 DENIED）。
//
// 它与源故障的区别：源本身在线且能正常服务白名单内的镜像（健康探测用的
// 常用镜像通常就在白名单内），只是不提供这一个仓库。因此它不应该计入
// 健康降级统计，否则源会在可用/不可用之间反复震荡。
type deniedError struct {
	msg string
}

func (e *deniedError) Error() string { return e.msg }

// isDenylistBody 判断上游 403 响应体是否为白名单拒绝。
// 已知来源的响应特征：
//   - DaoCloud: {"errors":[{"code":"DENIED","message":"…这镜像不在白名单.
//     this image is not in the allowlist."}]}
func isDenylistBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	low := strings.ToLower(string(body))
	return strings.Contains(low, "denied") ||
		strings.Contains(low, "allowlist") ||
		strings.Contains(low, "白名单")
}

// rewriteChallenge 把上游的 WWW-Authenticate 挑战改写给客户端：Bearer 挑战的
// realm 指向网关的 /token 端点（upstream 参数指定令牌换取的目标上游），
// service/scope 保留上游原值；Basic 挑战原样透传（docker 收到后会直接带
// 凭证重发请求，不访问 realm）。
func rewriteChallenge(raw, upstreamName, origin string) string {
	ch := parseChallenge(raw)
	if ch.Scheme != "bearer" {
		return raw
	}
	realm := origin + "/token?upstream=" + url.QueryEscape(upstreamName)
	v := `Bearer realm="` + realm + `"`
	if ch.Service != "" {
		v += `,service="` + ch.Service + `"`
	}
	if ch.Scope != "" {
		v += `,scope="` + ch.Scope + `"`
	}
	return v
}

// isHiddenNotFound 判断上游错误响应的 body 是否实际表达「不存在」语义。
// 部分镜像源（如 dockerproxy）把 registry 的 404 MANIFEST_UNKNOWN /
// BLOB_UNKNOWN 包装成 500 UNKNOWN 返回，真实错误码藏在 detail.errors 里，
// 从 body 错误码还原语义，避免把「镜像不存在」误判为源故障（误降级、误报 502）。
func isHiddenNotFound(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var parsed struct {
		Errors []struct {
			Code   string `json:"code"`
			Detail struct {
				Errors []struct {
					Code string `json:"code"`
				} `json:"errors"`
			} `json:"detail"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	for _, e := range parsed.Errors {
		if e.Code == "MANIFEST_UNKNOWN" || e.Code == "BLOB_UNKNOWN" || e.Code == "NAME_UNKNOWN" {
			return true
		}
		for _, de := range e.Detail.Errors {
			if de.Code == "MANIFEST_UNKNOWN" || de.Code == "BLOB_UNKNOWN" || de.Code == "NAME_UNKNOWN" {
				return true
			}
		}
	}
	return false
}

// Options 是 Proxy 的行为参数。
type Options struct {
	MaxManifestBytes int64
	// Activity 可选：外部构造的拉取记录器（例如开启文件持久化时）。
	// 留空则由 Proxy 创建纯内存记录器。
	Activity *activity.Tracker
}

// Proxy 实现 Docker Registry v2 的只读拉取代理与缓存。
type Proxy struct {
	router           *Router
	cache            cache.Store
	log              *slog.Logger
	maxManifestBytes int64
	activity         *activity.Tracker

	hits   *metrics.Counter
	misses *metrics.Counter
	bytes  *metrics.Counter
}

// NewProxy 创建代理。
func NewProxy(router *Router, c cache.Store, opts Options, log *slog.Logger, m *metrics.Collector) *Proxy {
	if log == nil {
		log = slog.Default()
	}
	if opts.MaxManifestBytes <= 0 {
		opts.MaxManifestBytes = 8 << 20
	}
	tracker := opts.Activity
	if tracker == nil {
		// 与持久化模式相同的保留上限，控制台的「保留上限」文案在两种模式下一致。
		tracker = activity.New(activity.DefaultStoreLimit)
	}
	p := &Proxy{
		router:           router,
		cache:            c,
		log:              log,
		maxManifestBytes: opts.MaxManifestBytes,
		activity:         tracker,
	}
	if m != nil {
		p.hits = m.Counter("mirror_gateway_cache_hits_total", "缓存命中次数", nil)
		p.misses = m.Counter("mirror_gateway_cache_misses_total", "缓存未命中次数", nil)
		p.bytes = m.Counter("mirror_gateway_bytes_served_total", "返回给客户端的字节数", nil)
	}
	return p
}

// 缓存键包含分组名，避免不同仓库的同名镜像互相覆盖。
func manifestKey(group, name, ref string, mutable bool, class string) string {
	base := group + "/" + name + "@" + ref
	if mutable {
		return base + "#" + class
	}
	return base
}

// Stats 是代理层的累计统计。
type Stats struct {
	CacheHits   int64 `json:"cacheHits"`
	CacheMisses int64 `json:"cacheMisses"`
	BytesServed int64 `json:"bytesServed"`
}

// Stats 返回累计统计。
func (p *Proxy) Stats() Stats {
	return Stats{
		CacheHits:   p.hits.Value(),
		CacheMisses: p.misses.Value(),
		BytesServed: p.bytes.Value(),
	}
}

// Activity 返回最近拉取记录，供控制台实时展示。
func (p *Proxy) Activity() *activity.Tracker {
	return p.activity
}

// ServeManifest 处理 /v2/<name>/manifests/<ref>。
func (p *Proxy) ServeManifest(w http.ResponseWriter, r *http.Request, name, ref string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "GET, HEAD")
		return
	}
	if !ValidRepositoryName(name) || !ValidReference(ref) {
		writeRegistryError(w, http.StatusBadRequest, "MANIFEST_INVALID", "无效的仓库名或引用", map[string]string{"name": name, "reference": ref})
		return
	}
	route, ok := p.resolve(w, r, name, ref)
	if !ok {
		return
	}
	name = route.Repo
	act := p.activity.Begin(activity.KindManifest, name, ref, route.Group.Name)
	defer act.Close()

	accept := r.Header.Get("Accept")
	class := AcceptClass(accept)
	mutable := !IsDigest(ref)
	key := manifestKey(route.Group.Name, name, ref, mutable, class)

	if m, ok := p.cache.GetManifest(key); ok {
		p.hits.Inc()
		act.SetUpstream(m.Upstream)
		act.SetBytes(int64(len(m.Body)))
		act.FinishCached()
		p.serveManifest(w, r, m, "hit", m.Upstream, route.Group.Name)
		return
	}
	p.misses.Inc()

	ctx := r.Context()
	var (
		lastErr       error
		notFound      int
		denied        int
		attempted     int
		authChallenge string // 第一个返回 401 挑战的上游原始挑战头
		challengeFrom string
	)
	// 客户端原始 Authorization 只是候选凭证：每个上游按自己的 auth 配置
	// 决定是否使用，config 源永远不会收到客户端凭证。
	clientAuth := r.Header.Get("Authorization")
	cands, prefetched := route.Group.Pool.CandidatesForManifest(ctx, name, ref, accept)
	for _, u := range cands {
		if ctx.Err() != nil {
			return
		}
		// 新旧探测时已经下载过这个 manifest，直接用它，避免再回源一次。
		if m := prefetched[u.Name]; m != nil {
			u.ReportTraffic(0)
			m.Upstream = u.Name
			m.Mutable = mutable
			act.SetUpstream(u.Name)
			act.SetCache("miss")
			act.SetBytes(int64(len(m.Body)))
			act.Finish()
			if !u.Passthrough() {
				p.storeManifest(route.Group.Name, name, ref, class, mutable, m)
			}
			p.serveManifest(w, r, m, "miss", u.Name, route.Group.Name)
			return
		}
		attempted++
		start := time.Now()
		m, ch, err := p.fetchManifest(ctx, u, name, ref, accept, mutable, clientAuth)
		p.log.Debug("manifest 拉取尝试", "upstream", u.Name, "ref", ref,
			"elapsed", time.Since(start).Round(time.Millisecond).String(), "err", err)
		if ch != "" && u.Passthrough() {
			// 该源要求客户端凭证（未登录、凭证无效/过期，或匿名被拒）：
			// 记下挑战继续尝试其他源，链尾把它改写后转给客户端。
			if authChallenge == "" {
				authChallenge, challengeFrom = ch, u.Name
			}
			continue
		}
		if err == nil {
			u.ReportTraffic(time.Since(start))
			m.Upstream = u.Name
			act.SetUpstream(u.Name)
			act.SetCache("miss")
			act.SetBytes(int64(len(m.Body)))
			act.Finish()
			if !u.Passthrough() {
				// 透传源的流量不写共享缓存：客户端身份换取的内容可能是私有镜像。
				p.storeManifest(route.Group.Name, name, ref, class, mutable, m)
			}
			p.serveManifest(w, r, m, "miss", u.Name, route.Group.Name)
			return
		}
		if errors.Is(err, errNotFound) {
			notFound++
			u.ReportSuccess(time.Since(start))
			act.SetUpstream(u.Name)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		// 失败也归因到具体上游（多次失败时保留最后一个，与返回给客户端
		// 的错误一致），否则控制台里失败记录的「上游」列是空的。
		act.SetUpstream(u.Name)
		var deniedErr *deniedError
		if errors.As(err, &deniedErr) {
			// 白名单/权限策略拒绝是该源对整个仓库的策略，不是源故障：
			// 不计入健康降级，否则探测镜像（常在白名单内）会让源在
			// 可用/不可用之间反复震荡。
			denied++
			lastErr = err
			continue
		}
		if errors.Is(err, ErrUnauthorized) {
			// 令牌接口拒绝（token 接口对不存在的仓库返回 403 DENIED）或
			// 401 无认证挑战——源在线但不对匿名提供这个仓库，同样是
			// 仓库级策略：不降级，换源重试。
			denied++
			lastErr = err
			continue
		}
		lastErr = err
		u.ReportFailure(err)
		p.log.Warn("从上游获取 manifest 失败", "group", route.Group.Name, "upstream", u.Name, "repo", name, "ref", ref, "error", err)
	}

	// 有 auth=passthrough 的上游返回 401 认证挑战（私有仓库、匿名配额用尽），
	// 把挑战改写后转给客户端——docker 会用登录凭证（或匿名）到网关的
	// /token 端点换取该上游的令牌，再带着令牌重试。
	if authChallenge != "" {
		act.Fail(errors.New("上游要求认证"))
		w.Header().Set("WWW-Authenticate", rewriteChallenge(authChallenge, challengeFrom, p.router.PublicOrigin(r)))
		writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "该仓库需要认证：请先 docker login 本站点，或检查登录凭证", nil)
		return
	}

	// 所有源都明确「不提供」（404 或 401/403 仓库级策略拒绝）时按 404 语义
	// 返回。典型场景：docker 拉取 OCI attestation manifest（platform=unknown）
	// 多数镜像站没有同步它；或 Elastic 等源对不存在的仓库统一返回
	// 401 authentication required——此时「镜像不存在」是最可能的真相，
	// 返回 502 + 401/403 详情只会误导客户端把整个 pull 中断。
	if notFound+denied == attempted {
		msg := "镜像不存在"
		if denied > 0 {
			if notFound > 0 {
				msg = fmt.Sprintf("各镜像源均未提供此镜像（%d 个源因白名单策略拒绝提供）", denied)
			} else {
				msg = fmt.Sprintf("各镜像源均拒绝提供此镜像（%d 个源要求认证或受白名单限制）", denied)
			}
		}
		act.Fail(errors.New(msg))
		writeRegistryError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", msg, map[string]string{"name": name, "reference": ref})
		return
	}
	act.Fail(lastErr)
	msg := "所有上游镜像源均不可用"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	if missing := notFound + denied; missing > 0 {
		// 混合场景：部分源故障（网络/5xx），其余源未收录此仓库。
		// 把「未收录」单独说明，避免用户误以为报错的那个源就是唯一原因。
		msg = fmt.Sprintf("%s（另有 %d 个镜像源未收录此镜像）", msg, missing)
	}
	writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", msg, nil)
}

// fetchManifest 从单个上游拉取清单。返回值 challenge 非 401 挑战相关：
// 上游返回 401 且带 WWW-Authenticate 时 resp 为 nil、challenge 为原始挑战头
// （透传模式下由上层改写后转给客户端；非透传上层按 err 处理）。
func (p *Proxy) fetchManifest(ctx context.Context, u *Upstream, name, ref, accept string, mutable bool, clientAuth string) (*cache.Manifest, string, error) {
	header := http.Header{}
	if strings.TrimSpace(accept) == "" {
		header.Set("Accept", DefaultManifestAccept)
	} else {
		header.Set("Accept", accept)
	}

	resp, challenge, err := u.DoAuth(ctx, http.MethodGet, manifestPath(name, ref), header, "repository:"+name+":pull", clientAuth)
	if err != nil {
		return nil, challenge, err
	}
	if resp == nil {
		return nil, challenge, nil
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, "", errNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		// 401 UNAUTHORIZED 是 registry 对不存在/私有仓库的标准响应（网关匿名
		// 拉取无法提供凭证）；403 白名单特征（如 DaoCloud DENIED）是仓库级
		// 策略——都不是源故障，换源重试且不计健康降级。
		if resp.StatusCode == http.StatusUnauthorized || isDenylistBody(body) {
			return nil, "", &deniedError{msg: fmt.Sprintf("%s 拒绝访问(%d): %s", u.Name, resp.StatusCode, snippet(body))}
		}
		// 无特征的 403 视为真实权限问题，计入健康降级。
		return nil, "", fmt.Errorf("%s 拒绝访问(%d): %s", u.Name, resp.StatusCode, snippet(body))
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		if resp.StatusCode == http.StatusInternalServerError && isHiddenNotFound(body) {
			return nil, "", errNotFound // 源把 404 包装成 500，按不存在处理
		}
		return nil, "", fmt.Errorf("%s 返回 %d: %s", u.Name, resp.StatusCode, snippet(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, p.maxManifestBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("读取 manifest 失败: %w", err)
	}
	if int64(len(body)) > p.maxManifestBytes {
		return nil, "", fmt.Errorf("manifest 超过 %d 字节上限", p.maxManifestBytes)
	}
	if len(body) == 0 {
		return nil, "", fmt.Errorf("%s 返回了空 manifest", u.Name)
	}

	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if IsDigest(ref) && !strings.EqualFold(ref, digest) {
		return nil, "", fmt.Errorf("%s 返回的 manifest digest 不匹配（期望 %s，实际 %s）", u.Name, ref, digest)
	}

	mediaType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	if !strings.HasPrefix(mediaType, "application/vnd.") {
		mediaType = GuessManifestMediaType(body)
	}
	return &cache.Manifest{Body: body, Digest: digest, MediaType: mediaType, Mutable: mutable}, "", nil
}

func (p *Proxy) storeManifest(group, name, ref, class string, mutable bool, m *cache.Manifest) {
	if p.cache.Backend() == "none" {
		return
	}
	m.FetchedAt = time.Now()
	m.Key = manifestKey(group, name, ref, mutable, class)
	if err := p.cache.PutManifest(m.Key, m); err != nil {
		p.log.Warn("写入 manifest 缓存失败", "key", m.Key, "error", err)
	}
	// 顺带按 digest 存一份，后续按 digest 拉取可命中缓存。
	if !IsDigest(ref) && m.Digest != "" {
		cp := *m
		cp.Mutable = false
		cp.Key = manifestKey(group, name, m.Digest, false, "")
		if err := p.cache.PutManifest(cp.Key, &cp); err != nil {
			p.log.Debug("写入 digest manifest 缓存失败", "key", cp.Key, "error", err)
		}
	}
}

func (p *Proxy) serveManifest(w http.ResponseWriter, r *http.Request, m *cache.Manifest, cacheState, upstream, group string) {
	h := w.Header()
	h.Set("Content-Type", m.MediaType)
	h.Set("Docker-Content-Digest", m.Digest)
	h.Set("ETag", `"`+m.Digest+`"`)
	if p.cache.Backend() == "none" {
		cacheState = "off"
	}
	h.Set("X-Mirror-Gateway-Cache", cacheState)
	if group != "" {
		h.Set("X-Mirror-Gateway-Group", group)
	}
	if upstream != "" {
		h.Set("X-Mirror-Gateway-Upstream", upstream)
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" && (inm == "*" || strings.Contains(inm, m.Digest)) {
		h.Del("Content-Length")
		w.WriteHeader(http.StatusNotModified)
		return
	}

	h.Set("Content-Length", strconv.Itoa(len(m.Body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if n, err := w.Write(m.Body); err != nil {
		p.log.Debug("写出 manifest 失败", "error", err)
	} else {
		p.bytes.Add(int64(n))
	}
}

// ServeBlob 处理 /v2/<name>/blobs/<digest>。
func (p *Proxy) ServeBlob(w http.ResponseWriter, r *http.Request, name, digest string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "GET, HEAD")
		return
	}
	if !ValidRepositoryName(name) || !IsDigest(digest) {
		writeRegistryError(w, http.StatusBadRequest, "DIGEST_INVALID", "无效的仓库名或 digest", map[string]string{"name": name, "digest": digest})
		return
	}

	// blob 缓存按 digest 全局共享，命中时无需解析分组。
	if size, ok := p.cache.HasBlob(digest); ok {
		f, _, err := p.cache.OpenBlob(digest)
		if err == nil {
			defer f.Close()
			p.hits.Inc()
			// 命中缓存也记一条，控制台能看出「这个层没有回源」。
			act := p.activity.Begin(activity.KindBlob, name, shortDigest(digest), "")
			act.SetBytes(size)
			act.FinishCached()
			h := w.Header()
			h.Set("Content-Type", MediaTypeOctetStream)
			h.Set("Docker-Content-Digest", digest)
			h.Set("ETag", `"`+digest+`"`)
			h.Set("X-Mirror-Gateway-Cache", "hit")
			p.cache.TouchBlob(digest)
			cw := &countingWriter{ResponseWriter: w}
			http.ServeContent(cw, r, "", time.Time{}, f)
			p.bytes.Add(cw.n)
			return
		}
		p.log.Warn("打开缓存 blob 失败，回源拉取", "digest", digest, "error", err)
	}

	p.misses.Inc()
	route, ok := p.resolve(w, r, name, "")
	if !ok {
		return
	}
	if r.Method == http.MethodHead {
		p.headBlob(w, r, route, digest)
		return
	}
	p.streamBlob(w, r, route, digest)
}

func (p *Proxy) headBlob(w http.ResponseWriter, r *http.Request, route *Route, digest string) {
	ctx := r.Context()
	name := route.Repo
	var (
		lastErr       error
		notFound      int
		denied        int
		attempted     int
		authChallenge string
		challengeFrom string
	)
	clientAuth := r.Header.Get("Authorization")
	for _, u := range route.Group.Pool.Candidates() {
		if ctx.Err() != nil {
			return
		}
		attempted++
		start := time.Now()
		resp, ch, err := u.DoAuth(ctx, http.MethodHead, blobPath(name, digest), nil, "repository:"+name+":pull", clientAuth)
		if ch != "" && u.Passthrough() {
			if authChallenge == "" {
				authChallenge, challengeFrom = ch, u.Name
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return // 客户端已断开，这不是上游的错，不计失败
			}
			lastErr = err
			if errors.Is(err, ErrUnauthorized) {
				// 令牌接口拒绝或 401 无挑战：仓库级策略而非源故障，不降级。
				denied++
				continue
			}
			u.ReportFailure(err)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusOK:
			size := resp.ContentLength
			u.ReportTraffic(time.Since(start))
			if size < 0 {
				if probed, err := p.probeBlobSize(ctx, u, name, digest); err == nil {
					size = probed
				}
			}
			h := w.Header()
			h.Set("Content-Type", MediaTypeOctetStream)
			h.Set("Docker-Content-Digest", digest)
			h.Set("Accept-Ranges", "bytes")
			h.Set("X-Mirror-Gateway-Cache", "miss")
			h.Set("X-Mirror-Gateway-Group", route.Group.Name)
			h.Set("X-Mirror-Gateway-Upstream", u.Name)
			if size >= 0 {
				h.Set("Content-Length", strconv.FormatInt(size, 10))
			}
			w.WriteHeader(http.StatusOK)
			return
		case resp.StatusCode == http.StatusNotFound:
			notFound++
			u.ReportSuccess(time.Since(start))
			continue
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			// HEAD 无响应体，但 401/403 状态码本身就表达仓库级策略拒绝
			//（不存在/私有仓库、白名单外仓库），不是源故障：不降级、换源。
			denied++
			lastErr = fmt.Errorf("%s 拒绝访问(%d)", u.Name, resp.StatusCode)
			continue
		default:
			lastErr = fmt.Errorf("%s 返回 %d", u.Name, resp.StatusCode)
			if isUpstreamFault(resp.StatusCode) {
				u.ReportFailure(lastErr)
			}
			continue
		}
	}

	if authChallenge != "" {
		// HEAD 响应不带 body，但 WWW-Authenticate 头足以触发 docker 换令牌。
		w.Header().Set("WWW-Authenticate", rewriteChallenge(authChallenge, challengeFrom, p.router.PublicOrigin(r)))
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if notFound+denied == attempted {
		writeRegistryError(w, http.StatusNotFound, "BLOB_UNKNOWN", "blob 不存在", map[string]string{"digest": digest})
		return
	}
	msg := "所有上游镜像源均不可用"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", msg, nil)
}

func (p *Proxy) streamBlob(w http.ResponseWriter, r *http.Request, route *Route, digest string) {
	ctx := r.Context()
	name := route.Repo
	rangeHeader := r.Header.Get("Range")
	act := p.activity.Begin(activity.KindBlob, name, shortDigest(digest), route.Group.Name)
	defer act.Close()
	var (
		lastErr       error
		notFound      int
		denied        int
		attempted     int
		authChallenge string
		challengeFrom string
	)
	clientAuth := r.Header.Get("Authorization")

	for _, u := range route.Group.Pool.Candidates() {
		if ctx.Err() != nil {
			return
		}
		attempted++
		start := time.Now()
		header := http.Header{}
		if rangeHeader != "" {
			header.Set("Range", rangeHeader)
		}

		resp, ch, err := u.DoAuth(ctx, http.MethodGet, blobPath(name, digest), header, "repository:"+name+":pull", clientAuth)
		if ch != "" && u.Passthrough() {
			// 上游要求认证：记下挑战换源，链尾改写后转给客户端。
			if authChallenge == "" {
				authChallenge, challengeFrom = ch, u.Name
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return // 客户端已断开，这不是上游的错，不计失败
			}
			lastErr = err
			act.SetUpstream(u.Name)
			if errors.Is(err, ErrUnauthorized) {
				// 令牌接口拒绝或 401 无挑战：源不对匿名提供这个仓库，
				// 仓库级策略而非源故障，不降级，换源重试。
				denied++
				continue
			}
			u.ReportFailure(err)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent:
			// 正常路径
		case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
			// 断点续传的 Range 超出文件大小时按规范透传 416（含上游的
			// Content-Range），否则 containerd 会一直重试同一个越界请求。
			p.writeBlobHeaders(w, resp, digest, route.Group.Name, u.Name, "miss")
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			act.SetUpstream(u.Name)
			act.SetCache("miss")
			act.Finish()
			return
		case resp.StatusCode == http.StatusNotFound:
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			notFound++
			u.ReportSuccess(time.Since(start))
			act.SetUpstream(u.Name)
			continue
		default:
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			if resp.StatusCode == http.StatusInternalServerError && isHiddenNotFound(body) {
				// 源把 404 BLOB_UNKNOWN 包装成 500，按不存在处理。
				notFound++
				u.ReportSuccess(time.Since(start))
				act.SetUpstream(u.Name)
				continue
			}
			if (resp.StatusCode == http.StatusForbidden && isDenylistBody(body)) || resp.StatusCode == http.StatusUnauthorized {
				// 401 UNAUTHORIZED（registry 对不存在/私有仓库的标准响应）
				// 与 403 白名单拒绝同属仓库级策略而非源故障：不降级健康度，
				// 照常归因并继续尝试下一个源。
				denied++
				lastErr = &deniedError{msg: fmt.Sprintf("%s 拒绝访问(%d): %s", u.Name, resp.StatusCode, snippet(body))}
				act.SetUpstream(u.Name)
				continue
			}
			lastErr = fmt.Errorf("%s 返回 %d: %s", u.Name, resp.StatusCode, snippet(body))
			act.SetUpstream(u.Name)
			if isUpstreamFault(resp.StatusCode) {
				u.ReportFailure(lastErr)
			}
			continue
		}

		u.ReportSuccess(time.Since(start))
		act.SetUpstream(u.Name)
		act.SetCache("miss")
		// Range 请求的 Content-Length 只是这一段的长度，不能当总大小用。
		if rangeHeader == "" {
			act.SetTotal(resp.ContentLength)
		}
		p.writeBlobHeaders(w, resp, digest, route.Group.Name, u.Name, "miss")

		var writer cache.Writer
		if !u.Passthrough() && rangeHeader == "" && resp.StatusCode == http.StatusOK && p.cache.Backend() != "none" {
			// 透传源的流量不写共享缓存：令牌属于客户端身份，其拉取的
			// 内容（可能是私有镜像）不应进入匿名共享缓存。
			writer, err = p.cache.NewBlobWriter(digest)
			if err != nil {
				p.log.Warn("创建缓存写入器失败，直接透传", "digest", digest, "error", err)
				writer = nil
			}
		}

		w.WriteHeader(resp.StatusCode)
		var written int64
		dst := &progressWriter{w: w, entry: act}
		// 只统计 body 传输阶段，避免把建连/TTFB 混进吞吐。
		transferStart := time.Now()
		if writer != nil {
			written, err = io.Copy(dst, io.TeeReader(resp.Body, writer))
			resp.Body.Close()
			if err != nil {
				writer.Abort()
				p.log.Debug("blob 传输中断，放弃缓存", "digest", digest, "error", err)
			} else if cerr := writer.Commit(); cerr != nil {
				p.log.Warn("blob 缓存落盘失败", "digest", digest, "error", cerr)
			}
		} else {
			written, err = io.Copy(dst, resp.Body)
			resp.Body.Close()
		}
		elapsed := time.Since(transferStart)
		// 真实拉取是吞吐数据最好的来源：零额外成本、完全反映当下体验。
		// 部分传输的样本若超过门槛（说明上游确实传得动）依然有效。
		u.ReportSpeed(written, elapsed)
		if err != nil {
			// 传输中断（最常见的客户端断开不是上游的错，不计失败）。
			if ctx.Err() == nil {
				u.ReportFailure(err)
			}
			act.Fail(err)
		} else {
			// 完整传输算一次成功的真实业务流量：窗口内的定时健康检查可以放心跳过。
			u.ReportTraffic(0)
			u.AddBytes(written)
			act.Finish()
		}
		p.bytes.Add(written)
		return
	}

	if authChallenge != "" {
		act.Fail(errors.New("上游要求认证"))
		w.Header().Set("WWW-Authenticate", rewriteChallenge(authChallenge, challengeFrom, p.router.PublicOrigin(r)))
		writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "该镜像层需要认证：请先 docker login 本站点，或检查登录凭证", nil)
		return
	}

	if notFound+denied == attempted {
		msg := "blob 不存在"
		if denied > 0 {
			if notFound > 0 {
				msg = fmt.Sprintf("各镜像源均未提供此 blob（%d 个源因白名单策略拒绝提供）", denied)
			} else {
				msg = fmt.Sprintf("各镜像源均拒绝提供此 blob（%d 个源要求认证或受白名单限制）", denied)
			}
		}
		act.Fail(errors.New(msg))
		writeRegistryError(w, http.StatusNotFound, "BLOB_UNKNOWN", msg, map[string]string{"digest": digest})
		return
	}
	act.Fail(lastErr)
	msg := "所有上游镜像源均不可用"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	if missing := notFound + denied; missing > 0 {
		// 混合场景：部分源故障（网络/5xx），其余源未收录此仓库。
		// 把「未收录」单独说明，避免用户误以为报错的那个源就是唯一原因。
		msg = fmt.Sprintf("%s（另有 %d 个镜像源未收录此镜像）", msg, missing)
	}
	writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", msg, nil)
}

func (p *Proxy) writeBlobHeaders(w http.ResponseWriter, resp *http.Response, digest, group, upstream, cacheState string) {
	h := w.Header()
	h.Set("Content-Type", MediaTypeOctetStream)
	h.Set("Docker-Content-Digest", digest)
	h.Set("Accept-Ranges", "bytes")
	if p.cache.Backend() == "none" {
		cacheState = "off"
	}
	h.Set("X-Mirror-Gateway-Cache", cacheState)
	h.Set("X-Mirror-Gateway-Group", group)
	h.Set("X-Mirror-Gateway-Upstream", upstream)
	if resp.ContentLength >= 0 {
		h.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		h.Set("Content-Range", cr)
	}
}

// probeBlobSize 通过 Range: bytes=0-0 获取 blob 总大小。
func (p *Proxy) probeBlobSize(ctx context.Context, u *Upstream, name, digest string) (int64, error) {
	header := http.Header{}
	header.Set("Range", "bytes=0-0")
	resp, err := u.Do(ctx, http.MethodGet, blobPath(name, digest), header, "repository:"+name+":pull")
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64))
	if resp.StatusCode != http.StatusPartialContent {
		return -1, fmt.Errorf("上游未返回 206")
	}
	_, total, ok := strings.Cut(resp.Header.Get("Content-Range"), "/")
	if !ok {
		return -1, fmt.Errorf("无法解析 Content-Range")
	}
	return strconv.ParseInt(total, 10, 64)
}

// ServeToken 处理挑战改写后的令牌换取：/token?upstream=<name>&scope=<scope>。
//
// 客户端（docker）带着 docker login 的凭证来请求本端点，网关先向上游 /v2/
// 探测真实的认证挑战获得 realm 与 service（不信任客户端传入的 realm，防止
// 本端点被用作向任意地址转发凭证的跳板），再带上客户端凭证转发令牌请求。
// 凭证只会发给该上游自己的令牌服务；令牌不进网关缓存，由客户端自己持有。
func (p *Proxy) ServeToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "GET, HEAD")
		return
	}
	upstreamName := r.URL.Query().Get("upstream")
	scope := r.URL.Query().Get("scope")

	// 只允许为该源自身配置 auth=passthrough 的上游换取令牌。
	// 不再按分组或 Host 推断：认证方式完全由源配置决定，
	// 多域名部署的安全责任由部署者承担（见配置文档）。
	u, _ := p.router.ByName(upstreamName)
	if u == nil || !u.Passthrough() {
		writeRegistryError(w, http.StatusNotFound, "UNSUPPORTED", "无效的令牌服务目标", map[string]string{"upstream": upstreamName})
		return
	}

	ch, err := u.Challenge(r.Context())
	if err != nil {
		writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", "探测上游认证失败: "+err.Error(), nil)
		return
	}

	body, err := u.ForwardToken(r.Context(), ch, scope, r.Header.Get("Authorization"))
	if err != nil {
		act := p.activity.Begin(activity.KindManifest, "token:"+upstreamName, "token", "")
		act.Fail(err)
		writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "上游令牌服务拒绝: "+err.Error(), nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// ServeTags 处理 /v2/<name>/tags/list，做简单透传。
func (p *Proxy) ServeTags(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "GET, HEAD")
		return
	}
	if !ValidRepositoryName(name) {
		writeRegistryError(w, http.StatusBadRequest, "NAME_INVALID", "无效的仓库名", map[string]string{"name": name})
		return
	}
	route, ok := p.resolve(w, r, name, "")
	if !ok {
		return
	}
	name = route.Repo

	path := tagsPath(name)
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	ctx := r.Context()
	var (
		lastErr       error
		notFound      int
		denied        int
		attempted     int
		authChallenge string
		challengeFrom string
	)
	clientAuth := r.Header.Get("Authorization")
	for _, u := range route.Group.Pool.Candidates() {
		if ctx.Err() != nil {
			return
		}
		attempted++
		start := time.Now()
		resp, ch, err := u.DoAuth(ctx, http.MethodGet, path, nil, "repository:"+name+":pull", clientAuth)
		if ch != "" && u.Passthrough() {
			if authChallenge == "" {
				authChallenge, challengeFrom = ch, u.Name
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return // 客户端已断开，这不是上游的错，不计失败
			}
			lastErr = err
			if errors.Is(err, ErrUnauthorized) {
				// 上游令牌接口拒绝或 401 无挑战：仓库级策略，不降级源。
				denied++
				continue
			}
			u.ReportFailure(err)
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			notFound++
			u.ReportSuccess(time.Since(start))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			if resp.StatusCode == http.StatusInternalServerError && isHiddenNotFound(body) {
				// 源把 404 NAME_UNKNOWN/MANIFEST_UNKNOWN 包装成 500，按不存在处理。
				notFound++
				u.ReportSuccess(time.Since(start))
				continue
			}
			if resp.StatusCode == http.StatusUnauthorized || (resp.StatusCode == http.StatusForbidden && isDenylistBody(body)) {
				// 仓库级策略拒绝，不是源故障。
				denied++
				lastErr = fmt.Errorf("%s 拒绝访问(%d): %s", u.Name, resp.StatusCode, snippet(body))
				continue
			}
			lastErr = fmt.Errorf("%s 返回 %d: %s", u.Name, resp.StatusCode, snippet(body))
			if isUpstreamFault(resp.StatusCode) {
				u.ReportFailure(lastErr)
			}
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if err != nil {
			// 读取失败（多为客户端断开）：断开不算上游的错，也不算成功流量。
			if ctx.Err() == nil {
				u.ReportFailure(err)
			}
			lastErr = err
			continue
		}
		u.ReportTraffic(time.Since(start))
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Content-Length", strconv.Itoa(len(body)))
		if link := resp.Header.Get("Link"); link != "" {
			// 分页游标必须透传，否则 containerd 列大仓库的标签时只拿到第一页。
			h.Set("Link", link)
		}
		h.Set("X-Mirror-Gateway-Group", route.Group.Name)
		h.Set("X-Mirror-Gateway-Upstream", u.Name)
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			if n, err := w.Write(body); err == nil {
				p.bytes.Add(int64(n))
			}
		}
		return
	}

	if authChallenge != "" {
		w.Header().Set("WWW-Authenticate", rewriteChallenge(authChallenge, challengeFrom, p.router.PublicOrigin(r)))
		writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "该仓库需要认证：请先 docker login 本站点，或检查登录凭证", nil)
		return
	}

	if notFound+denied == attempted {
		msg := "仓库不存在"
		if denied > 0 {
			msg = "各镜像源均未提供此仓库（部分源要求认证或受白名单限制）"
		}
		writeRegistryError(w, http.StatusNotFound, "NAME_UNKNOWN", msg, map[string]string{"name": name})
		return
	}
	msg := "所有上游镜像源均不可用"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	if missing := notFound + denied; missing > 0 {
		msg = fmt.Sprintf("%s（另有 %d 个镜像源未收录此仓库）", msg, missing)
	}
	writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", msg, nil)
}

// maxCatalogPage 是 /v2/_catalog 单页返回的仓库数上限。
const maxCatalogPage = 1000

// ServeCatalog 处理 /v2/_catalog：按名称排序列出拉取记录中出现过的仓库。
//
// 网关是拉取代理，无法枚举上游全部仓库；这里把「本地见过的仓库」当作目录，
// 让依赖 _catalog 的第三方 UI 能正常浏览，而不是收到 404。
// 支持 Registry v2 规范的 n / last 分页。
func (p *Proxy) ServeCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "GET, HEAD")
		return
	}
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n > maxCatalogPage {
		n = maxCatalogPage
	}

	repos := p.knownRepositories()
	start := 0
	if last := r.URL.Query().Get("last"); last != "" {
		// last 是上一页最后一个仓库名，下一页从它之后开始。
		start = sort.Search(len(repos), func(i int) bool { return repos[i] > last })
	}
	end := start + n
	if end > len(repos) {
		end = len(repos)
	}
	page := repos[start:end]
	if page == nil {
		page = []string{}
	}
	if end < len(repos) && len(page) > 0 {
		q := url.Values{}
		q.Set("n", strconv.Itoa(n))
		q.Set("last", page[len(page)-1])
		w.Header().Set("Link", fmt.Sprintf("</v2/_catalog?%s>; rel=\"next\"", q.Encode()))
	}
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string][]string{"repositories": page})
}

// knownRepositories 返回拉取记录中出现过的仓库名（去重、按字母排序）。
func (p *Proxy) knownRepositories() []string {
	records := p.activity.List(0)
	seen := make(map[string]struct{}, len(records))
	repos := make([]string, 0, len(records))
	for _, rec := range records {
		if rec.Repo == "" {
			continue
		}
		if _, ok := seen[rec.Repo]; ok {
			continue
		}
		seen[rec.Repo] = struct{}{}
		repos = append(repos, rec.Repo)
	}
	sort.Strings(repos)
	return repos
}

// resolve 把仓库名解析到分组；ref 非空时用该 manifest 做精确判定。
func (p *Proxy) resolve(w http.ResponseWriter, r *http.Request, name, ref string) (*Route, bool) {
	var (
		route *Route
		err   error
	)
	if ref != "" {
		route, err = p.router.ResolveManifest(r.Context(), r.Host, name, ref)
	} else {
		route, err = p.router.Resolve(r.Context(), r.Host, name)
	}
	if err != nil {
		if r.Context().Err() != nil {
			return nil, false
		}
		switch {
		case errors.Is(err, ErrInvalidRepository):
			writeRegistryError(w, http.StatusBadRequest, "NAME_INVALID", "无效的仓库名", map[string]string{"name": name})
		case errors.Is(err, ErrRepoNotFound):
			writeRegistryError(w, http.StatusNotFound, "NAME_UNKNOWN", "仓库不存在", map[string]string{"name": name})
		default:
			writeRegistryError(w, http.StatusBadGateway, "UNKNOWN", err.Error(), map[string]string{"name": name})
		}
		return nil, false
	}
	return route, true
}

func isUpstreamFault(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests || status == http.StatusRequestTimeout
}

type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}

// progressWriter 把写入量同步到活动记录，用于计算实时速度。
// 这里只做一次原子累加，成本可以忽略。
type progressWriter struct {
	w     io.Writer
	entry *activity.Entry
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		p.entry.AddBytes(int64(n))
	}
	return n, err
}

// shortDigest 把 sha256:xxxx 缩成便于展示的短形式。
func shortDigest(d string) string {
	alg, sum, ok := strings.Cut(d, ":")
	if !ok {
		if len(d) > 16 {
			return d[:16]
		}
		return d
	}
	if len(sum) > 12 {
		sum = sum[:12]
	}
	return alg + ":" + sum
}
