package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/activity"
	"github.com/midaug/docker-mirror-gateway/internal/cache"
	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

type fakeManifest struct {
	body      []byte
	mediaType string
	digest    string
}

// fakeRegistry 模拟一个带 token 认证的 Docker Registry。
type fakeRegistry struct {
	*httptest.Server
	t                 *testing.T
	token             string
	manifests         map[string]fakeManifest
	blobs             map[string][]byte
	fail              bool
	deny              map[string]bool // 这些仓库返回 403 DENIED 白名单响应
	plainForbidden    map[string]bool // 这些仓库返回无特征的 403
	unauthorizedRepos map[string]bool // 这些仓库返回 401 authentication required
	wrapNotFound      bool            // 把 404 包装成 500 UNKNOWN（模拟 dockerproxy）
	tokenDeniedScopes map[string]bool // token 接口对这些 scope 返回 403（模拟 ghcr.io）
	tokenUser         string          // token 接口要求的 Basic 账号（空 = 不校验）
	tokenPass         string
	alwaysChallenge   bool // 数据接口忽略凭证恒返回 401 挑战（模拟令牌权限不足）
	manifestReqs      atomic.Int64
	blobReqs          atomic.Int64
	tagsReqs          atomic.Int64
	seen              atomic.Value
	lastAuth          atomic.Value // 数据接口最近一次收到的 Authorization
	lastTokenAuth     atomic.Value // token 接口最近一次收到的 Authorization
}

// writeWrappedNotFound 模拟 dockerproxy 把 404 包装成 500 UNKNOWN 的响应，
// 真实错误码藏在 detail.errors[0].code 里。
func (f *fakeRegistry) writeWrappedNotFound(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = io.WriteString(w, `{"errors":[{"code":"UNKNOWN","message":"unknown error","detail":{"errors":[{"code":"`+code+`","message":"manifest unknown","detail":{}}]}}]}`)
}

func newFakeRegistry(t *testing.T) *fakeRegistry {
	t.Helper()
	f := &fakeRegistry{
		t:         t,
		token:     "test-token",
		manifests: map[string]fakeManifest{},
		blobs:     map[string][]byte{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		f.lastTokenAuth.Store(r.Header.Get("Authorization"))
		// 模拟 token 接口对特定仓库 scope 返回 403 DENIED（ghcr.io 对
		// 不存在仓库的匿名令牌请求行为）
		if f.tokenDeniedScopes[r.URL.Query().Get("scope")] {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"errors":[{"code":"DENIED","message":"requested access to the resource is denied"}]}`)
			return
		}
		if f.tokenUser != "" {
			// 模拟真实 registry：只有用正确账号换 token 才发放有效凭证
			if u, p, ok := r.BasicAuth(); !ok || u != f.tokenUser || p != f.tokenPass {
				w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="%s/token"`, f.Server.URL))
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"token":"`+f.token+`","expires_in":300}`)
	})
	mux.HandleFunc("/v2/", f.handle)
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Server.Close)
	return f
}

func (f *fakeRegistry) addManifest(name, ref string, mediaType string, body []byte) string {
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	f.manifests[name+"@"+ref] = fakeManifest{body: body, mediaType: mediaType, digest: digest}
	return digest
}

func (f *fakeRegistry) handle(w http.ResponseWriter, r *http.Request) {
	f.lastAuth.Store(r.Header.Get("Authorization"))
	kind, name, ref := ParseV2Path(strings.TrimPrefix(r.URL.Path, "/v2/"))
	if f.handleAlwaysChallenge(w, name) {
		return
	}
	if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fake-registry"`, f.Server.URL))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if f.fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	f.seen.Store(r.URL.Path)

	if f.deny[name] {
		// 模拟 DaoCloud public-image-mirror 的白名单拒绝
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"errors":[{"code":"DENIED","message":"这镜像不在白名单. this image is not in the allowlist."}]}`)
		return
	}
	if f.plainForbidden[name] {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if f.unauthorizedRepos[name] {
		// 模拟 Elastic 等源：对不存在/私有的仓库返回 401 authentication required
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`)
		return
	}
	switch kind {
	case PathManifest:
		f.manifestReqs.Add(1)
		m, ok := f.manifests[name+"@"+ref]
		if !ok {
			if f.wrapNotFound {
				f.writeWrappedNotFound(w, "MANIFEST_UNKNOWN")
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		w.Header().Set("Content-Type", m.mediaType)
		w.Header().Set("Docker-Content-Digest", m.digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(m.body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(m.body)
	case PathTags:
		f.tagsReqs.Add(1)
		prefix := name + "@"
		tags := make([]string, 0, 2)
		for k := range f.manifests {
			if strings.HasPrefix(k, prefix) {
				if ref := strings.TrimPrefix(k, prefix); !IsDigest(ref) {
					tags = append(tags, ref)
				}
			}
		}
		if len(tags) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		body, _ := json.Marshal(map[string]any{"name": name, "tags": tags})
		_, _ = w.Write(body)
	case PathBlob:
		f.blobReqs.Add(1)
		data, ok := f.blobs[ref]
		if !ok {
			if f.wrapNotFound {
				f.writeWrappedNotFound(w, "BLOB_UNKNOWN")
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(data)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[:1])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", ref)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newTestProxy(t *testing.T, upstreams []config.UpstreamConfig, c *cache.Cache) *Proxy {
	t.Helper()
	p, _ := newTestProxyWithRouter(t, upstreams, c)
	return p
}

func newTestProxyWithRouter(t *testing.T, upstreams []config.UpstreamConfig, c *cache.Cache) (*Proxy, *Router) {
	t.Helper()
	log := testLogger()
	m := metrics.New()
	cfg := config.Default()
	cfg.Groups = []config.GroupConfig{{
		Name:             "test",
		Priority:         10,
		Aliases:          []string{"test", "docker.io"},
		Default:          true,
		LibraryNamespace: "library",
		Upstreams:        upstreams,
	}}
	cfg.Routing.AutoDetect = false
	cfg.Routing.DefaultGroup = "test"
	cfg.Health.Timeout = config.Duration(2 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	router, err := NewRouter(cfg, log, m)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return NewProxy(router, c, Options{MaxManifestBytes: 1 << 20}, log, m), router
}

func newTestCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.New(cache.Config{Dir: t.TempDir(), MaxBytes: 64 << 20, TagTTL: time.Minute})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	return c
}

func TestProxyManifestAndBlob(t *testing.T) {
	f := newFakeRegistry(t)
	manifestBody := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[]}`)
	digest := f.addManifest("library/alpine", "latest", MediaTypeDockerManifestList, manifestBody)

	blob := []byte("hello-blob-content")
	blobSum := sha256.Sum256(blob)
	blobDigest := "sha256:" + hex.EncodeToString(blobSum[:])
	f.blobs[blobDigest] = blob

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}}, c)

	accept := MediaTypeDockerManifestList + ", " + MediaTypeDockerManifest

	// 1. 首次拉取 manifest（未命中缓存）
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	req.Header.Set("Accept", accept)
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "alpine", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("manifest 状态码 = %d, 期望 200, body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Docker-Content-Digest"); got != digest {
		t.Errorf("Docker-Content-Digest = %q, 期望 %q", got, digest)
	}
	if got := rec.Header().Get("Content-Type"); got != MediaTypeDockerManifestList {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("X-Mirror-Gateway-Upstream"); got != "fake" {
		t.Errorf("X-Mirror-Gateway-Upstream = %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), manifestBody) {
		t.Errorf("manifest body 不一致")
	}
	if path, _ := f.seen.Load().(string); path != "/v2/library/alpine/manifests/latest" {
		t.Errorf("上游收到的路径 = %q，说明名称归一化失败", path)
	}

	// 2. 再次拉取应命中缓存
	rec = httptest.NewRecorder()
	p.ServeManifest(rec, req.Clone(req.Context()), "alpine", "latest")
	if got := rec.Header().Get("X-Mirror-Gateway-Cache"); got != "hit" {
		t.Errorf("第二次请求缓存状态 = %q, 期望 hit", got)
	}
	if n := f.manifestReqs.Load(); n != 1 {
		t.Errorf("上游 manifest 请求次数 = %d, 期望 1", n)
	}

	// 3. 按 digest 拉取也应命中（写入缓存时同时存了 digest key）
	rec = httptest.NewRecorder()
	digestReq := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/"+digest, nil)
	digestReq.Header.Set("Accept", accept)
	p.ServeManifest(rec, digestReq, "library/alpine", digest)
	if rec.Code != http.StatusOK || rec.Header().Get("X-Mirror-Gateway-Cache") != "hit" {
		t.Errorf("按 digest 拉取未命中缓存: code=%d cache=%q", rec.Code, rec.Header().Get("X-Mirror-Gateway-Cache"))
	}
	if n := f.manifestReqs.Load(); n != 1 {
		t.Errorf("按 digest 拉取后上游请求次数 = %d, 期望仍为 1", n)
	}

	// 4. HEAD blob（未缓存）
	rec = httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodHead, "/v2/library/alpine/blobs/"+blobDigest, nil), "library/alpine", blobDigest)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD blob 状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(blob)) {
		t.Errorf("HEAD blob Content-Length = %q, 期望 %d", got, len(blob))
	}

	// 5. GET blob，应边下发边缓存
	rec = httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil), "library/alpine", blobDigest)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET blob 状态码 = %d", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), blob) {
		t.Errorf("blob 内容不一致")
	}
	if size, ok := c.HasBlob(blobDigest); !ok || size != int64(len(blob)) {
		t.Errorf("blob 未落盘, ok=%v size=%d", ok, size)
	}

	// 6. 再次 GET 应命中缓存
	before := f.blobReqs.Load()
	rec = httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil), "library/alpine", blobDigest)
	if got := rec.Header().Get("X-Mirror-Gateway-Cache"); got != "hit" {
		t.Errorf("第二次 blob 缓存状态 = %q", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), blob) {
		t.Errorf("缓存 blob 内容不一致")
	}
	if after := f.blobReqs.Load(); after != before {
		t.Errorf("缓存命中后仍回源: %d -> %d", before, after)
	}

	stats := p.Stats()
	if stats.CacheHits == 0 || stats.CacheMisses == 0 {
		t.Errorf("缓存统计异常: %+v", stats)
	}
}

func TestProxyFailover(t *testing.T) {
	bad := newFakeRegistry(t)
	bad.fail = true
	good := newFakeRegistry(t)

	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`)
	digest := good.addManifest("library/nginx", "latest", MediaTypeDockerManifest, body)

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "bad", URL: bad.URL, Priority: 10},
		{Name: "good", URL: good.URL, Priority: 20},
	}, c)

	req := httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil)
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "library/nginx", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("故障转移后状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Mirror-Gateway-Upstream"); got != "good" {
		t.Errorf("实际服务上游 = %q, 期望 good", got)
	}
	if got := rec.Header().Get("Docker-Content-Digest"); got != digest {
		t.Errorf("digest = %q", got)
	}
}

func TestProxyManifestNotFound(t *testing.T) {
	f := newFakeRegistry(t)
	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}}, c)

	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/missing/manifests/latest", nil), "library/missing", "latest")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "MANIFEST_UNKNOWN") {
		t.Errorf("错误码不符合 Registry 规范: %s", rec.Body.String())
	}
}

func TestProxyRejectsInvalidInput(t *testing.T) {
	f := newFakeRegistry(t)
	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}}, c)

	rec := httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/not-a-digest", nil), "library/alpine", "not-a-digest")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("非法 digest 状态码 = %d, 期望 400", rec.Code)
	}

	rec = httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodPut, "/v2/library/alpine/manifests/latest", nil), "library/alpine", "latest")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT 状态码 = %d, 期望 405", rec.Code)
	}
}

func TestProxyBlobDigestMismatchNotCached(t *testing.T) {
	f := newFakeRegistry(t)
	// 上游返回的内容与请求的 digest 不一致
	wrong := []byte("actual-content")
	sum := sha256.Sum256(wrong)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	f.blobs[digest] = []byte("tampered-content")

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}}, c)

	rec := httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+digest, nil), "library/alpine", digest)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if _, ok := c.HasBlob(digest); ok {
		t.Error("digest 校验失败的内容不应写入缓存")
	}
}

// 拉取活动应被完整记录：manifest 一条、blob 一条，按时间倒序输出。
func TestProxyRecordsActivity(t *testing.T) {
	f := newFakeRegistry(t)
	manifestBody := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`)
	f.addManifest("library/alpine", "1.0", MediaTypeDockerManifest, manifestBody)

	blob := bytes.Repeat([]byte("a"), 8192)
	sum := sha256.Sum256(blob)
	blobDigest := "sha256:" + hex.EncodeToString(sum[:])
	f.blobs[blobDigest] = blob

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}}, c)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/1.0", nil)
	req.Header.Set("Accept", MediaTypeDockerManifest)
	p.ServeManifest(rec, req, "library/alpine", "1.0")
	if rec.Code != http.StatusOK {
		t.Fatalf("manifest 状态码 = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil), "library/alpine", blobDigest)
	if rec.Code != http.StatusOK {
		t.Fatalf("blob 状态码 = %d", rec.Code)
	}

	items := p.Activity().List(0)
	if len(items) != 2 {
		t.Fatalf("记录条数 = %d, 期望 2: %+v", len(items), items)
	}
	blobRec := items[0]
	if blobRec.Kind != activity.KindBlob || blobRec.State != activity.StateDone {
		t.Errorf("最新记录 = %+v", blobRec)
	}
	if blobRec.Upstream != "fake" || blobRec.Group != "test" {
		t.Errorf("上游/分组 = %q/%q", blobRec.Upstream, blobRec.Group)
	}
	if blobRec.Bytes != int64(len(blob)) {
		t.Errorf("记录字节 = %d, 期望 %d", blobRec.Bytes, len(blob))
	}
	if blobRec.Reference != shortDigest(blobDigest) {
		t.Errorf("引用 = %q", blobRec.Reference)
	}

	manifestRec := items[1]
	if manifestRec.Kind != activity.KindManifest || manifestRec.Reference != "1.0" {
		t.Errorf("manifest 记录 = %+v", manifestRec)
	}
	if manifestRec.Bytes != int64(len(manifestBody)) {
		t.Errorf("manifest 字节 = %d", manifestRec.Bytes)
	}
	if manifestRec.State != activity.StateDone {
		t.Errorf("manifest 状态 = %s", manifestRec.State)
	}

	// 再拉一次全部命中缓存，应记为 cached。
	// 注意：manifest 缓存键包含 Accept 分类，这里要带同样的 Accept 才会命中。
	hitReq := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/1.0", nil)
	hitReq.Header.Set("Accept", MediaTypeDockerManifest)
	rec = httptest.NewRecorder()
	p.ServeManifest(rec, hitReq, "library/alpine", "1.0")
	if got := rec.Header().Get("X-Mirror-Gateway-Cache"); got != "hit" {
		t.Fatalf("第二次 manifest 缓存状态 = %q", got)
	}
	rec = httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/alpine/blobs/"+blobDigest, nil), "library/alpine", blobDigest)
	items = p.Activity().List(0)
	if items[0].State != activity.StateCached || items[1].State != activity.StateCached {
		t.Errorf("缓存命中未记录: %s / %s", items[0].State, items[1].State)
	}

	sum2 := p.Activity().Summary()
	if sum2.Total != 4 || sum2.Cached != 2 {
		t.Errorf("汇总 = %+v", sum2)
	}
}

// 拉取带 attestation 的镜像时，健康源可能一致 404（镜像站未同步
// platform=unknown 的 attestation manifest），白名单源（如 DaoCloud）
// 兜底返回 403 DENIED。此时应按 404 语义返回，而不是把兜底源的 403
// 包成 502 中断整个 pull；且白名单拒绝是仓库级策略，不能降级源健康度。
func TestProxyManifestDenylistFallback(t *testing.T) {
	ok := newFakeRegistry(t) // 该仓库存在但请求的 manifest 不存在 → 404
	// 占位 manifest 保证 registry 本身可用，但 digest 与请求的不同
	ok.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))
	deny := newFakeRegistry(t)
	deny.deny = map[string]bool{"library/nginx": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "deny", URL: deny.URL, Priority: 20},
	}, c)

	missing := "sha256:" + strings.Repeat("ab", 32)
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/"+missing, nil), "library/nginx", missing)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404（而非兜底源 403 的 502）, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "MANIFEST_UNKNOWN") {
		t.Errorf("错误码不符合 Registry 规范: %s", rec.Body.String())
	}

	// 失败记录应归因到最后一个被尝试的上游（deny），而不是空白
	items := p.Activity().List(0)
	if len(items) == 0 || items[0].State != activity.StateFailed {
		t.Fatalf("活动记录 = %+v", items)
	}
	if items[0].Upstream != "deny" {
		t.Errorf("失败记录上游 = %q, 期望 deny", items[0].Upstream)
	}

	// 白名单 403 不计入健康降级：deny 源应保持健康
	router := p.router
	var pool *Pool
	for _, g := range router.Groups() {
		if g.Name == "test" {
			pool = g.Pool
		}
	}
	if pool == nil {
		t.Fatal("找不到 test 分组")
	}
	for _, u := range pool.Upstreams() {
		if u.Name == "deny" && !u.Healthy() {
			t.Errorf("白名单 403 不应降级 deny 源健康度")
		}
	}
}

// 无白名单特征的 403（真实权限问题）仍然计入健康降级。
func TestProxyManifestPlainForbiddenDegrades(t *testing.T) {
	f := newFakeRegistry(t)
	f.plainForbidden = map[string]bool{"library/nginx": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "fake", URL: f.URL, Priority: 10},
	}, c)

	for i := 0; i < 3; i++ { // failThreshold = 3
		rec := httptest.NewRecorder()
		p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil), "library/nginx", "latest")
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("第 %d 次状态码 = %d, 期望 502", i+1, rec.Code)
		}
	}

	router := p.router
	for _, g := range router.Groups() {
		if g.Name == "test" {
			for _, u := range g.Pool.Upstreams() {
				if u.Name == "fake" && u.Healthy() {
					t.Errorf("无特征的 403 连续 %d 次后应降级健康度", 3)
				}
			}
		}
	}
}

// blob 路径同样：健康源 404 + 白名单源 403 → 404 BLOB_UNKNOWN。
func TestProxyBlobDenylistFallback(t *testing.T) {
	ok := newFakeRegistry(t)
	deny := newFakeRegistry(t)
	deny.deny = map[string]bool{"library/nginx": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "deny", URL: deny.URL, Priority: 20},
	}, c)

	missing := "sha256:" + strings.Repeat("cd", 32)
	rec := httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/blobs/"+missing, nil), "library/nginx", missing)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "BLOB_UNKNOWN") {
		t.Errorf("错误码不符合 Registry 规范: %s", rec.Body.String())
	}

	items := p.Activity().List(0)
	if len(items) == 0 || items[0].Upstream != "deny" {
		t.Errorf("失败记录上游 = %+v, 期望 deny", items)
	}
}

// Elastic 等源对不存在/私有的仓库返回 401 authentication required（而非 404）。
// 若另一源正常返回 404，链尾应按 404 MANIFEST_UNKNOWN 返回（镜像不存在是最可能
// 的真相），而不是把 401 包成 502；且 401 不应降级源健康度。
func TestProxyManifestUnauthorizedAsPolicy(t *testing.T) {
	ok := newFakeRegistry(t) // 不存在的 manifest → 404
	elastic := newFakeRegistry(t)
	elastic.unauthorizedRepos = map[string]bool{"library/nginx": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "elastic", URL: elastic.URL, Priority: 20},
	}, c)

	missing := "sha256:" + strings.Repeat("ef", 32)
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/"+missing, nil), "library/nginx", missing)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404（而非 401 包成的 502）, body=%s", rec.Code, rec.Body.String())
	}

	// 失败归因到最后尝试的源
	items := p.Activity().List(0)
	if len(items) == 0 || items[0].Upstream != "elastic" {
		t.Errorf("失败记录上游 = %+v, 期望 elastic", items)
	}

	// 401 不应降级源健康度
	for _, g := range p.router.Groups() {
		if g.Name == "test" {
			for _, u := range g.Pool.Upstreams() {
				if u.Name == "elastic" && !u.Healthy() {
					t.Errorf("401 不应降级 elastic 源健康度")
				}
			}
		}
	}
}

// dockerproxy 等源把 404 MANIFEST_UNKNOWN/BLOB_UNKNOWN 包装成 500 UNKNOWN。
// 网关应从 body 错误码还原 404 语义（换源、不降级），而不是误判为源故障。
func TestProxyManifestWrappedNotFound(t *testing.T) {
	ok := newFakeRegistry(t)
	wrapped := newFakeRegistry(t)
	wrapped.wrapNotFound = true

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "wrapped", URL: wrapped.URL, Priority: 20},
	}, c)

	missing := "sha256:" + strings.Repeat("12", 32)
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/"+missing, nil), "library/nginx", missing)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404, body=%s", rec.Code, rec.Body.String())
	}

	// wrapped 源把 404 包装成 500，不应被降级健康度
	for _, g := range p.router.Groups() {
		if g.Name == "test" {
			for _, u := range g.Pool.Upstreams() {
				if u.Name == "wrapped" && !u.Healthy() {
					t.Errorf("包装 404 为 500 不应降级 wrapped 源健康度")
				}
			}
		}
	}
}

// blob 路径同样识别被包装的 BLOB_UNKNOWN。
func TestProxyBlobWrappedNotFound(t *testing.T) {
	ok := newFakeRegistry(t)
	ok.blobs["sha256:"+strings.Repeat("aa", 32)] = []byte("data")
	wrapped := newFakeRegistry(t)
	wrapped.wrapNotFound = true

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "wrapped", URL: wrapped.URL, Priority: 20},
	}, c)

	missing := "sha256:" + strings.Repeat("34", 32)
	rec := httptest.NewRecorder()
	p.ServeBlob(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/blobs/"+missing, nil), "library/nginx", missing)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404, body=%s", rec.Code, rec.Body.String())
	}
	for _, g := range p.router.Groups() {
		if g.Name == "test" {
			for _, u := range g.Pool.Upstreams() {
				if u.Name == "wrapped" && !u.Healthy() {
					t.Errorf("包装 404 为 500 不应降级 wrapped 源健康度")
				}
			}
		}
	}
}

// ghcr.io 等源对不存在仓库的令牌请求返回 403 DENIED，sparkcr 等源对未同步
// 的仓库返回 401 且无认证挑战——两种都应视为仓库级策略：换源重试、不降级。
func TestProxyManifestTokenDeniedIsRepoPolicy(t *testing.T) {
	ok := newFakeRegistry(t)
	ok.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))
	strict := newFakeRegistry(t)
	strict.tokenDeniedScopes = map[string]bool{"repository:library/nginx:pull": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "ok", URL: ok.URL, Priority: 10},
		{Name: "strict", URL: strict.URL, Priority: 20},
	}, c)

	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil), "library/nginx", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200（从 ok 源拉取）, body=%s", rec.Code, rec.Body.String())
	}
	// strict 源的 token 被拒，不应降级其健康度
	for _, g := range p.router.Groups() {
		if g.Name == "test" {
			for _, u := range g.Pool.Upstreams() {
				if u.Name == "strict" && !u.Healthy() {
					t.Errorf("token 接口 403 不应降级 strict 源健康度")
				}
			}
		}
	}
}

// 所有源的令牌都被拒绝时，返回 404 而不是把 token 错误包成 502。
func TestProxyManifestAllTokenDenied(t *testing.T) {
	a := newFakeRegistry(t)
	a.tokenDeniedScopes = map[string]bool{"repository:library/private:pull": true}
	b := newFakeRegistry(t)
	b.tokenDeniedScopes = map[string]bool{"repository:library/private:pull": true}

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "a", URL: a.URL, Priority: 10},
		{Name: "b", URL: b.URL, Priority: 20},
	}, c)

	rec := httptest.NewRecorder()
	p.ServeManifest(rec, httptest.NewRequest(http.MethodGet, "/v2/library/private/manifests/latest", nil), "library/private", "latest")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "MANIFEST_UNKNOWN") {
		t.Errorf("错误码不符合 Registry 规范: %s", rec.Body.String())
	}
}

// docker login 到网关的客户端凭证（Authorization: Basic ...）绝不能被转发到
// 上游：上游认证只能来自配置文件里该 upstream 的 username/password。否则
// 登录 dockerhub 组的凭证会被带去访问 quay/ghcr 等组的上游，轻则认证失败，
// 重则把用户的真实账号密码泄漏给无关镜像源。
func TestProxyClientCredentialsNotForwarded(t *testing.T) {
	f := newFakeRegistry(t)
	f.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))

	c := newTestCache(t)
	// 上游未配置 username/password（最常见场景）
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "fake", URL: f.URL, Priority: 10},
	}, c)

	// 模拟 docker login 后的拉取请求：docker 会带上 Basic 凭证
	req := httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil)
	req.SetBasicAuth("docker-login-user", "docker-login-password")
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "library/nginx", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}

	// 上游数据接口收到的应是网关自己换的 Bearer token，绝不是客户端凭证
	got, _ := f.lastAuth.Load().(string)
	if got != "Bearer test-token" {
		t.Errorf("上游收到的 Authorization = %q, 期望 \"Bearer test-token\"（客户端凭证不得透传）", got)
	}
	// token 接口同样不得收到客户端凭证（该上游未配置账号，应完全没有 Authorization）
	if got := f.lastTokenAuth.Load(); got != nil && got != "" {
		t.Errorf("token 接口收到 Authorization = %v, 期望空", got)
	}
}

// 上游配置了自己的账号时，客户端凭证也不应覆盖它：上游收到的必须是基于
// 配置账号换取的 token。
func TestProxyClientCredentialsVsUpstreamConfig(t *testing.T) {
	f := newFakeRegistry(t)
	f.token = "upstream-config-token"
	f.tokenUser, f.tokenPass = "cfg-user", "cfg-pass"
	f.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))

	c := newTestCache(t)
	p := newTestProxy(t, []config.UpstreamConfig{
		{Name: "fake", URL: f.URL, Priority: 10, Username: "cfg-user", Password: "cfg-pass"},
	}, c)

	req := httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil)
	req.SetBasicAuth("docker-login-user", "docker-login-password")
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "library/nginx", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	got, _ := f.lastAuth.Load().(string)
	if got != "Bearer upstream-config-token" {
		t.Errorf("上游收到的 Authorization = %q, 期望配置账号换的 token", got)
	}
}

// alwaysChallenge 在数据接口忽略任何凭证恒返回 401 挑战，模拟
// 「令牌换到了但 scope 权限被拒」的私有仓库/配额场景。
func (f *fakeRegistry) handleAlwaysChallenge(w http.ResponseWriter, name string) bool {
	if !f.alwaysChallenge {
		return false
	}
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fake-registry",scope="repository:%s:pull"`, f.Server.URL, name))
	w.WriteHeader(http.StatusUnauthorized)
	return true
}

// newPassthroughProxy 构造开启认证透传、分组绑定域名的测试代理。
func newPassthroughProxy(t *testing.T, upstreams []config.UpstreamConfig, hosts []string, c *cache.Cache) (*Proxy, *Router) {
	t.Helper()
	log := testLogger()
	m := metrics.New()
	cfg := config.Default()
	cfg.Auth.Passthrough = true
	cfg.Groups = []config.GroupConfig{{
		Name:             "test",
		Priority:         10,
		Hosts:            hosts,
		Default:          true,
		LibraryNamespace: "library",
		Upstreams:        upstreams,
	}}
	cfg.Routing.AutoDetect = false
	cfg.Routing.DefaultGroup = "test"
	cfg.Health.Timeout = config.Duration(2 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	router, err := NewRouter(cfg, log, m)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return NewProxy(router, c, Options{MaxManifestBytes: 1 << 20}, log, m), router
}

// 认证透传：上游对网关自动换取的令牌仍拒绝（私有仓库/配额场景）时，
// 401 挑战应改写 realm 后透传给客户端，docker 据此到网关 /token 换令牌。
func TestProxyPassthroughChallengeRelayed(t *testing.T) {
	f := newFakeRegistry(t)
	f.alwaysChallenge = true

	c := newTestCache(t)
	p, _ := newPassthroughProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}},
		[]string{"docker.example.com"}, c)

	req := httptest.NewRequest(http.MethodGet, "/v2/library/private/manifests/latest", nil)
	req.Host = "docker.example.com"
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "library/private", "latest")

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("状态码 = %d, 期望 401, body=%s", rec.Code, rec.Body.String())
	}
	ch := rec.Header().Get("WWW-Authenticate")
	if ch == "" {
		t.Fatal("缺少 WWW-Authenticate 挑战头")
	}
	if !strings.Contains(ch, `realm="http://docker.example.com/token?upstream=fake"`) {
		t.Errorf("挑战 realm 未指向网关令牌端点: %s", ch)
	}
	if !strings.Contains(ch, `service="fake-registry"`) {
		t.Errorf("挑战 service 未保留上游原值: %s", ch)
	}
	if !strings.Contains(ch, `scope="repository:library/private:pull"`) {
		t.Errorf("挑战 scope 未保留上游原值: %s", ch)
	}
}

// 认证透传：客户端 docker login 后的 Basic 凭证原样发给上游，且该流量
// 不写缓存（第二次带凭证请求仍是 miss）。
func TestProxyPassthroughClientAuthForwarded(t *testing.T) {
	f := newFakeRegistry(t)
	f.token = "" // 免认证源（模拟 quay.io 的 Basic 直接访问）
	f.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))

	c := newTestCache(t)
	p, _ := newPassthroughProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}},
		[]string{"quay.example.com"}, c)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil)
		req.Host = "quay.example.com"
		req.SetBasicAuth("docker-login-user", "docker-login-password")
		rec := httptest.NewRecorder()
		p.ServeManifest(rec, req, "library/nginx", "latest")
		if rec.Code != http.StatusOK {
			t.Fatalf("第 %d 次状态码 = %d, body=%s", i+1, rec.Code, rec.Body.String())
		}
		if i == 0 && rec.Header().Get("X-Mirror-Gateway-Cache") != "miss" {
			t.Errorf("首次拉取应为 miss")
		}
		if i == 1 && rec.Header().Get("X-Mirror-Gateway-Cache") != "miss" {
			t.Errorf("带凭证的流量不应写缓存，第二次拉取不应命中缓存")
		}
	}
	got, _ := f.lastAuth.Load().(string)
	if got != "Basic "+base64.StdEncoding.EncodeToString([]byte("docker-login-user:docker-login-password")) {
		t.Errorf("上游收到的 Authorization = %q, 期望客户端 Basic 原文", got)
	}
}

// 令牌换取端点：凭证只发给目标上游自己的令牌服务；未开启透传的分组、
// 不存在的上游一律 404（防把端点当跳板）。
func TestProxyPassthroughTokenEndpoint(t *testing.T) {
	f := newFakeRegistry(t)
	f.token = "upstream-token-xyz"
	f.tokenUser, f.tokenPass = "alice", "s3cret"

	c := newTestCache(t)
	p, router := newPassthroughProxy(t, []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}},
		[]string{"docker.example.com"}, c)
	// 对照组：未开透传的分组（无 hosts）也挂了一个 fake 名字的上游
	noAuth := newFakeRegistry(t)
	cfg2 := config.Default()
	cfg2.Groups = []config.GroupConfig{{
		Name: "plain", Priority: 20, Default: false,
		Upstreams: []config.UpstreamConfig{{Name: "fake", URL: noAuth.URL, Priority: 10}},
	}}
	cfg2.Routing.AutoDetect = false
	cfg2.Routing.DefaultGroup = "plain"
	cfg2.Health.Timeout = config.Duration(2 * time.Second)
	if err := cfg2.Validate(); err != nil {
		t.Fatal(err)
	}
	r2, err := NewRouter(cfg2, testLogger(), metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	_ = r2

	tokenReq := func(upstream, user, pass string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/token?upstream="+upstream+"&service=fake-registry&scope=repository:library/private:pull", nil)
		req.Host = "docker.example.com"
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		rec := httptest.NewRecorder()
		p.ServeToken(rec, req)
		return rec
	}

	// 正确凭证：网关探测上游挑战获得 realm，带客户端 Basic 转发 → 令牌
	rec := tokenReq("fake", "alice", "s3cret")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "upstream-token-xyz") {
		t.Errorf("令牌响应不符: %s", rec.Body.String())
	}
	got, _ := f.lastTokenAuth.Load().(string)
	if got != "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:s3cret")) {
		t.Errorf("上游令牌接口收到的凭证 = %q, 期望客户端 Basic 原文", got)
	}

	// 错误凭证：上游令牌接口 401 → 端点返回 401
	rec = tokenReq("fake", "alice", "wrong")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("错误凭证状态码 = %d, 期望 401", rec.Code)
	}

	// 不存在的上游 → 404（防止端点被当作向任意目标转发凭证的跳板）
	rec = tokenReq("nonexistent", "alice", "s3cret")
	if rec.Code != http.StatusNotFound {
		t.Errorf("不存在上游状态码 = %d, 期望 404", rec.Code)
	}
	_ = router
}

// 全局开关开着但分组没配 hosts（单域名多分组）：凭证不得透传——
// 否则登录 Docker Hub 的密码会被带给 ghcr/quay 等其他组的上游。
func TestProxyPassthroughRequiresHosts(t *testing.T) {
	f := newFakeRegistry(t)
	f.addManifest("library/nginx", "latest", MediaTypeDockerManifest,
		[]byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`))

	c := newTestCache(t)
	log := testLogger()
	m := metrics.New()
	cfg := config.Default()
	cfg.Auth.Passthrough = true // 全局开启
	cfg.Groups = []config.GroupConfig{{
		Name: "test", Priority: 10, Default: true, // 无 hosts
		Upstreams: []config.UpstreamConfig{{Name: "fake", URL: f.URL, Priority: 10}},
	}}
	cfg.Routing.AutoDetect = false
	cfg.Routing.DefaultGroup = "test"
	cfg.Health.Timeout = config.Duration(2 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(cfg, log, m)
	if err != nil {
		t.Fatal(err)
	}
	p := NewProxy(router, c, Options{MaxManifestBytes: 1 << 20}, log, m)

	req := httptest.NewRequest(http.MethodGet, "/v2/library/nginx/manifests/latest", nil)
	req.SetBasicAuth("docker-login-user", "docker-login-password")
	rec := httptest.NewRecorder()
	p.ServeManifest(rec, req, "library/nginx", "latest")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	got, _ := f.lastAuth.Load().(string)
	if got != "Bearer "+f.token {
		t.Errorf("未配 hosts 的分组不应透传凭证, 上游收到 %q", got)
	}
}
