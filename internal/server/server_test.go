package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/cache"
	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
	"github.com/midaug/docker-mirror-gateway/internal/registry"
	"github.com/midaug/docker-mirror-gateway/internal/search"
)

func newTestServer(t *testing.T, adminToken string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Server.AdminToken = adminToken
	cfg.Groups = []config.GroupConfig{{
		Name:             "dockerhub",
		Priority:         10,
		Default:          true,
		LibraryNamespace: "library",
		Aliases:          []string{"docker.io"},
		Upstreams:        []config.UpstreamConfig{{Name: "fake", URL: "http://127.0.0.1:1", Priority: 10}},
	}}
	cfg.Group = config.GroupConfig{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	m := metrics.New()
	c, err := cache.New(cache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20, TagTTL: 60e9})
	if err != nil {
		t.Fatal(err)
	}
	router, err := registry.NewRouter(cfg, log, m)
	if err != nil {
		t.Fatal(err)
	}
	proxy := registry.NewProxy(router, c, registry.Options{}, log, m)
	s, err := New(Options{Config: cfg, Router: router, Proxy: proxy, Cache: c, Logger: log, Metrics: m, Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestV2Root(t *testing.T) {
	s := newTestServer(t, "")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if got := rec.Header().Get("Docker-Distribution-Api-Version"); got != "registry/2.0" {
		t.Errorf("API 版本头 = %q", got)
	}
	if strings.TrimSpace(rec.Body.String()) != "{}" {
		t.Errorf("响应体 = %q", rec.Body.String())
	}
}

func TestUnknownRegistryRoute(t *testing.T) {
	s := newTestServer(t, "")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/unknown/path", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "UNSUPPORTED") {
		t.Errorf("响应体 = %s", rec.Body.String())
	}
}

func TestPushIsRejected(t *testing.T) {
	s := newTestServer(t, "")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/library/nginx/blobs/uploads/", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("状态码 = %d, 期望 405", rec.Code)
	}
}

func TestStatusAndDashboard(t *testing.T) {
	s := newTestServer(t, "")
	handler := s.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status 状态码 = %d", rec.Code)
	}
	var body struct {
		Version   string                 `json:"version"`
		Upstreams []registry.Status      `json:"upstreams"`
		Groups    []registry.GroupStatus `json:"groups"`
		Cache     struct {
			Dir string `json:"dir"`
		} `json:"cache"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析 status 失败: %v", err)
	}
	if body.Version != "test" || len(body.Upstreams) != 1 || body.Cache.Dir == "" {
		t.Errorf("status 内容异常: %+v", body)
	}
	if len(body.Groups) != 1 || body.Groups[0].Name != "dockerhub" || body.Groups[0].Upstreams[0].Group != "dockerhub" {
		t.Errorf("分组信息异常: %+v", body.Groups)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Docker Mirror Gateway") {
		t.Errorf("控制台页面异常: code=%d", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "mirror_gateway_cache_bytes") {
		t.Errorf("指标输出异常: code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdminToken(t *testing.T) {
	s := newTestServer(t, "s3cret")
	handler := s.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("未带令牌状态码 = %d, 期望 401", rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.Header.Set("X-Admin-Token", "s3cret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("带令牌状态码 = %d, 期望 200", rec.Code)
	}

	// 注册表接口不应受管理令牌影响
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/v2/ 状态码 = %d", rec.Code)
	}
}

func TestHealthEndpoints(t *testing.T) {
	s := newTestServer(t, "")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz 状态码 = %d", rec.Code)
	}
}

func TestSplitAdminPort(t *testing.T) {
	s := newTestServer(t, "s3cret")
	s.cfg.Server.AdminAddr = ":18081"
	s.cfg.Server.PublicURL = "https://mirror.example.com"

	pull, admin := s.Handler(), s.AdminHandler()

	cases := []struct {
		name    string
		path    string
		handler http.Handler
		token   string
		want    int
	}{
		{"拉取端口提供 /v2/", "/v2/", pull, "", http.StatusOK},
		{"拉取端口提供健康检查", "/healthz", pull, "", http.StatusOK},
		{"拉取端口不暴露控制台", "/", pull, "", http.StatusNotFound},
		{"拉取端口不暴露管理接口", "/api/v1/status", pull, "", http.StatusNotFound},
		{"拉取端口不暴露指标", "/metrics", pull, "", http.StatusNotFound},
		{"管理端口不提供镜像接口", "/v2/", admin, "", http.StatusNotFound},
		{"管理端口提供控制台", "/", admin, "", http.StatusOK},
		{"管理端口校验令牌", "/api/v1/status", admin, "", http.StatusUnauthorized},
		{"管理端口带令牌可访问", "/api/v1/status", admin, "s3cret", http.StatusOK},
		{"管理端口提供指标", "/metrics", admin, "s3cret", http.StatusOK},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.token != "" {
			req.Header.Set("X-Admin-Token", tc.token)
		}
		rec := httptest.NewRecorder()
		tc.handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s: 状态码 = %d, 期望 %d", tc.name, rec.Code, tc.want)
		}
	}

	// status 应告诉控制台端口分离情况，便于生成正确的接入示例。
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	req.Header.Set("X-Admin-Token", "s3cret")
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	var body struct {
		Listen struct {
			PullAddr   string `json:"pullAddr"`
			AdminAddr  string `json:"adminAddr"`
			AdminSplit bool   `json:"adminSplit"`
			AdminAuth  bool   `json:"adminAuth"`
			PullURL    string `json:"pullUrl"`
		} `json:"listen"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析 status 失败: %v", err)
	}
	if !body.Listen.AdminSplit || body.Listen.AdminAddr != ":18081" || body.Listen.PullAddr == "" {
		t.Errorf("listen 信息异常: %+v", body.Listen)
	}
	if !body.Listen.AdminAuth || body.Listen.PullURL != "https://mirror.example.com" {
		t.Errorf("listen 鉴权/公网地址异常: %+v", body.Listen)
	}
}

func TestActivityEndpoint(t *testing.T) {
	s := newTestServer(t, "")
	rec := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Items []struct {
			Kind  string `json:"kind"`
			State string `json:"state"`
		} `json:"items"`
		Summary struct {
			Total int `json:"total"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	// 没有真实拉取时应返回空数组而不是 null，前端才能直接遍历。
	if payload.Items == nil {
		t.Error("items 应为空数组而非 null")
	}

	// 管理令牌开启后必须鉴权。
	secured := newTestServer(t, "secret")
	rec = httptest.NewRecorder()
	secured.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("无令牌状态码 = %d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil)
	req.Header.Set("X-Admin-Token", "secret")
	rec = httptest.NewRecorder()
	secured.AdminHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("带令牌状态码 = %d", rec.Code)
	}
}

// rewriteTransport 把搜索请求统一改写到本地假服务，避免测试依赖外网。
type rewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	return t.base.RoundTrip(clone)
}

func newV1SearchServer(t *testing.T, hub http.Handler) *Server {
	t.Helper()
	fake := httptest.NewServer(hub)
	t.Cleanup(fake.Close)
	target, err := url.Parse(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &rewriteTransport{target: target, base: http.DefaultTransport}}
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := search.New(client, 2*time.Second, []string{"hub"}, false, log, search.WithHubClient(client))
	s := newTestServer(t, "")
	s.search = svc
	return s
}

func TestV1SearchCompat(t *testing.T) {
	hub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/search/repositories/" {
			t.Errorf("请求路径 = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("query"); got != "nginx" {
			t.Errorf("query 参数 = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"count":3,"results":[
			{"repo_name":"library/nginx","short_description":"Official build of Nginx.","star_count":20000,"pull_count":1000000,"is_official":true},
			{"repo_name":"nginxinc/nginx-unprivileged","short_description":"unprivileged","star_count":120,"pull_count":1000},
			{"repo_name":"bitnami/nginx","short_description":"Bitnami","star_count":50,"pull_count":500}
		]}`)
	})
	s := newV1SearchServer(t, hub)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/search?q=nginx&n=2&page=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 响应体 = %s", rec.Code, rec.Body.String())
	}
	var body v1SearchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if body.Query != "nginx" || body.NumResults != 2 || len(body.Results) != 2 {
		t.Fatalf("响应内容异常: %+v", body)
	}
	first := body.Results[0]
	if first.Name != "library/nginx" || !first.IsOfficial || first.StarCount != 20000 {
		t.Errorf("首条结果 = %+v", first)
	}
	if first.Description != "Official build of Nginx." || first.IsAutomated {
		t.Errorf("首条结果其余字段 = %+v", first)
	}

	// 第二页取剩余结果。
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/search?q=nginx&n=2&page=2", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析第二页失败: %v", err)
	}
	if len(body.Results) != 1 || body.Results[0].Name != "bitnami/nginx" {
		t.Errorf("第二页结果 = %+v", body.Results)
	}

	// 缺少 q、方法不对、搜索关闭时的错误码。
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/search", nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("缺少 q 状态码 = %d, 期望 400", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/search?q=nginx", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST 状态码 = %d, 期望 405", rec.Code)
	}
	s.cfg.Search.Enabled = false
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/search?q=nginx", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("搜索关闭状态码 = %d, 期望 404", rec.Code)
	}
}

func TestCatalogListsKnownRepositories(t *testing.T) {
	s := newTestServer(t, "")
	tracker := s.proxy.Activity()
	for _, repo := range []string{"library/nginx", "library/alpine", "library/nginx"} {
		e := tracker.Begin("manifest", repo, "latest", "dockerhub")
		e.Finish()
	}

	var body struct {
		Repositories []string `json:"repositories"`
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/_catalog", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 响应体 = %s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	want := []string{"library/alpine", "library/nginx"}
	if len(body.Repositories) != 2 || body.Repositories[0] != want[0] || body.Repositories[1] != want[1] {
		t.Fatalf("目录 = %v, 期望 %v", body.Repositories, want)
	}

	// n / last 分页与 Link 头。
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/_catalog?n=1", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析分页响应失败: %v", err)
	}
	if len(body.Repositories) != 1 || body.Repositories[0] != "library/alpine" {
		t.Fatalf("第一页 = %v", body.Repositories)
	}
	if link := rec.Header().Get("Link"); !strings.Contains(link, "last=library%2Falpine") {
		t.Errorf("Link 头 = %q", link)
	}
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/_catalog?n=1&last=library%2Falpine", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析第二页失败: %v", err)
	}
	if len(body.Repositories) != 1 || body.Repositories[0] != "library/nginx" {
		t.Errorf("第二页 = %v", body.Repositories)
	}
}
