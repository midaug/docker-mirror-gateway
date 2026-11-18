package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

func newRouterForTest(t *testing.T, groups []config.GroupConfig, tune func(*config.Config)) *Router {
	t.Helper()
	cfg := config.Default()
	cfg.Groups = groups
	cfg.Group = config.GroupConfig{}
	cfg.Health.Timeout = config.Duration(2 * time.Second)
	if tune != nil {
		tune(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	r, err := NewRouter(cfg, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

func deadUpstream(name string) config.UpstreamConfig {
	return config.UpstreamConfig{Name: name, URL: "http://127.0.0.1:1", Priority: 10}
}

func TestResolveByAliasAndHost(t *testing.T) {
	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true,
			LibraryNamespace: "library",
			Aliases:          []string{"dockerhub"},
			Upstreams:        []config.UpstreamConfig{deadUpstream("hub")},
		},
		{
			Name: "ghcr", Priority: 20,
			Aliases:   []string{"ghcr", "ghcr.io"},
			Hosts:     []string{"ghcr.mirror.local"},
			Upstreams: []config.UpstreamConfig{deadUpstream("ghcr")},
		},
	}, func(c *config.Config) { c.Routing.AutoDetect = false })

	ctx := context.Background()
	cases := []struct {
		host, repo string
		group      string
		wantRepo   string
		via        string
	}{
		{"", "ghcr.io/stefanprodan/podinfo", "ghcr", "stefanprodan/podinfo", "alias"},
		{"", "ghcr/stefanprodan/podinfo", "ghcr", "stefanprodan/podinfo", "alias"},
		{"mirror.local:8080", "nginx", "dockerhub", "library/nginx", "default"},
		{"ghcr.mirror.local:5000", "stefanprodan/podinfo", "ghcr", "stefanprodan/podinfo", "host"},
		{"ghcr.mirror.local", "stefanprodan/podinfo", "ghcr", "stefanprodan/podinfo", "host"},
	}
	for _, tc := range cases {
		route, err := r.Resolve(ctx, tc.host, tc.repo)
		if err != nil {
			t.Fatalf("Resolve(%q, %q): %v", tc.host, tc.repo, err)
		}
		if route.Group.Name != tc.group || route.Repo != tc.wantRepo || route.Via != tc.via {
			t.Errorf("Resolve(%q, %q) = {%s, %s, %s}, 期望 {%s, %s, %s}",
				tc.host, tc.repo, route.Group.Name, route.Repo, route.Via, tc.group, tc.wantRepo, tc.via)
		}
	}

	if _, err := r.Resolve(ctx, "", "Library/nginx"); !errors.Is(err, ErrInvalidRepository) {
		t.Errorf("大写仓库名应被拒绝, err=%v", err)
	}
}

func TestResolveAutoDetectAcrossGroups(t *testing.T) {
	hub := newFakeRegistry(t)
	ghcr := newFakeRegistry(t)
	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`)
	hub.addManifest("library/nginx", "latest", MediaTypeDockerManifest, body)
	ghcr.addManifest("stefanprodan/podinfo", "latest", MediaTypeDockerManifest, body)

	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true, LibraryNamespace: "library",
			Upstreams: []config.UpstreamConfig{{Name: "hub", URL: hub.URL, Priority: 10}},
		},
		{
			Name: "ghcr", Priority: 20,
			Upstreams: []config.UpstreamConfig{{Name: "ghcr", URL: ghcr.URL, Priority: 10}},
		},
	}, nil)

	ctx := context.Background()
	route, err := r.Resolve(ctx, "mirror.local", "stefanprodan/podinfo")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if route.Group.Name != "ghcr" || route.Repo != "stefanprodan/podinfo" || route.Via != "detect" {
		t.Errorf("自动探测结果 = {%s, %s, %s}", route.Group.Name, route.Repo, route.Via)
	}

	hubTags, ghcrTags := hub.tagsReqs.Load(), ghcr.tagsReqs.Load()
	if hubTags == 0 || ghcrTags == 0 {
		t.Errorf("应并发探测两个分组, hub=%d ghcr=%d", hubTags, ghcrTags)
	}

	// 第二次应直接命中路由缓存，不再探测上游。
	route, err = r.Resolve(ctx, "mirror.local", "stefanprodan/podinfo")
	if err != nil {
		t.Fatalf("第二次 Resolve: %v", err)
	}
	if route.Via != "cache" {
		t.Errorf("第二次路由来源 = %q, 期望 cache", route.Via)
	}
	if hub.tagsReqs.Load() != hubTags || ghcr.tagsReqs.Load() != ghcrTags {
		t.Error("命中路由缓存后仍探测了上游")
	}

	// 单段镜像名应由 dockerhub 补全 library 前缀后命中。
	route, err = r.Resolve(ctx, "mirror.local", "nginx")
	if err != nil {
		t.Fatalf("Resolve(nginx): %v", err)
	}
	if route.Group.Name != "dockerhub" || route.Repo != "library/nginx" {
		t.Errorf("nginx 路由结果 = {%s, %s}", route.Group.Name, route.Repo)
	}
}

func TestResolveAutoDetectRespectsGroupPriority(t *testing.T) {
	hub := newFakeRegistry(t)
	ghcr := newFakeRegistry(t)
	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`)
	// 两个分组都有同名仓库，应优先选择 priority 更小的分组。
	hub.addManifest("acme/app", "latest", MediaTypeDockerManifest, body)
	ghcr.addManifest("acme/app", "latest", MediaTypeDockerManifest, body)

	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "ghcr", Priority: 20,
			Upstreams: []config.UpstreamConfig{{Name: "ghcr", URL: ghcr.URL, Priority: 10}},
		},
		{
			Name: "dockerhub", Priority: 10, Default: true, LibraryNamespace: "library",
			Upstreams: []config.UpstreamConfig{{Name: "hub", URL: hub.URL, Priority: 10}},
		},
	}, nil)

	route, err := r.Resolve(context.Background(), "", "acme/app")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if route.Group.Name != "dockerhub" {
		t.Errorf("优先级更高的分组应胜出, 实际 %s", route.Group.Name)
	}
}

func TestResolveNegativeCache(t *testing.T) {
	hub := newFakeRegistry(t)
	ghcr := newFakeRegistry(t)

	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true, LibraryNamespace: "library",
			Upstreams: []config.UpstreamConfig{{Name: "hub", URL: hub.URL, Priority: 10}},
		},
		{
			Name: "ghcr", Priority: 20,
			Upstreams: []config.UpstreamConfig{{Name: "ghcr", URL: ghcr.URL, Priority: 10}},
		},
	}, nil)

	ctx := context.Background()
	if _, err := r.Resolve(ctx, "", "nobody/missing"); !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("err = %v, 期望 ErrRepoNotFound", err)
	}
	before := hub.tagsReqs.Load() + ghcr.tagsReqs.Load()
	if _, err := r.Resolve(ctx, "", "nobody/missing"); !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("第二次 err = %v", err)
	}
	if after := hub.tagsReqs.Load() + ghcr.tagsReqs.Load(); after != before {
		t.Errorf("负缓存未生效: 探测次数 %d -> %d", before, after)
	}
	if len(r.RouteInfos()) != 1 {
		t.Errorf("路由缓存条目数 = %d", len(r.RouteInfos()))
	}
}

func TestResolveWithoutAutoDetectUsesDefault(t *testing.T) {
	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true, LibraryNamespace: "library",
			Upstreams: []config.UpstreamConfig{deadUpstream("hub")},
		},
		{
			Name: "ghcr", Priority: 20,
			Upstreams: []config.UpstreamConfig{deadUpstream("ghcr")},
		},
	}, func(c *config.Config) { c.Routing.AutoDetect = false })

	route, err := r.Resolve(context.Background(), "", "some/unknown")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if route.Via != "default" || route.Group.Name != "dockerhub" {
		t.Errorf("兜底路由 = {%s, %s}", route.Group.Name, route.Via)
	}
}

func TestResolveManifestUsesExactReference(t *testing.T) {
	hub := newFakeRegistry(t)
	ghcr := newFakeRegistry(t)
	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{}}`)
	// 仓库存在，但只有 v1.8.0 这个 tag。
	ghcr.addManifest("prometheus/node-exporter", "v1.8.0", MediaTypeDockerManifest, body)

	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true, LibraryNamespace: "library",
			Upstreams: []config.UpstreamConfig{{Name: "hub", URL: hub.URL, Priority: 10}},
		},
		{
			Name: "ghcr", Priority: 20,
			Upstreams: []config.UpstreamConfig{{Name: "ghcr", URL: ghcr.URL, Priority: 10}},
		},
	}, nil)

	ctx := context.Background()
	// 请求不存在的 latest，应判定为不存在，而不是因为仓库有别的 tag 就误判归属。
	if _, err := r.ResolveManifest(ctx, "", "prometheus/node-exporter", "latest"); !errors.Is(err, ErrRepoNotFound) {
		t.Fatalf("err = %v, 期望 ErrRepoNotFound", err)
	}
	// 请求真实存在的 tag，应命中 ghcr。
	route, err := r.ResolveManifest(ctx, "", "prometheus/node-exporter", "v1.8.0")
	if err != nil {
		t.Fatalf("ResolveManifest: %v", err)
	}
	if route.Group.Name != "ghcr" || route.Repo != "prometheus/node-exporter" {
		t.Errorf("路由结果 = {%s, %s}", route.Group.Name, route.Repo)
	}
	// 精确判定不应依赖 tags/list。
	if hub.tagsReqs.Load() != 0 || ghcr.tagsReqs.Load() != 0 {
		t.Errorf("manifest 判定不应调用 tags/list: hub=%d ghcr=%d", hub.tagsReqs.Load(), ghcr.tagsReqs.Load())
	}
}

// ProxiedSearchClient 应挑出显式配了代理的上游，而不是最快的镜像源。
func TestProxiedSearchClientPrefersExplicitProxy(t *testing.T) {
	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true,
			Upstreams: []config.UpstreamConfig{
				{Name: "fast-mirror", URL: "https://mirror.example.com", Priority: 10},
				{Name: "direct", URL: "https://registry-1.docker.io", Priority: 100, Proxy: "socks5://127.0.0.1:1080"},
			},
		},
	}, nil)

	client := r.ProxiedSearchClient()
	if client == nil {
		t.Fatal("存在显式代理的上游时不该返回 nil")
	}
	plain := r.SearchClient()
	if plain == nil {
		t.Fatal("SearchClient 不该返回 nil")
	}
	if client == plain {
		t.Fatal("ProxiedSearchClient 应返回代理上游的客户端，而不是最快的镜像源")
	}
}

// 没有显式代理时返回 nil，调用方退回默认客户端。
func TestProxiedSearchClientWithoutProxy(t *testing.T) {
	r := newRouterForTest(t, []config.GroupConfig{
		{
			Name: "dockerhub", Priority: 10, Default: true,
			Upstreams: []config.UpstreamConfig{
				{Name: "mirror", URL: "https://mirror.example.com", Priority: 10},
			},
		},
	}, nil)

	if client := r.ProxiedSearchClient(); client != nil {
		t.Fatal("没有显式代理时应返回 nil")
	}
}
