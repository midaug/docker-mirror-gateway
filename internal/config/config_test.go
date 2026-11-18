package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValidates(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("默认配置应合法: %v", err)
	}
	if cfg.Server.Addr != ":8080" {
		t.Errorf("默认监听地址 = %q", cfg.Server.Addr)
	}
	if cfg.Selection.Strategy != "speed" {
		t.Errorf("默认策略 = %q，期望按下载吞吐排序", cfg.Selection.Strategy)
	}
	if cfg.Selection.SpeedProbeBytes != 256<<10 {
		t.Errorf("默认吞吐探测字节 = %d", cfg.Selection.SpeedProbeBytes)
	}
	if len(cfg.Group.Upstreams) != 1 {
		t.Errorf("默认上游数量 = %d", len(cfg.Group.Upstreams))
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
server:
  addr: ":9090"
  adminToken: "s3cret"
cache:
  dir: "` + dir + `/cache"
  maxSizeGB: 12
  tagTTL: 90s
selection:
  strategy: latency
health:
  interval: 15s
  failThreshold: 5
group:
  name: dockerhub
  libraryNamespace: library
  upstreams:
    - name: first
      url: https://first.example.com/
      priority: 5
    - name: second
      url: ${TEST_MIRROR_URL}
      priority: 20
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_MIRROR_URL", "https://second.example.com")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Path != path {
		t.Errorf("Path = %q", cfg.Path)
	}
	if cfg.Server.Addr != ":9090" || cfg.Server.AdminToken != "s3cret" {
		t.Errorf("server 配置错误: %+v", cfg.Server)
	}
	if cfg.Cache.MaxSizeGB != 12 || cfg.Cache.TagTTL.D() != 90*time.Second {
		t.Errorf("cache 配置错误: %+v", cfg.Cache)
	}
	if cfg.Selection.Strategy != "latency" {
		t.Errorf("策略 = %q", cfg.Selection.Strategy)
	}
	if cfg.Health.Interval.D() != 15*time.Second || cfg.Health.FailThreshold != 5 {
		t.Errorf("health 配置错误: %+v", cfg.Health)
	}
	if len(cfg.Group.Upstreams) != 2 {
		t.Fatalf("上游数量 = %d", len(cfg.Group.Upstreams))
	}
	if cfg.Group.Upstreams[0].URL != "https://first.example.com" {
		t.Errorf("URL 未去掉结尾斜杠: %q", cfg.Group.Upstreams[0].URL)
	}
	if cfg.Group.Upstreams[1].URL != "https://second.example.com" {
		t.Errorf("环境变量未展开: %q", cfg.Group.Upstreams[1].URL)
	}
	// 未显式配置的字段应保留默认值
	if cfg.Server.IdleTimeout.D() != 120*time.Second {
		t.Errorf("默认值被意外覆盖: %v", cfg.Server.IdleTimeout)
	}
}

func TestLoadMissingExplicitFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "not-exists.yaml")); err == nil {
		t.Fatal("显式指定不存在的配置文件应返回错误")
	}
}

func TestValidateRejectsInvalidConfig(t *testing.T) {
	cfg := Default()
	cfg.Group.Upstreams[0].URL = "ftp://mirror.example.com"
	if err := cfg.Validate(); err == nil {
		t.Error("非 http/https 的 url 应被拒绝")
	}

	cfg = Default()
	cfg.Selection.Strategy = "fastest"
	if err := cfg.Validate(); err == nil {
		t.Error("未知策略应被拒绝")
	}

	cfg = Default()
	cfg.Group.Upstreams = append(cfg.Group.Upstreams, cfg.Group.Upstreams[0])
	if err := cfg.Validate(); err == nil {
		t.Error("重名的上游应被拒绝")
	}

	cfg = Default()
	cfg.Cache.Enabled = true
	cfg.Cache.MaxSizeGB = 0
	if err := cfg.Validate(); err == nil {
		t.Error("启用缓存时容量上限为 0 应被拒绝")
	}

	// 缓存默认关闭，此时容量字段不参与校验。
	cfg = Default()
	cfg.Cache.MaxSizeGB = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("缓存关闭时不应校验容量: %v", err)
	}
}

func TestDefaultPriorityAndTimeout(t *testing.T) {
	cfg := Default()
	cfg.Group.Upstreams[0].Priority = 0
	cfg.Group.Upstreams[0].Timeout = 0
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Group.Upstreams[0].Priority != 100 {
		t.Errorf("priority 应补为 100, 实际 %d", cfg.Group.Upstreams[0].Priority)
	}
	if cfg.Group.Upstreams[0].Timeout.D() != 30*time.Second {
		t.Errorf("timeout 应补为 30s, 实际 %v", cfg.Group.Upstreams[0].Timeout)
	}
}

func TestCacheDirExpansion(t *testing.T) {
	cfg := Default()
	dir, err := cfg.CacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(dir) {
		t.Errorf("缓存目录应为绝对路径: %s", dir)
	}
}

func TestLoadMultiGroups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
cache:
  dir: "` + dir + `/cache"
  maxSizeGB: 5
routing:
  autoDetect: true
  strategy: fastest
  defaultGroup: dockerhub
groups:
  - name: dockerhub
    priority: 10
    libraryNamespace: library
    aliases: ["docker.io", "https://dockerhub/"]
    probeRepo: library/alpine
    upstreams:
      - name: hub
        url: https://registry-1.docker.io
  - name: ghcr
    priority: 20
    hosts: ["ghcr.mirror.example.com:8443"]
    probeRepo: stefanprodan/podinfo
    upstreams:
      - name: ghcr
        url: https://ghcr.io
  - name: quay
    priority: 30
    upstreams:
      - name: quay
        url: https://quay.io
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Groups) != 3 {
		t.Fatalf("分组数量 = %d", len(cfg.Groups))
	}
	if cfg.Routing.Strategy != "fastest" || !cfg.Routing.AutoDetect {
		t.Errorf("routing 配置错误: %+v", cfg.Routing)
	}
	if cfg.Routing.DefaultGroup != "dockerhub" {
		t.Errorf("默认分组 = %q", cfg.Routing.DefaultGroup)
	}
	ghcr := cfg.GroupByName("ghcr")
	if ghcr == nil {
		t.Fatal("未找到 ghcr 分组")
	}
	// host 应自动成为路径别名，且去掉端口。
	found := false
	for _, a := range ghcr.Aliases {
		if a == "ghcr.mirror.example.com" {
			found = true
		}
	}
	if !found {
		t.Errorf("host 未加入路径别名: %v", ghcr.Aliases)
	}
	if ghcr.ProbeTag != "latest" {
		t.Errorf("probeTag 默认值 = %q", ghcr.ProbeTag)
	}
}

func TestLoadRejectsGroupAndGroups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
group:
  name: dockerhub
  upstreams:
    - name: hub
      url: https://registry-1.docker.io
groups:
  - name: ghcr
    upstreams:
      - name: ghcr
        url: https://ghcr.io
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("同时配置 group 与 groups 应返回错误")
	}
}

func TestValidateRejectsDuplicateUpstreamAcrossGroups(t *testing.T) {
	cfg := Default()
	cfg.Group = GroupConfig{}
	cfg.Groups = []GroupConfig{
		{
			Name:      "a",
			Upstreams: []UpstreamConfig{{Name: "same", URL: "https://a.example.com"}},
		},
		{
			Name:      "b",
			Upstreams: []UpstreamConfig{{Name: "same", URL: "https://b.example.com"}},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("跨分组重名的上游应被拒绝")
	}
}

func TestProxyNormalization(t *testing.T) {
	cases := map[string]string{
		"":                                  "",
		"env":                               "",
		"none":                              ProxyDirect,
		"OFF":                               ProxyDirect,
		"http://127.0.0.1:7890":             "http://127.0.0.1:7890",
		"HTTPS://proxy.example.com:8443":    "https://proxy.example.com:8443",
		"socks5://user:pass@10.0.0.2:1080":  "socks5://user:pass@10.0.0.2:1080",
		"socks5h://user:pass@10.0.0.2:1080": "socks5h://user:pass@10.0.0.2:1080",
	}
	for in, want := range cases {
		got, err := NormalizeProxy(in)
		if err != nil {
			t.Errorf("NormalizeProxy(%q) 返回错误: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("NormalizeProxy(%q) = %q, 期望 %q", in, got, want)
		}
	}

	for _, bad := range []string{"ftp://127.0.0.1:21", "http://", "127.0.0.1:8080", "socket://1.2.3.4:5"} {
		if _, err := NormalizeProxy(bad); err == nil {
			t.Errorf("NormalizeProxy(%q) 应返回错误", bad)
		}
	}
}

func TestEffectiveProxyCredentials(t *testing.T) {
	// socks5 + 独立账号密码：密码里的特殊字符要正确转义，解码后必须还原。
	got, err := EffectiveProxy("socks5://127.0.0.1:1080", "user", "p@ss:word#1")
	if err != nil {
		t.Fatalf("EffectiveProxy: %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("解析结果失败: %v", err)
	}
	if u.User.Username() != "user" {
		t.Errorf("用户名 = %q", u.User.Username())
	}
	if pw, _ := u.User.Password(); pw != "p@ss:word#1" {
		t.Errorf("密码 = %q", pw)
	}
	if strings.Contains(got, "p@ss") {
		t.Errorf("密码没有被转义: %q", got)
	}
	// 状态里展示的必须是脱敏后的地址。
	if masked := MaskProxy(got); strings.Contains(masked, "user") || strings.Contains(masked, "word") {
		t.Errorf("代理凭据未脱敏: %q", masked)
	}

	// http 代理同样适用。
	if got, err := EffectiveProxy("http://proxy.example.com:3128", "u", "p"); err != nil || !strings.Contains(got, "u:p@") {
		t.Errorf("http 代理带凭据 = %q, err=%v", got, err)
	}

	// 地址里已经有凭据时不再接受独立字段。
	if _, err := EffectiveProxy("socks5://u:p@127.0.0.1:1080", "u", "p"); err == nil {
		t.Error("两种凭据写法同时出现应报错")
	}
	// 没有代理地址时不能单独配账号。
	if _, err := EffectiveProxy("", "u", "p"); err == nil {
		t.Error("缺少代理地址时应报错")
	}
	if _, err := EffectiveProxy(ProxyDirect, "", "p"); err == nil {
		t.Error("direct 模式下不应接受代理账号")
	}
	// 只有密码没有用户名。
	if _, err := EffectiveProxy("socks5://127.0.0.1:1080", "", "p"); err == nil {
		t.Error("缺少用户名时应报错")
	}
}

func TestMaskProxy(t *testing.T) {
	if got := MaskProxy(""); got != "env" {
		t.Errorf("空代理 = %q", got)
	}
	if got := MaskProxy(ProxyDirect); got != ProxyDirect {
		t.Errorf("direct = %q", got)
	}
	got := MaskProxy("socks5://user:secret@127.0.0.1:1080")
	if strings.Contains(got, "user") || strings.Contains(got, "secret") {
		t.Errorf("凭据未脱敏: %q", got)
	}
	if !strings.Contains(got, "127.0.0.1:1080") {
		t.Errorf("丢失代理地址: %q", got)
	}
	if got := MaskProxy("http://127.0.0.1:7890"); got != "http://127.0.0.1:7890" {
		t.Errorf("无凭据代理被改写: %q", got)
	}
}

func TestValidateProxyAndAdminAddr(t *testing.T) {
	cfg := Default()
	cfg.Group.Upstreams[0].Proxy = "socks5://user:secret@127.0.0.1:1080"
	cfg.Server.AdminAddr = ":8081"
	cfg.Server.PublicURL = "https://mirror.example.com/"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := cfg.Groups[0].Upstreams[0].Proxy; got != "socks5://user:secret@127.0.0.1:1080" {
		t.Errorf("proxy = %q", got)
	}
	if cfg.Server.AdminAddr != ":8081" {
		t.Errorf("adminAddr = %q", cfg.Server.AdminAddr)
	}
	if cfg.Server.PublicURL != "https://mirror.example.com" {
		t.Errorf("publicURL = %q", cfg.Server.PublicURL)
	}

	// 与管理端口写成同一个地址等价于不分离。
	same := Default()
	same.Server.AdminAddr = same.Server.Addr
	if err := same.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if same.Server.AdminAddr != "" {
		t.Errorf("同端口时应合并，adminAddr = %q", same.Server.AdminAddr)
	}

	// publicURL 必须是带 host 的 http(s) 地址。
	bad := Default()
	bad.Server.PublicURL = "mirror.example.com"
	if err := bad.Validate(); err == nil {
		t.Error("publicURL 缺少 scheme 时应报错")
	}

	// 非法代理在加载阶段就被拦下。
	for _, spec := range []string{"ftp://127.0.0.1:21", "http://"} {
		c := Default()
		c.Group.Upstreams[0].Proxy = spec
		if err := c.Validate(); err == nil {
			t.Errorf("proxy %q 应被拒绝", spec)
		}
	}
}

func TestCacheDisabledSkipsValidation(t *testing.T) {
	cfg := Default()
	cfg.Cache.Enabled = false
	cfg.Cache.Dir = ""
	cfg.Cache.MaxSizeGB = 0
	cfg.Cache.Backend = "s3" // 关闭时不该要求 endpoint / bucket
	if err := cfg.Validate(); err != nil {
		t.Fatalf("缓存关闭时不该校验后端参数: %v", err)
	}
}

func TestCacheS3RequiresEndpointAndBucket(t *testing.T) {
	cfg := Default()
	cfg.Cache.Enabled = true
	cfg.Cache.Backend = "s3"
	if err := cfg.Validate(); err == nil {
		t.Error("缺 endpoint 应被拒绝")
	}

	cfg.Cache.S3.Endpoint = "minio.example.com:9000"
	if err := cfg.Validate(); err == nil {
		t.Error("缺 bucket 应被拒绝")
	}

	cfg.Cache.S3.Bucket = "docker-mirror-gateway"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("参数齐全时应通过: %v", err)
	}
	if cfg.Cache.S3.UseSSL == nil || !*cfg.Cache.S3.UseSSL {
		t.Error("useSSL 默认应为 true")
	}
	if cfg.Cache.S3.Prefix != "docker-mirror-gateway" {
		t.Errorf("prefix 默认值 = %q", cfg.Cache.S3.Prefix)
	}
}

func TestCacheBackendValidation(t *testing.T) {
	cfg := Default()
	cfg.Cache.Backend = "cephfs"
	if err := cfg.Validate(); err == nil {
		t.Error("未知后端应被拒绝")
	}
}

func TestSearchSourcesValidation(t *testing.T) {
	cfg := Default()
	cfg.Search.Sources = []string{"hub", "dockerhub"}
	if err := cfg.Validate(); err == nil {
		t.Error("非法搜索来源应被拒绝")
	}

	cfg = Default()
	cfg.Search.Sources = nil
	if err := cfg.Validate(); err != nil {
		t.Fatalf("空来源应回落到默认值: %v", err)
	}
	if len(cfg.Search.Sources) != 3 {
		t.Errorf("默认来源 = %v", cfg.Search.Sources)
	}
	// 默认开启 Quay 更新时间补查。
	if cfg.Search.EnrichUpdated == nil || !*cfg.Search.EnrichUpdated {
		t.Errorf("默认应开启 enrichUpdated")
	}
}

func TestSearchSourcesAcceptMCR(t *testing.T) {
	cfg := Default()
	cfg.Search.Sources = []string{" MCR ", "hub", "hub"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("mcr 应被接受: %v", err)
	}
	if len(cfg.Search.Sources) != 2 || cfg.Search.Sources[0] != "mcr" {
		t.Errorf("来源应去重并小写，got %v", cfg.Search.Sources)
	}

	cfg = Default()
	cfg.Search.Sources = []string{"hub"}
	f := false
	cfg.Search.EnrichUpdated = &f
	if err := cfg.Validate(); err != nil || *cfg.Search.EnrichUpdated {
		t.Errorf("显式关闭 enrichUpdated 应保留: %v %v", err, cfg.Search.EnrichUpdated)
	}
}

func TestFreshnessConfigDefaults(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !cfg.Selection.PreferFresh {
		t.Error("默认应开启 preferFresh")
	}
	if len(cfg.Selection.FreshTags) != 1 || cfg.Selection.FreshTags[0] != "latest" {
		t.Errorf("默认 freshTags = %v", cfg.Selection.FreshTags)
	}
	if cfg.Selection.FreshnessTTL.D() != 10*time.Minute {
		t.Errorf("默认 freshnessTTL = %s", cfg.Selection.FreshnessTTL.D())
	}
	if cfg.Selection.FreshnessTolerance.D() != 0 {
		t.Errorf("默认 freshnessTolerance = %s，应为 0（严格）", cfg.Selection.FreshnessTolerance.D())
	}

	cfg = Default()
	cfg.Selection.FreshTags = []string{"LATEST", "Stable", " ", "latest"}
	cfg.Selection.FreshnessTTL = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(cfg.Selection.FreshTags) != 2 || cfg.Selection.FreshTags[0] != "latest" || cfg.Selection.FreshTags[1] != "stable" {
		t.Errorf("freshTags 应小写去重，got %v", cfg.Selection.FreshTags)
	}
	if cfg.Selection.FreshnessTTL.D() != 10*time.Minute {
		t.Errorf("空 TTL 应回落到 10m，got %s", cfg.Selection.FreshnessTTL.D())
	}

	bad := Default()
	bad.Selection.FreshnessTolerance = Duration(-time.Second)
	if err := bad.Validate(); err == nil {
		t.Error("负的 freshnessTolerance 应被拒绝")
	}

	short := Default()
	short.Selection.FreshnessTTL = Duration(10 * time.Second)
	if err := short.Validate(); err == nil {
		t.Error("过短的 freshnessTTL 应被拒绝")
	}
}
