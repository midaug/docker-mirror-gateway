package registry

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

// TestUpstreamUsesConfiguredProxy 验证单个上游可以走专属代理。
func TestUpstreamUsesConfiguredProxy(t *testing.T) {
	var hits atomic.Int64
	var gotHost atomic.Value

	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotHost.Store(r.Host)
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
	}))
	defer proxySrv.Close()

	u, err := NewUpstream(config.UpstreamConfig{
		Name:  "proxied",
		URL:   "http://registry.invalid",
		Proxy: proxySrv.URL,
	}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := u.Ping(ctx); err != nil {
		t.Fatalf("Ping 经代理失败: %v", err)
	}
	if hits.Load() == 0 {
		t.Fatal("请求没有经过配置的代理")
	}
	// HTTP 代理收到的是 absolute-form 请求，Host 仍是上游域名。
	if host, _ := gotHost.Load().(string); host != "registry.invalid" {
		t.Errorf("代理收到的 Host = %q, 期望 registry.invalid", host)
	}
	if st := u.Status(); st.Proxy != proxySrv.URL {
		t.Errorf("状态中的 proxy = %q", st.Proxy)
	}
}

// TestUpstreamProxyCredentials 验证代理账号密码既能写在地址里，也能拆成独立字段。
func TestUpstreamProxyCredentials(t *testing.T) {
	var gotAuth atomic.Value
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Proxy-Authorization"))
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "{}")
	}))
	defer proxySrv.Close()

	u, err := NewUpstream(config.UpstreamConfig{
		Name:          "proxied",
		URL:           "http://registry.invalid",
		Proxy:         proxySrv.URL,
		ProxyUsername: "alice",
		ProxyPassword: "s3cret!",
	}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := u.Ping(ctx); err != nil {
		t.Fatalf("Ping 经代理失败: %v", err)
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret!"))
	if auth, _ := gotAuth.Load().(string); auth != want {
		t.Errorf("Proxy-Authorization = %q, 期望 %q", auth, want)
	}
	// 状态里的代理地址必须脱敏。
	if st := u.Status(); strings.Contains(st.Proxy, "alice") || strings.Contains(st.Proxy, "s3cret") {
		t.Errorf("状态里的代理未脱敏: %q", st.Proxy)
	}
}

func TestBuildProxyModes(t *testing.T) {
	fn, label, err := buildProxy("")
	if err != nil || fn == nil || label != "env" {
		t.Errorf("空配置应沿用环境变量: label=%q err=%v", label, err)
	}
	fn, label, err = buildProxy(config.ProxyDirect)
	if err != nil || fn != nil || label != "direct" {
		t.Errorf("direct 应为无代理: label=%q err=%v", label, err)
	}

	fn, label, err = buildProxy("socks5://user:secret@127.0.0.1:1080")
	if err != nil || fn == nil {
		t.Fatalf("socks5 代理应被支持: %v", err)
	}
	if strings.Contains(label, "secret") || !strings.Contains(label, "***") {
		t.Errorf("代理凭据未脱敏: %q", label)
	}

	if _, _, err := buildProxy("ftp://127.0.0.1:21"); err == nil {
		t.Error("ftp 代理应被拒绝")
	}
}

// TestUpstreamDirectSkipsProxy 确认 direct 模式不会把请求交给代理。
func TestUpstreamDirectSkipsProxy(t *testing.T) {
	var hits atomic.Int64
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer proxySrv.Close()

	u, err := NewUpstream(config.UpstreamConfig{
		Name:    "direct",
		URL:     "http://127.0.0.1:1",
		Proxy:   config.ProxyDirect,
		Timeout: config.Duration(2 * time.Second),
	}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := u.Ping(ctx); err == nil {
		t.Fatal("直连不可达端口应失败")
	}
	if hits.Load() != 0 {
		t.Fatalf("direct 模式不应经过代理，实际被调用 %d 次", hits.Load())
	}
}
