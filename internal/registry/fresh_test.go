package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

// freshRegistry 是一个只提供 manifest + config 的最小注册表，用于新旧探测。
type freshRegistry struct {
	*httptest.Server
	manifestReqs atomic.Int64
	configReqs   atomic.Int64
}

// newFreshRegistry 起一个假注册表，latest 指向 created 指定的镜像。
// 同一个 created 会构造出同一个 manifest，方便验证「digest 相同就跳过」。
func newFreshRegistry(t *testing.T, created string) *freshRegistry {
	t.Helper()
	return newFreshRegistryWithDelay(t, created, 0)
}

// newFreshRegistryWithDelay 在每次响应前停顿 delay，用来模拟慢源。
func newFreshRegistryWithDelay(t *testing.T, created string, delay time.Duration) *freshRegistry {
	t.Helper()
	f := &freshRegistry{}
	configBody := []byte(fmt.Sprintf(`{"created":%q}`, created))
	sum := sha256.Sum256(configBody)
	cfgDigest := "sha256:" + hex.EncodeToString(sum[:])
	manifestBody, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeDockerManifest,
		"config": map[string]any{
			"mediaType": "application/vnd.docker.container.image.v1+json",
			"size":      len(configBody),
			"digest":    cfgDigest,
		},
		"layers": []any{},
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/manifests/latest"):
			f.manifestReqs.Add(1)
			w.Header().Set("Content-Type", MediaTypeDockerManifest)
			_, _ = w.Write(manifestBody)
		case strings.Contains(r.URL.Path, "/blobs/"):
			f.configReqs.Add(1)
			w.Header().Set("Content-Type", "application/vnd.docker.container.image.v1+json")
			_, _ = w.Write(configBody)
		default:
			http.NotFound(w, r)
		}
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Server.Close)
	return f
}

func testUpstream(t *testing.T, name, url string) *Upstream {
	t.Helper()
	u, err := NewUpstream(config.UpstreamConfig{
		Name:     name,
		URL:      url,
		Priority: 100,
		Timeout:  config.Duration(3 * time.Second),
	}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	return u
}

func freshPool(upstreams ...*Upstream) *Pool {
	p := newTestPool("speed", upstreams...)
	p.preferFresh = true
	p.freshTags = []string{"latest"}
	p.fresh = newFreshnessCache(10 * time.Minute)
	return p
}

func TestCandidatesForPrefersNewestLatest(t *testing.T) {
	oldSrv := newFreshRegistry(t, "2026-11-01T00:00:00Z")
	newSrv := newFreshRegistry(t, "2026-11-15T00:00:00Z")

	oldUp := testUpstream(t, "old-but-fast", oldSrv.URL)
	newUp := testUpstream(t, "new-but-slow", newSrv.URL)
	// 没有新旧信息时 old 更快，按速度应该排第一。
	oldUp.ReportSpeed(16<<20, time.Second)
	newUp.ReportSpeed(256<<10, time.Second)
	p := freshPool(oldUp, newUp)

	if got := names(p.Candidates()); got[0] != "old-but-fast" {
		t.Fatalf("基线顺序 = %v, 期望 old-but-fast 在前", got)
	}
	list, manifests := p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	got := names(list)
	if got[0] != "new-but-slow" {
		t.Errorf("latest 应优先更新的源, got %v", got)
	}
	// 探测阶段下载的 manifest 应该能直接复用。
	if m := manifests["new-but-slow"]; m == nil || len(m.Body) == 0 {
		t.Errorf("缺少探测阶段缓存的 manifest: %v", manifests)
	}
}

func TestCandidatesForSkipsConfigWhenDigestEqual(t *testing.T) {
	srv1 := newFreshRegistry(t, "2026-11-15T00:00:00Z")
	srv2 := newFreshRegistry(t, "2026-11-15T00:00:00Z")
	a := testUpstream(t, "a", srv1.URL)
	b := testUpstream(t, "b", srv2.URL)

	p := freshPool(a, b)
	p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	if n := srv1.configReqs.Load() + srv2.configReqs.Load(); n != 0 {
		t.Errorf("清单一致时不该下载 config，实际 %d 次", n)
	}
}

func TestCandidatesForIgnoresNonFreshTags(t *testing.T) {
	srv1 := newFreshRegistry(t, "2026-11-01T00:00:00Z")
	srv2 := newFreshRegistry(t, "2026-11-15T00:00:00Z")
	a := testUpstream(t, "a", srv1.URL)
	b := testUpstream(t, "b", srv2.URL)
	p := freshPool(a, b)

	p.CandidatesForManifest(context.Background(), "library/app", "1.27", "")
	p.CandidatesForManifest(context.Background(), "library/app", "sha256:"+strings.Repeat("0", 64), "")
	if n := srv1.manifestReqs.Load() + srv2.manifestReqs.Load(); n != 0 {
		t.Errorf("非 freshTags 不该探测，实际 %d 次请求", n)
	}

	// 关闭开关后同样不探测。
	p.preferFresh = false
	p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	if n := srv1.manifestReqs.Load(); n != 0 {
		t.Errorf("关闭 preferFresh 后不该探测，实际 %d 次", n)
	}
}

func TestCandidatesForCachesFreshness(t *testing.T) {
	srv1 := newFreshRegistry(t, "2026-11-01T00:00:00Z")
	srv2 := newFreshRegistry(t, "2026-11-15T00:00:00Z")
	a := testUpstream(t, "a", srv1.URL)
	b := testUpstream(t, "b", srv2.URL)
	p := freshPool(a, b)

	p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	after := srv1.manifestReqs.Load()
	if after == 0 {
		t.Fatal("首次应探测")
	}
	// 第二次命中缓存：不再探测，也不再附带 prefetch 的 manifest。
	list, manifests := p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	if got := srv1.manifestReqs.Load(); got != after {
		t.Errorf("TTL 内不该重复探测: %d -> %d", after, got)
	}
	if manifests != nil {
		t.Errorf("缓存命中时不该返回 prefetch 结果")
	}
	if names(list)[0] != "b" {
		t.Errorf("缓存命中后仍应按新旧排序, got %v", names(list))
	}
}

func TestParseCreated(t *testing.T) {
	if v := parseCreated("2026-11-15T01:02:03.123Z"); v.Year() != 2026 {
		t.Errorf("正常时间解析失败: %v", v)
	}
	if v := parseCreated("0001-01-01T00:00:00Z"); !v.IsZero() {
		t.Errorf("零值应被忽略: %v", v)
	}
	if v := parseCreated(""); !v.IsZero() {
		t.Errorf("空字符串应返回零值")
	}
}

// 慢源不该拖慢首次请求，但探测要在后台跑完并写入缓存。
func TestCandidatesForProbesSlowSourceInBackground(t *testing.T) {
	if testing.Short() {
		t.Skip("依赖真实等待")
	}
	fast := newFreshRegistry(t, "2026-11-01T00:00:00Z")
	slow := newFreshRegistryWithDelay(t, "2026-11-15T00:00:00Z", 1200*time.Millisecond)

	fastUp := testUpstream(t, "fast", fast.URL)
	slowUp := testUpstream(t, "slow", slow.URL)
	fastUp.ReportSpeed(16<<20, time.Second)

	p := freshPool(fastUp, slowUp)
	start := time.Now()
	list, _ := p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("首次请求不该等慢源，实际耗时 %v", elapsed)
	}
	if names(list)[0] != "fast" {
		t.Errorf("超时后应按原策略选源, got %v", names(list))
	}

	// 后台探测完成后应写入缓存，下一次请求就能用上完整结果。
	key := "library/app@latest"
	deadline := time.Now().Add(6 * time.Second)
	for {
		if _, ok := p.fresh.peek(key); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("后台探测结果没有写入缓存")
		}
		time.Sleep(50 * time.Millisecond)
	}

	list2, _ := p.CandidatesForManifest(context.Background(), "library/app", "latest", "")
	if names(list2)[0] != "slow" {
		t.Errorf("缓存写入后应优先更新的源, got %v", names(list2))
	}
}

// claim/release 保证同一仓库的并发探测只有一个在跑，其余请求立即跳过。
func TestFreshnessClaimExclusive(t *testing.T) {
	c := newFreshnessCache(time.Minute)
	if !c.claim("repo@latest") {
		t.Fatal("首次认领应成功")
	}
	if c.claim("repo@latest") {
		t.Fatal("认领中的 key 不应再次认领成功")
	}
	c.release("repo@latest")
	if !c.claim("repo@latest") {
		t.Fatal("释放后应可以再次认领")
	}
	c.release("repo@latest")
}
