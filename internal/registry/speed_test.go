package registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
)

// reportSpeed 用 n 字节在 d 时间内传完这一事实喂一个吞吐样本。
func reportSpeed(u *Upstream, n int64, d time.Duration) {
	u.ReportSpeed(n, d)
}

func TestReportSpeedIgnoresNoisySamples(t *testing.T) {
	u := &Upstream{Name: "a", healthy: true}
	// 太小：握手开销占主导，测出来的数没有参考价值
	reportSpeed(u, 4<<10, 100*time.Millisecond)
	reportSpeed(u, 32<<10, 100*time.Millisecond)
	if got := u.SpeedBPS(); got != 0 {
		t.Fatalf("过小的样本不该被采纳，SpeedBPS = %v", got)
	}
	if n, _ := u.SpeedSample(); n != 0 {
		t.Fatalf("样本数 = %d, 期望 0", n)
	}

	// 够了门限就采纳，哪怕传输快得离谱（本地回环就是这种）
	reportSpeed(u, 1<<20, 100*time.Millisecond) // 10 MB/s
	if got := u.SpeedBPS(); got < 9e6 || got > 11e6 {
		t.Fatalf("首个有效样本应直接采纳，SpeedBPS = %v", got)
	}
	if n, _ := u.SpeedSample(); n != 1 {
		t.Fatalf("样本数 = %d, 期望 1", n)
	}
}

func TestReportSpeedUsesEWMA(t *testing.T) {
	u := &Upstream{Name: "a", healthy: true}
	reportSpeed(u, 1<<20, time.Second) // 1 MB/s
	reportSpeed(u, 4<<20, time.Second) // 4 MB/s

	// 1*(1-0.35) + 4*0.35 = 2.05 MB/s
	got := u.SpeedBPS() / (1 << 20)
	if got < 1.9 || got > 2.2 {
		t.Fatalf("EWMA 后吞吐 = %.2f MB/s, 期望约 2.05", got)
	}
}

func TestPoolSpeedStrategyPrefersThroughput(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 10, healthy: true}
	b := &Upstream{Name: "b", Priority: 20, healthy: true}
	reportSpeed(a, 1<<20, time.Second) // 1 MB/s
	reportSpeed(b, 8<<20, time.Second) // 8 MB/s

	p := newTestPool("speed", a, b)
	got := names(p.Candidates())
	if got[0] != "b" {
		t.Fatalf("排序 = %v, 期望吞吐更高的 b 优先（priority 更小也不该救 a）", got)
	}

	// priority 策略下仍然应该尊重配置
	if got := names(newTestPool("priority", a, b).Candidates()); got[0] != "a" {
		t.Fatalf("priority 策略排序 = %v, 期望 a 优先", got)
	}
}

func TestPoolSpeedToleranceFallsBackToLatency(t *testing.T) {
	// 吞吐只差 5%，落在 25% 容差内 → 视为持平，此时才看延迟。
	a := &Upstream{Name: "a", Priority: 10, healthy: true}
	b := &Upstream{Name: "b", Priority: 5, healthy: true} // b 的配置优先级更高
	reportSpeed(a, 10<<20, time.Second)
	reportSpeed(b, 9_500_000, time.Second)
	a.ReportSuccess(100 * time.Millisecond)
	b.ReportSuccess(900 * time.Millisecond) // 比 a 慢 800ms，超出 400ms 的容忍

	got := names(newTestPool("speed", a, b).Candidates())
	if got[0] != "a" {
		t.Fatalf("排序 = %v, 吞吐持平时延迟更低的 a 应优先", got)
	}
}

func TestPoolSpeedToleranceIgnoresSmallLatencyGap(t *testing.T) {
	// 吞吐持平 + 延迟只差 5ms → 不值得为它换源，退回 priority。
	a := &Upstream{Name: "a", Priority: 10, healthy: true}
	b := &Upstream{Name: "b", Priority: 5, healthy: true}
	reportSpeed(a, 10<<20, time.Second)
	reportSpeed(b, 10<<20, time.Second)
	a.ReportSuccess(100 * time.Millisecond)
	b.ReportSuccess(95 * time.Millisecond)

	got := names(newTestPool("speed", a, b).Candidates())
	if got[0] != "b" {
		t.Fatalf("排序 = %v, 延迟差可忽略时应按 priority 排（b=5）", got)
	}
}

func TestPoolSpeedPrefersUpstreamWithSamples(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 10, healthy: true} // 还没有样本
	b := &Upstream{Name: "b", Priority: 20, healthy: true}
	reportSpeed(b, 5<<20, time.Second)

	got := names(newTestPool("speed", a, b).Candidates())
	if got[0] != "b" {
		t.Fatalf("排序 = %v, 有吞吐样本的源应排在无样本之前", got)
	}

	// 两个都没有样本时退回 priority。
	c := &Upstream{Name: "c", Priority: 10, healthy: true}
	d := &Upstream{Name: "d", Priority: 20, healthy: true}
	if got := names(newTestPool("speed", c, d).Candidates()); got[0] != "c" {
		t.Fatalf("排序 = %v, 无样本时应按 priority", got)
	}
}

func TestPoolSpeedUnknownLatencyIsNotTreatedAsFastest(t *testing.T) {
	// 两个源吞吐持平。a 探测从没成功过（latency 还是 0），b 记录到 800ms。
	// 0 表示「未知」，不能当成「零延迟」让 a 白占便宜。
	a := &Upstream{Name: "a", Priority: 20, healthy: true}
	b := &Upstream{Name: "b", Priority: 10, healthy: true}
	reportSpeed(a, 5<<20, time.Second)
	reportSpeed(b, 5<<20, time.Second)
	b.ReportSuccess(800 * time.Millisecond)

	got := names(newTestPool("speed", a, b).Candidates())
	if got[0] != "b" {
		t.Fatalf("排序 = %v, 延迟未知的源不该被当成最快", got)
	}
}

func TestPoolSpeedUnhealthyStillGoesLast(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 10, healthy: true, failThreshold: 1}
	b := &Upstream{Name: "b", Priority: 20, healthy: true, failThreshold: 1}
	reportSpeed(a, 1<<20, time.Second) // 快但会挂
	reportSpeed(b, 8<<20, time.Second)
	a.ReportFailure(fmt.Errorf("connection refused"))

	got := names(newTestPool("speed", a, b).Candidates())
	if got[0] != "b" {
		t.Fatalf("排序 = %v, 失效源必须排最后，哪怕它更快", got)
	}
}

// TestPoolProbeSpeedProducesSample 验证主动探测能从 manifest 一路走到 blob 并产出吞吐样本。
func TestPoolProbeSpeedProducesSample(t *testing.T) {
	layer := bytes.Repeat([]byte("x"), 512<<10)
	sum := sha256.Sum256(layer)
	layerDigest := "sha256:" + hex.EncodeToString(sum[:])
	manifestBody := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":%q,"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","digest":%q,"size":%d}]}`,
		MediaTypeDockerManifest, layerDigest, len(layer)))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/":
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", MediaTypeDockerManifest)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(manifestBody)
		case strings.Contains(r.URL.Path, "/blobs/"):
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(layer))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	u, err := NewUpstream(config.UpstreamConfig{Name: "u1", URL: srv.URL, Priority: 10}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	p := &Pool{
		upstreams:      []*Upstream{u},
		strategy:       "speed",
		probeRepo:      "library/alpine",
		probeTag:       "latest",
		health:         config.HealthConfig{Timeout: config.Duration(5 * time.Second)},
		speedProbe:     256 << 10,
		speedInterval:  10 * time.Minute,
		speedTolerance: 0.25,
		log:            testLogger(),
	}

	if err := p.Probe(context.Background(), u); err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if u.SpeedBPS() <= 0 {
		t.Fatal("探测后应该有吞吐样本")
	}
	if n, at := u.SpeedSample(); n != 1 || at.IsZero() {
		t.Fatalf("样本数 = %d, 时间 = %v", n, at)
	}
}

// TestPoolProbeSpeedThrottled 验证 interval 内不会重复测速。
func TestPoolProbeSpeedThrottled(t *testing.T) {
	var blobHits int
	layer := bytes.Repeat([]byte("y"), 512<<10)
	sum := sha256.Sum256(layer)
	layerDigest := "sha256:" + hex.EncodeToString(sum[:])
	manifestBody := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"mediaType":%q,"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","digest":%q,"size":%d}]}`,
		MediaTypeDockerManifest, layerDigest, len(layer)))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/":
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", MediaTypeDockerManifest)
			_, _ = w.Write(manifestBody)
		case strings.Contains(r.URL.Path, "/blobs/"):
			blobHits++
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(layer))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	u, err := NewUpstream(config.UpstreamConfig{Name: "u1", URL: srv.URL, Priority: 10}, "test", 3, testLogger(), metrics.New())
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	p := &Pool{
		upstreams:      []*Upstream{u},
		strategy:       "speed",
		probeRepo:      "library/alpine",
		probeTag:       "latest",
		health:         config.HealthConfig{Timeout: config.Duration(5 * time.Second)},
		speedProbe:     256 << 10,
		speedInterval:  10 * time.Minute,
		speedTolerance: 0.25,
		log:            testLogger(),
	}

	for i := 0; i < 3; i++ {
		if err := p.Probe(context.Background(), u); err != nil {
			t.Fatalf("第 %d 次 Probe: %v", i, err)
		}
	}
	if blobHits != 1 {
		t.Fatalf("blob 被下载了 %d 次，interval 内应该只测一次", blobHits)
	}
}
