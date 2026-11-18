package registry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newPingRegistry 起一个最小注册表并统计 /v2/ ping 次数，
// 用于验证定时探测是否真的发出了请求。
func newPingRegistry(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var pings atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
			pings.Add(1)
		}
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &pings
}

func newTrafficPool(upstreams ...*Upstream) *Pool {
	p := newTestPool("speed", upstreams...)
	p.trafficWindow = 2 * time.Minute
	return p
}

func TestProbeScheduledSkipsRecentTraffic(t *testing.T) {
	srv, pings := newPingRegistry(t)
	u := testUpstream(t, "busy", srv.URL)
	p := newTrafficPool(u)

	// 没有真实流量：正常探测。
	if err := p.probeScheduled(context.Background(), u); err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if n := pings.Load(); n != 1 {
		t.Fatalf("无真实流量时应主动探测一次，实际 ping=%d", n)
	}

	// 刚刚成功处理过真实拉取：延迟与吞吐已有真实样本，跳过。
	u.ReportTraffic(5 * time.Millisecond)
	if err := p.probeScheduled(context.Background(), u); err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if n := pings.Load(); n != 1 {
		t.Fatalf("窗口内有真实流量时不应再探测，实际 ping=%d", n)
	}
}

func TestProbeScheduledResumesAfterWindow(t *testing.T) {
	srv, pings := newPingRegistry(t)
	u := testUpstream(t, "idle", srv.URL)
	p := newTrafficPool(u)

	u.ReportTraffic(5 * time.Millisecond)
	// 把窗口缩到最小，模拟最后一次真实拉取已经出了窗口。
	p.trafficWindow = time.Nanosecond
	time.Sleep(time.Millisecond)
	if err := p.probeScheduled(context.Background(), u); err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if n := pings.Load(); n != 1 {
		t.Fatalf("窗口过后应恢复主动探测，实际 ping=%d", n)
	}
}

func TestManualProbeIgnoresTrafficWindow(t *testing.T) {
	srv, pings := newPingRegistry(t)
	u := testUpstream(t, "manual", srv.URL)
	p := newTrafficPool(u)

	u.ReportTraffic(5 * time.Millisecond)
	if err := p.Probe(context.Background(), u); err != nil {
		t.Fatalf("手动探测失败: %v", err)
	}
	if n := pings.Load(); n != 1 {
		t.Fatalf("控制台手动探测不受窗口限制，实际 ping=%d", n)
	}
}

func TestProbeSkipKeepsAccumulatedFailures(t *testing.T) {
	srv, pings := newPingRegistry(t)
	u := testUpstream(t, "flaky", srv.URL)
	p := newTrafficPool(u)

	// 上次成功拉取在窗口内，之后该源开始出错（真实流量失败会累积 failures）。
	u.ReportTraffic(0)
	u.ReportFailure(errors.New("timeout"))
	u.ReportFailure(errors.New("timeout"))

	// 跳过探测，但绝不许把正在累积的失败计数清零，
	// 否则一个已经病了的上游会被永远当健康源轮询。
	if err := p.probeScheduled(context.Background(), u); err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if n := pings.Load(); n != 0 {
		t.Fatalf("窗口内应跳过探测，实际 ping=%d", n)
	}
	if got := u.Status().Failures; got != 2 {
		t.Fatalf("跳过探测不应清零失败计数，期望 2 实际 %d", got)
	}
}

func TestFailedTrafficDoesNotRefreshWindow(t *testing.T) {
	srv, pings := newPingRegistry(t)
	u := testUpstream(t, "sick", srv.URL)
	p := newTrafficPool(u)

	// 只有成功才续窗口：失败的真实流量不能让探测一直被跳过，
	// 否则故障源要等到下一次成功（不可能发生）之后才会被探测发现。
	u.ReportFailure(errors.New("connection refused"))
	if u.RecentTraffic(p.trafficWindow) {
		t.Fatal("失败的真实流量不应刷新 traffic window")
	}
	if err := p.probeScheduled(context.Background(), u); err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	if n := pings.Load(); n != 1 {
		t.Fatalf("只有失败流量时应正常探测，实际 ping=%d", n)
	}
}
