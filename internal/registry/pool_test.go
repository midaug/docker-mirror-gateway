package registry

import (
	"errors"
	"testing"
	"time"
)

func names(list []*Upstream) []string {
	out := make([]string, len(list))
	for i, u := range list {
		out[i] = u.Name
	}
	return out
}

func newTestPool(strategy string, upstreams ...*Upstream) *Pool {
	if strategy == "" {
		strategy = "speed"
	}
	return &Pool{
		upstreams:      upstreams,
		strategy:       strategy,
		speedTolerance: 0.25,
		speedProbe:     256 << 10,
		speedInterval:  10 * time.Minute,
		log:            testLogger(),
	}
}

func TestPoolPriorityOrdering(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 30, healthy: true, failThreshold: 3}
	b := &Upstream{Name: "b", Priority: 10, healthy: true, failThreshold: 3}
	c := &Upstream{Name: "c", Priority: 20, healthy: true, failThreshold: 3}
	p := newTestPool("priority", a, b, c)

	got := names(p.Candidates())
	want := []string{"b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("排序结果 = %v, 期望 %v", got, want)
		}
	}
}

func TestPoolUnhealthyGoesLast(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 30, healthy: true, failThreshold: 1}
	b := &Upstream{Name: "b", Priority: 10, healthy: true, failThreshold: 1}
	c := &Upstream{Name: "c", Priority: 20, healthy: true, failThreshold: 1}

	b.ReportFailure(errors.New("connection refused"))
	if b.Healthy() {
		t.Fatal("连续失败达到阈值后应标记为不可用")
	}

	p := newTestPool("priority", a, b, c)
	got := names(p.Candidates())
	want := []string{"c", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("排序结果 = %v, 期望 %v", got, want)
		}
	}

	// 恢复后应回到优先级顺序
	b.ReportSuccess(10 * time.Millisecond)
	if !b.Healthy() {
		t.Fatal("成功后应恢复为可用")
	}
	got = names(p.Candidates())
	if got[0] != "b" {
		t.Fatalf("恢复后排序 = %v, 期望 b 在最前", got)
	}
}

func TestPoolLatencyStrategy(t *testing.T) {
	a := &Upstream{Name: "a", Priority: 10, healthy: true, failThreshold: 3}
	b := &Upstream{Name: "b", Priority: 20, healthy: true, failThreshold: 3}
	a.ReportSuccess(200 * time.Millisecond)
	b.ReportSuccess(15 * time.Millisecond)

	byPriority := newTestPool("priority", a, b)
	if got := names(byPriority.Candidates()); got[0] != "a" {
		t.Errorf("priority 策略排序 = %v, 期望 a 优先", got)
	}

	byLatency := newTestPool("latency", a, b)
	if got := names(byLatency.Candidates()); got[0] != "b" {
		t.Errorf("latency 策略排序 = %v, 期望 b 优先", got)
	}
}

func TestPoolStatusesAssignRank(t *testing.T) {
	a := &Upstream{Name: "a", BaseURL: "https://a", Priority: 20, healthy: true, failThreshold: 3}
	b := &Upstream{Name: "b", BaseURL: "https://b", Priority: 10, healthy: true, failThreshold: 3}
	p := newTestPool("priority", a, b)

	statuses := p.Statuses()
	if len(statuses) != 2 || statuses[0].Name != "b" || statuses[0].Rank != 1 {
		t.Fatalf("状态列表排序或排名错误: %+v", statuses)
	}
}
