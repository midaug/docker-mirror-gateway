package activity

import (
	"errors"
	"testing"
	"time"
)

func TestTrackerKeepsNewestFirst(t *testing.T) {
	tr := New(3)
	for i := 0; i < 5; i++ {
		e := tr.Begin(KindBlob, "library/nginx", "sha256:abc", "dockerhub")
		e.SetUpstream("src-" + string(rune('a'+i)))
		e.Finish()
	}
	got := tr.List(0)
	if len(got) != 3 {
		t.Fatalf("保留条数 = %d, 期望 3", len(got))
	}
	// 最新在前：最后创建的 src-e 应该排第一。
	if got[0].Upstream != "src-e" || got[2].Upstream != "src-c" {
		t.Errorf("顺序不对: %s ... %s", got[0].Upstream, got[2].Upstream)
	}
	if got[0].ID <= got[1].ID {
		t.Errorf("ID 应递减, got %d, %d", got[0].ID, got[1].ID)
	}
}

func TestTrackerKeepsActiveWhenEvicting(t *testing.T) {
	tr := New(3)
	live := tr.Begin(KindBlob, "library/nginx", "sha256:live", "dockerhub")
	live.AddBytes(1 << 20)

	// 后续请求把列表填满，还不能把正在传输的那条挤掉。
	for i := 0; i < 10; i++ {
		e := tr.Begin(KindBlob, "library/redis", "sha256:x", "dockerhub")
		e.SetBytes(10)
		e.Finish()
	}
	var found bool
	for _, r := range tr.List(0) {
		if r.Reference == "sha256:live" {
			found = true
			if r.State != StateActive {
				t.Errorf("进行中的记录状态 = %s", r.State)
			}
		}
	}
	if !found {
		t.Fatal("进行中的记录被淘汰了")
	}
}

func TestEntrySpeedAndStates(t *testing.T) {
	tr := New(10)
	e := tr.Begin(KindBlob, "library/alpine", "sha256:123", "dockerhub")
	e.SetUpstream("nju")
	e.SetCache("miss")
	e.SetTotal(64 << 10)

	if got := e.State(); got != StateActive {
		t.Fatalf("初始状态 = %s", got)
	}
	e.AddBytes(32 << 10)
	time.Sleep(15 * time.Millisecond)
	e.AddBytes(32 << 10)
	e.Finish()

	rec := tr.List(1)[0]
	if rec.State != StateDone {
		t.Errorf("状态 = %s", rec.State)
	}
	if rec.Bytes != 64<<10 || rec.Total != 64<<10 {
		t.Errorf("字节 = %d/%d", rec.Bytes, rec.Total)
	}
	if rec.SpeedBPS <= 0 {
		t.Errorf("速度应大于 0, got %f", rec.SpeedBPS)
	}
	if rec.Upstream != "nju" || rec.Group != "dockerhub" || rec.Cache != "miss" {
		t.Errorf("元数据缺失: %+v", rec)
	}
	if rec.DurationMS < 0 {
		t.Errorf("耗时 = %d", rec.DurationMS)
	}
}

func TestEntrySmallTransferHasNoSpeed(t *testing.T) {
	tr := New(10)
	e := tr.Begin(KindManifest, "library/nginx", "latest", "dockerhub")
	e.SetBytes(1200) // 预取命中这类没有传输计时的场景
	e.Finish()
	if got := tr.List(1)[0].SpeedBPS; got != 0 {
		t.Errorf("小对象不应产生速度值, got %f", got)
	}
}

func TestEntryCachedAndFail(t *testing.T) {
	tr := New(10)
	c := tr.Begin(KindBlob, "library/busybox", "sha256:abc", "")
	c.SetBytes(2048)
	c.FinishCached()

	f := tr.Begin(KindManifest, "library/nope", "latest", "dockerhub")
	f.Fail(errors.New("镜像不存在"))

	items := tr.List(0)
	if items[1].State != StateCached || items[1].Cache != "hit" {
		t.Errorf("缓存记录 = %+v", items[1])
	}
	if items[0].State != StateFailed || items[0].Error == "" {
		t.Errorf("失败记录 = %+v", items[0])
	}

	sum := tr.Summary()
	if sum.Total != 2 || sum.Cached != 1 || sum.Failed != 1 {
		t.Errorf("汇总 = %+v", sum)
	}
}

func TestNilEntryIsSafe(t *testing.T) {
	var e *Entry
	e.SetUpstream("x")
	e.SetBytes(1)
	e.AddBytes(1)
	e.Finish()
	e.Fail(errors.New("x"))
	e.Close()
	if got := e.Snapshot(); got.ID != 0 {
		t.Errorf("nil 快照 = %+v", got)
	}
	var tr *Tracker
	if got := tr.List(10); len(got) != 0 {
		t.Errorf("nil Tracker.List = %v", got)
	}
	if e := tr.Begin(KindBlob, "a", "b", "c"); e != nil {
		t.Error("nil Tracker.Begin 应返回 nil")
	}
}

func TestCloseMarksAborted(t *testing.T) {
	tr := New(10)
	e := tr.Begin(KindBlob, "library/nginx", "sha256:abc", "dockerhub")
	e.Close() // 模拟客户端断开后 defer 兜底
	rec := tr.List(1)[0]
	if rec.State != StateFailed {
		t.Fatalf("状态 = %s", rec.State)
	}
	if rec.Error == "" {
		t.Error("应记录中断原因")
	}
}
