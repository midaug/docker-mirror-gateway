package activity

import (
	"sync"
	"testing"
)

// 并发 SetGroup 与 Snapshot 必须安全：Snapshot 曾在锁外读取 group 字段，
// 与「拿到路由后再补分组」的写路径构成数据竞争（go test -race 可检出）。
func TestSnapshotConcurrentWithSetGroup(t *testing.T) {
	tr := New(10)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := tr.Begin(KindBlob, "repo", "ref", "")
			e.SetUpstream("up")
			e.SetGroup("group")
			e.SetBytes(100)
			e.AddBytes(50)
			e.Finish()
			_ = tr.List(0)
			if i < 0 {
				t.Fatal("unreachable")
			}
		}()
	}
	wg.Wait()
	if n := len(tr.List(0)); n != 8 {
		t.Fatalf("期望 8 条记录，实际 %d", n)
	}
}

// List 的快照里 group 必须与 SetGroup 写入的值一致。
func TestSnapshotIncludesGroup(t *testing.T) {
	tr := New(4)
	e := tr.Begin(KindManifest, "repo", "latest", "")
	e.SetGroup("dockerhub")
	e.Finish()
	recs := tr.List(0)
	if len(recs) != 1 || recs[0].Group != "dockerhub" {
		t.Fatalf("快照缺少分组信息: %+v", recs)
	}
}
