package activity

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var errTest = errors.New("test failure")

func TestPersistentSaveAndReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "activity.json")

	tr := NewPersistent(10, path, nil)
	// 三种终态各写一条。
	e1 := tr.Begin(KindManifest, "library/nginx", "1.27", "dockerhub")
	e1.SetUpstream("official")
	e1.SetBytes(2048)
	e1.Finish()

	e2 := tr.Begin(KindBlob, "library/nginx", "sha256:abc", "dockerhub")
	e2.SetCache("hit")
	e2.SetBytes(4096)
	e2.FinishCached()

	e3 := tr.Begin(KindBlob, "library/nginx", "sha256:def", "dockerhub")
	e3.Fail(errTest)

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("持久化文件未创建: %v", err)
	}
	// 文件里不应残留临时文件。
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("临时文件未清理")
	}

	loaded := NewPersistent(10, path, nil)
	defer loaded.Close()
	records := loaded.List(0)
	if len(records) != 3 {
		t.Fatalf("重载记录数 = %d, 期望 3", len(records))
	}
	// 文件按正序保存，List 倒序返回：最后写入的失败记录应在最前。
	if records[0].State != StateFailed || records[0].Reference != "sha256:def" {
		t.Errorf("最新记录不符: %+v", records[0])
	}
	if records[1].State != StateCached || records[1].Cache != "hit" {
		t.Errorf("缓存记录不符: %+v", records[1])
	}
	if records[2].State != StateDone || records[2].Upstream != "official" || records[2].Bytes != 2048 {
		t.Errorf("完成记录不符: %+v", records[2])
	}
	// ID 续号，不能与历史冲突。
	next := loaded.Begin(KindBlob, "library/busybox", "latest", "dockerhub")
	next.Finish()
	if next.id != 4 {
		t.Errorf("新记录 ID = %d, 期望 4", next.id)
	}
}

func TestPersistentStoreLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "activity.json")
	const storeLimit = 7

	tr := NewPersistent(storeLimit, path, nil)
	for i := 0; i < 30; i++ {
		e := tr.Begin(KindBlob, "repo", "ref", "")
		e.Finish()
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != storeLimit {
		t.Fatalf("文件记录数 = %d, 期望上限 %d", len(records), storeLimit)
	}
}

func TestPersistentSkipsActiveAndCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "activity.json")
	// 文件里混入一条上次异常退出遗留的进行中记录，加载时应跳过。
	raw := `[{"id":1,"kind":"blob","repo":"repo","state":"active"}]`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	tr := NewPersistent(10, path, nil)
	if got := len(tr.List(0)); got != 0 {
		t.Fatalf("进行中记录应被跳过, 实际 %d 条", got)
	}
	tr.Close()

	// 损坏文件不应导致 panic 或报错，退化为纯内存记录器。
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	tr2 := NewPersistent(10, path, nil)
	e := tr2.Begin(KindManifest, "repo", "latest", "")
	e.Finish()
	if err := tr2.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("损坏文件未恢复为合法内容: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("重载记录数 = %d, 期望 1", len(records))
	}
}

func TestNoPersistenceLeavesNoFiles(t *testing.T) {
	tr := New(10) // 纯内存模式，没有 storePath
	e := tr.Begin(KindBlob, "repo", "ref", "")
	e.Finish()
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if tr.storePath != "" {
		t.Error("纯内存记录器不应设置持久化路径")
	}
}

func TestPersistentSaveAfterDebounce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "activity.json")
	tr := NewPersistent(10, path, nil)
	defer tr.Close()
	e := tr.Begin(KindManifest, "repo", "latest", "")
	e.Finish()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("节流窗口后仍未写盘")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
