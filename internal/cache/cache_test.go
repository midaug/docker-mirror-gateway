package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testCache(t *testing.T, maxBytes int64) *Cache {
	t.Helper()
	c, err := New(Config{Dir: t.TempDir(), MaxBytes: maxBytes, TagTTL: time.Minute, ManifestMaxAge: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestManifestCacheRoundTrip(t *testing.T) {
	c := testCache(t, 1<<20)
	body := []byte(`{"schemaVersion":2}`)
	m := &Manifest{Body: body, Digest: digestOf(body), MediaType: "application/json", Mutable: true, FetchedAt: time.Now()}
	if err := c.PutManifest("library/nginx@latest#multi", m); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}

	got, ok := c.GetManifest("library/nginx@latest#multi")
	if !ok {
		t.Fatal("未命中刚写入的 manifest")
	}
	if string(got.Body) != string(body) || got.Digest != m.Digest {
		t.Fatalf("读回内容不一致: %+v", got)
	}

	if _, ok := c.GetManifest("不存在的 key"); ok {
		t.Error("不存在的 key 不应命中")
	}
}

func TestManifestTagTTL(t *testing.T) {
	c := testCache(t, 1<<20)
	c.tagTTL = time.Minute
	body := []byte(`{}`)
	m := &Manifest{Body: body, Digest: digestOf(body), Mutable: true, FetchedAt: time.Now().Add(-2 * time.Minute)}
	if err := c.PutManifest("tag-key", m); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.GetManifest("tag-key"); ok {
		t.Error("过期的 tag 缓存不应命中")
	}

	// 不可变内容（按 digest）不受 TTL 影响
	m2 := &Manifest{Body: body, Digest: digestOf(body), Mutable: false, FetchedAt: time.Now().Add(-48 * time.Hour)}
	if err := c.PutManifest("digest-key", m2); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.GetManifest("digest-key"); !ok {
		t.Error("不可变缓存不应过期")
	}
}

func TestBlobWriter(t *testing.T) {
	c := testCache(t, 1<<20)
	data := []byte("some layer bytes")
	digest := digestOf(data)

	w, err := c.NewBlobWriter(digest)
	if err != nil {
		t.Fatalf("NewBlobWriter: %v", err)
	}
	if _, err := w.Write(data[:5]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data[5:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	size, ok := c.HasBlob(digest)
	if !ok || size != int64(len(data)) {
		t.Fatalf("HasBlob = (%d,%v)", size, ok)
	}
	f, n, err := c.OpenBlob(digest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if n != int64(len(data)) {
		t.Errorf("OpenBlob 大小 = %d", n)
	}
	buf := make([]byte, len(data))
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != string(data) {
		t.Error("读取内容不一致")
	}
}

func TestBlobWriterDigestMismatch(t *testing.T) {
	c := testCache(t, 1<<20)
	data := []byte("real content")
	wrongDigest := digestOf([]byte("other content"))

	w, err := c.NewBlobWriter(wrongDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err == nil {
		t.Fatal("digest 不匹配时 Commit 应报错")
	}
	if _, ok := c.HasBlob(wrongDigest); ok {
		t.Error("校验失败的内容不应落盘")
	}

	// 临时文件应被清理
	_ = filepath.WalkDir(filepath.Join(c.Dir(), "blobs"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			t.Errorf("残留文件: %s", path)
		}
		return nil
	})
}

func TestBlobAbort(t *testing.T) {
	c := testCache(t, 1<<20)
	digest := digestOf([]byte("x"))
	w, err := c.NewBlobWriter(digest)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("x"))
	w.Abort()
	if _, ok := c.HasBlob(digest); ok {
		t.Error("Abort 后不应存在 blob")
	}
}

func TestEnforceLimitEvictsOldest(t *testing.T) {
	c := testCache(t, 1000)

	write := func(content []byte, age time.Duration) string {
		digest := digestOf(content)
		w, err := c.NewBlobWriter(digest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
		path, _ := c.blobPath(digest)
		ts := time.Now().Add(-age)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatal(err)
		}
		return digest
	}

	oldest := write(bytes.Repeat([]byte("a"), 400), 48*time.Hour)
	write(bytes.Repeat([]byte("b"), 400), 24*time.Hour)
	write(bytes.Repeat([]byte("c"), 400), time.Minute)

	res, err := c.EnforceLimit()
	if err != nil {
		t.Fatalf("EnforceLimit: %v", err)
	}
	if res.Removed == 0 || res.FreedBytes == 0 {
		t.Fatalf("应淘汰部分文件: %+v", res)
	}
	if res.TotalBytes > 1000 {
		t.Errorf("淘汰后仍超限: %d", res.TotalBytes)
	}
	if _, ok := c.HasBlob(oldest); ok {
		t.Error("最久未访问的 blob 应被淘汰")
	}
}

func TestPurge(t *testing.T) {
	c := testCache(t, 1<<20)
	body := []byte("x")
	w, _ := c.NewBlobWriter(digestOf(body))
	_, _ = w.Write(body)
	_ = w.Commit()
	if c.Size() == 0 {
		t.Fatal("写入后大小应大于 0")
	}
	if err := c.Purge(); err != nil {
		t.Fatal(err)
	}
	if c.Size() != 0 || c.Objects() != 0 {
		t.Errorf("清空后统计未归零: size=%d objects=%d", c.Size(), c.Objects())
	}
	entries, _ := os.ReadDir(filepath.Join(c.Dir(), "blobs"))
	if len(entries) != 0 {
		t.Errorf("清空后仍有残留: %v", entries)
	}
}

func TestNoopStore(t *testing.T) {
	n := NewNoop()
	if n.Backend() != "none" {
		t.Errorf("Backend = %q", n.Backend())
	}
	if n.Size() != 0 || n.Objects() != 0 || n.MaxBytes() != 0 {
		t.Error("空实现不该报告占用")
	}
	if _, ok := n.GetManifest("anything"); ok {
		t.Error("空实现不该命中 manifest")
	}
	if err := n.PutManifest("k", &Manifest{Body: []byte("x")}); err != nil {
		t.Errorf("空实现写入应静默成功: %v", err)
	}
	if _, ok := n.HasBlob("sha256:abc"); ok {
		t.Error("空实现不该命中 blob")
	}
	const dg = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if _, _, err := n.OpenBlob(dg); err != ErrDisabled {
		t.Errorf("OpenBlob 应返回 ErrDisabled, got %v", err)
	}
	if _, err := n.NewBlobWriter(dg); err != ErrDisabled {
		t.Errorf("NewBlobWriter 应返回 ErrDisabled, got %v", err)
	}
	if _, err := n.EnforceLimit(); err != nil {
		t.Errorf("EnforceLimit 应无操作: %v", err)
	}
	if err := n.Purge(); err != nil {
		t.Errorf("Purge 应无操作: %v", err)
	}
	if err := n.Close(); err != nil {
		t.Errorf("Close 应无操作: %v", err)
	}
}
