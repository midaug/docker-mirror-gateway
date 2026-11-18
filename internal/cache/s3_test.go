package cache

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 是一个够用的内存版 S3，覆盖 minio-go 在本项目用到的调用：
// BucketExists(HEAD)、PutObject、GetObject、StatObject(HEAD)、RemoveObject、ListObjects。
// 不校验签名——签名本身是 minio-go 的职责，这里只验证请求语义。
type fakeS3 struct {
	mu      sync.Mutex
	bucket  string
	objects map[string]fakeObject
}

type fakeObject struct {
	data []byte
	mod  time.Time
}

func newFakeS3(t *testing.T, bucket string) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{bucket: bucket, objects: make(map[string]fakeObject)}
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeS3) put(key string, data []byte, mod time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{data: data, mod: mod}
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (f *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket != f.bucket {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if key == "" {
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			f.list(w, r)
		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			f.deleteMulti(w, r)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := readBody(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.put(key, body, time.Now())
		w.Header().Set("ETag", `"`+etagOf(body)+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		f.mu.Lock()
		obj, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
			return
		}
		w.Header().Set("ETag", `"`+etagOf(obj.data)+`"`)
		w.Header().Set("Last-Modified", obj.mod.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			serveRange(w, r, obj.data, rng)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(obj.data)
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func serveRange(w http.ResponseWriter, r *http.Request, data []byte, rng string) {
	start, end, ok := parseRange(rng, int64(len(data)))
	if !ok {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	chunk := data[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(chunk)
}

func parseRange(header string, size int64) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false
	}
	left, right, _ := strings.Cut(spec, "-")
	if left == "" {
		n, err := strconv.ParseInt(right, 10, 64)
		if err != nil || n <= 0 || n > size {
			return 0, 0, false
		}
		return size - n, size - 1, true
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if right != "" {
		if v, err := strconv.ParseInt(right, 10, 64); err == nil && v < end {
			end = v
		}
	}
	return start, end, true
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// readBody 兼容 minio-go 的 aws-chunked 流式签名编码。
func readBody(r *http.Request) ([]byte, error) {
	if !strings.HasPrefix(strings.ToUpper(r.Header.Get("X-Amz-Content-Sha256")), "STREAMING") {
		return io.ReadAll(r.Body)
	}
	var out bytes.Buffer
	br := bufio.NewReader(r.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		sizeStr, _, _ := strings.Cut(line, ";")
		n, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("无法解析分块长度 %q", line)
		}
		if n == 0 {
			break
		}
		if _, err := io.CopyN(&out, br, n); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

type listResult struct {
	XMLName  xml.Name       `xml:"ListBucketResult"`
	Name     string         `xml:"Name"`
	Prefix   string         `xml:"Prefix"`
	KeyCount int            `xml:"KeyCount"`
	MaxKeys  int            `xml:"MaxKeys"`
	Contents []listContents `xml:"Contents"`
}

type listContents struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	StorageClass string `xml:"StorageClass"`
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := listResult{Name: f.bucket, Prefix: prefix, KeyCount: len(keys), MaxKeys: 1000}
	for _, k := range keys {
		obj := f.objects[k]
		res.Contents = append(res.Contents, listContents{
			Key:          k,
			Size:         int64(len(obj.data)),
			LastModified: obj.mod.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + etagOf(obj.data) + `"`,
			StorageClass: "STANDARD",
		})
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, xml.Header)
	xml.NewEncoder(w).Encode(res)
}

// deleteMulti 实现 S3 的批量删除（POST /bucket?delete）。
func (f *fakeS3) deleteMulti(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	for _, obj := range req.Objects {
		delete(f.objects, obj.Key)
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, xml.Header+"<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
	for _, obj := range req.Objects {
		io.WriteString(w, "<Deleted><Key>"+obj.Key+"</Key></Deleted>")
	}
	io.WriteString(w, "</DeleteResult>")
}

// ---------- 测试辅助 ----------

func newTestS3(t *testing.T, srv *httptest.Server) *S3Store {
	t.Helper()
	useSSL, pathStyle := false, true
	st, err := NewS3(S3Config{
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
		Bucket:    "docker-mirror-gateway",
		AccessKey: "test",
		SecretKey: "test",
		Prefix:    "docker-mirror-gateway",
		UseSSL:    useSSL,
		PathStyle: pathStyle,
		MaxBytes:  64 << 20,
		TagTTL:    time.Minute,
		TmpDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("创建 S3 缓存失败: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func mustDigest(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("超时等待: %s", msg)
}

// ---------- 用例 ----------

func TestS3ManifestRoundTrip(t *testing.T) {
	_, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)

	body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`)
	m := &Manifest{
		Body:      body,
		Digest:    mustDigest(t, body),
		MediaType: "application/vnd.docker.distribution.manifest.v2+json",
		FetchedAt: time.Now(),
	}
	const key = "dockerhub/library/alpine:latest"
	if err := st.PutManifest(key, m); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	got, ok := st.GetManifest(key)
	if !ok {
		t.Fatal("应当命中缓存")
	}
	if !bytes.Equal(got.Body, body) {
		t.Errorf("正文不一致: %q", got.Body)
	}
	if got.Digest != m.Digest || got.MediaType != m.MediaType {
		t.Errorf("元数据不一致: %+v", got)
	}
}

func TestS3ManifestTTL(t *testing.T) {
	_, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)

	body := []byte(`{"schemaVersion":2}`)
	stale := &Manifest{
		Body:      body,
		Digest:    mustDigest(t, body),
		Mutable:   true,
		FetchedAt: time.Now().Add(-10 * time.Minute), // TagTTL 是 1 分钟
	}
	if err := st.PutManifest("hub/nginx:latest", stale); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.GetManifest("hub/nginx:latest"); ok {
		t.Error("过期的 tag 缓存不该命中")
	}

	fresh := &Manifest{Body: body, Digest: mustDigest(t, body), Mutable: true, FetchedAt: time.Now()}
	if err := st.PutManifest("hub/nginx:stable", fresh); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.GetManifest("hub/nginx:stable"); !ok {
		t.Error("未过期的 tag 缓存应当命中")
	}
}

func TestS3BlobWriteAndRead(t *testing.T) {
	fake, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)

	data := bytes.Repeat([]byte("mirror"), 20<<10)
	digest := mustDigest(t, data)

	w, err := st.NewBlobWriter(digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data[:1000]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data[1000:]); err != nil {
		t.Fatal(err)
	}
	if w.Written() != int64(len(data)) {
		t.Errorf("Written = %d", w.Written())
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}

	// 上传是异步的。
	waitFor(t, 3*time.Second, func() bool {
		size, ok := st.HasBlob(digest)
		return ok && size == int64(len(data))
	}, "blob 异步上传完成")

	rc, size, err := st.OpenBlob(digest)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(data)) || !bytes.Equal(got, data) {
		t.Errorf("内容不一致: size=%d len=%d", size, len(got))
	}

	// 对象键应落在 prefix 下并按 digest 分目录。
	found := false
	for _, k := range fake.keys() {
		if strings.HasPrefix(k, "docker-mirror-gateway/blobs/sha256/") {
			found = true
		}
	}
	if !found {
		t.Errorf("对象键不符合预期: %v", fake.keys())
	}
}

func TestS3BlobDigestMismatchDiscards(t *testing.T) {
	fake, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)

	wrong := "sha256:" + strings.Repeat("0", 64)
	w, err := st.NewBlobWriter(wrong)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("不匹配的内容")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err == nil {
		t.Fatal("digest 不匹配时 Commit 应当报错")
	}
	time.Sleep(100 * time.Millisecond)
	if len(fake.keys()) != 0 {
		t.Errorf("校验失败不该留下对象: %v", fake.keys())
	}
}

func TestS3PurgeOnlyTouchesPrefix(t *testing.T) {
	fake, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)

	// 同 bucket 里别人的数据，必须保留。
	fake.put("other/important.bin", []byte("keep me"), time.Now())
	fake.put("docker-mirror-gateway/blobs/sha256/aa/aabb", []byte("cache"), time.Now())

	if err := st.Purge(); err != nil {
		t.Fatalf("Purge 失败: %v", err)
	}
	keys := fake.keys()
	if len(keys) != 1 || keys[0] != "other/important.bin" {
		t.Errorf("Purge 只应删除自己前缀下的对象，剩余: %v", keys)
	}
}

func TestS3EnforceLimitEvicts(t *testing.T) {
	fake, srv := newFakeS3(t, "docker-mirror-gateway")
	st := newTestS3(t, srv)
	st.maxBytes = 2 << 10 // 2KB

	// 三个 1KB 对象，访问时间人为拉开。
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("docker-mirror-gateway/blobs/sha256/0%d/obj%d", i, i)
		old := time.Now().Add(-time.Duration(10-i) * time.Hour)
		fake.put(key, bytes.Repeat([]byte("x"), 1<<10), old)
		st.lastUse.Store(key, old)
	}

	res, err := st.EnforceLimit()
	if err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	if res.Removed == 0 || res.TotalBytes > int64(2<<10) {
		t.Errorf("应淘汰到上限内: %+v", res)
	}
	if len(fake.keys()) == 0 {
		t.Error("不该把对象删光")
	}
}

func TestS3RejectsBadEndpoint(t *testing.T) {
	if _, err := NewS3(S3Config{Bucket: "b", Endpoint: ""}); err == nil {
		t.Error("空 endpoint 应报错")
	}
	if _, err := NewS3(S3Config{Bucket: "b", Endpoint: "https://s3.example.com/path"}); err == nil {
		t.Error("带路径的 endpoint 应报错")
	}
	if _, err := NewS3(S3Config{Endpoint: "s3.example.com"}); err == nil {
		t.Error("空 bucket 应报错")
	}
}
