package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 后端的对象布局：
//
//	<prefix>/blobs/<alg>/<hex[:2]>/<hex>         blob 原始字节
//	<prefix>/manifests/<hash[:2]>/<hash>         [8字节 meta 长度][meta JSON][manifest 原始字节]
//
// manifest 的元数据和正文塞进同一个对象，一次 GET 就能取全，
// 避免每次读 manifest 都多花一次往返。
const (
	s3MetaHeaderLen   = 8
	s3MaxMetaBytes    = 1 << 20
	s3UploadWorkers   = 4
	s3DefaultPrefix   = "docker-mirror-gateway"
	s3ListTimeout     = 5 * time.Minute
	s3UploadTimeout   = 30 * time.Minute
	s3TempFileMaxAge  = 6 * time.Hour
	s3StatRefreshTTL  = 10 * time.Minute
	s3MetaContentType = "application/octet-stream"
)

// S3Config 是 S3 兼容对象存储的配置。
type S3Config struct {
	Endpoint       string
	Bucket         string
	AccessKey      string
	SecretKey      string
	Region         string
	Prefix         string
	UseSSL         bool
	PathStyle      bool
	MaxBytes       int64
	TagTTL         time.Duration
	ManifestMaxAge time.Duration
	TmpDir         string
	Logger         *slog.Logger
}

// S3Store 把缓存放在 S3 兼容的对象存储里。
type S3Store struct {
	cli      *minio.Client
	bucket   string
	prefix   string
	endpoint string
	maxBytes int64
	tagTTL   time.Duration
	maxAge   time.Duration
	tmpDir   string
	log      *slog.Logger

	size    atomic.Int64
	objects atomic.Int64
	// lastUse 记录对象最近一次被读取的时间。S3 无法修改对象时间，
	// 淘汰时优先用它，缺失时退回 LastModified。
	lastUse sync.Map

	uploadSem chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// enqMu/stopping 保证 Close 的 wg.Wait 不会与新上传的 wg.Add 竞争。
	enqMu    sync.Mutex
	stopping bool
}

// NewS3 创建 S3 缓存后端，并确保 bucket 可用。
func NewS3(cfg S3Config) (*S3Store, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("cache.s3.bucket 不能为空")
	}
	endpoint := strings.TrimSpace(cfg.Endpoint)
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimSuffix(endpoint, "/")
	if endpoint == "" {
		return nil, errors.New("cache.s3.endpoint 不能为空")
	}
	if strings.Contains(endpoint, "/") {
		return nil, fmt.Errorf("cache.s3.endpoint 只填主机名和端口，不要带路径: %q", cfg.Endpoint)
	}

	lookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		lookup = minio.BucketLookupPath
	}
	cli, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.UseSSL,
		Region:       cfg.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化 S3 客户端失败: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// prefix 必须有值：否则清理任务会把整个 bucket 当成自己的地盘。
	prefix := strings.Trim(strings.TrimSpace(cfg.Prefix), "/")
	if prefix == "" {
		prefix = s3DefaultPrefix
	}
	prefix += "/"

	tmpDir := cfg.TmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &S3Store{
		cli:       cli,
		bucket:    cfg.Bucket,
		prefix:    prefix,
		endpoint:  endpoint,
		maxBytes:  cfg.MaxBytes,
		tagTTL:    cfg.TagTTL,
		maxAge:    cfg.ManifestMaxAge,
		tmpDir:    tmpDir,
		log:       logger,
		uploadSem: make(chan struct{}, s3UploadWorkers),
		ctx:       ctx,
		cancel:    cancel,
	}

	startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startCancel()
	exists, err := cli.BucketExists(startCtx, cfg.Bucket)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("检查 bucket %q 失败: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := cli.MakeBucket(startCtx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			cancel()
			return nil, fmt.Errorf("创建 bucket %q 失败: %w", cfg.Bucket, err)
		}
		logger.Info("已创建 S3 bucket", "bucket", cfg.Bucket)
	}

	s.cleanStaleTempFiles()
	s.wg.Add(1)
	go s.refreshStatsLoop()
	return s, nil
}

func (s *S3Store) Backend() string { return "s3" }

func (s *S3Store) Dir() string { return fmt.Sprintf("s3://%s/%s", s.bucket, s.prefix) }

func (s *S3Store) MaxBytes() int64 { return s.maxBytes }

func (s *S3Store) Size() int64 { return s.size.Load() }

func (s *S3Store) Objects() int64 { return s.objects.Load() }

func (s *S3Store) TagTTL() time.Duration { return s.tagTTL }

// Close 停止后台任务并等待在途上传收尾（最多 10 秒）。
func (s *S3Store) Close() error {
	s.enqMu.Lock()
	s.stopping = true
	s.enqMu.Unlock()
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.log.Warn("退出时仍有 S3 上传任务未完成，已放弃等待")
	}
	return nil
}

func (s *S3Store) join(parts ...string) string {
	return s.prefix + path.Join(parts...)
}

func (s *S3Store) blobKey(digest string) (string, error) {
	alg, h, err := parseDigest(digest)
	if err != nil {
		return "", err
	}
	return s.join("blobs", alg, h[:2], h), nil
}

func (s *S3Store) manifestKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	return s.join("manifests", h[:2], h)
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == 404
}

// ---------- manifest ----------

// GetManifest 读取 manifest 缓存，未命中或已过期返回 false。
func (s *S3Store) GetManifest(key string) (*Manifest, bool) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	obj, err := s.cli.GetObject(ctx, s.bucket, s.manifestKey(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, false
	}
	defer obj.Close()

	body, err := io.ReadAll(io.LimitReader(obj, 32<<20))
	if err != nil {
		return nil, false
	}
	m, err := decodeManifestObject(body)
	if err != nil {
		return nil, false
	}
	if m.Mutable && s.tagTTL > 0 && time.Since(m.FetchedAt) > s.tagTTL {
		return nil, false
	}
	m.Key = key
	return m, true
}

// decodeManifestObject 拆出 [长度][meta][body] 结构。
func decodeManifestObject(raw []byte) (*Manifest, error) {
	if len(raw) < s3MetaHeaderLen {
		return nil, errors.New("manifest 对象过短")
	}
	metaLen := int(binary.BigEndian.Uint64(raw[:s3MetaHeaderLen]))
	if metaLen <= 0 || metaLen > s3MaxMetaBytes || s3MetaHeaderLen+metaLen > len(raw) {
		return nil, errors.New("manifest 元数据长度非法")
	}
	var m Manifest
	if err := json.Unmarshal(raw[s3MetaHeaderLen:s3MetaHeaderLen+metaLen], &m); err != nil {
		return nil, err
	}
	body := raw[s3MetaHeaderLen+metaLen:]
	if int64(len(body)) != m.Size {
		return nil, errors.New("manifest 长度不匹配")
	}
	if m.Digest != "" {
		sum := sha256.Sum256(body)
		if "sha256:"+hex.EncodeToString(sum[:]) != m.Digest {
			return nil, errors.New("manifest 摘要不匹配")
		}
	}
	m.Body = body
	return &m, nil
}

func encodeManifestObject(m *Manifest) ([]byte, error) {
	meta, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if len(meta) > s3MaxMetaBytes {
		return nil, errors.New("manifest 元数据过大")
	}
	buf := make([]byte, s3MetaHeaderLen, s3MetaHeaderLen+len(meta)+len(m.Body))
	binary.BigEndian.PutUint64(buf[:s3MetaHeaderLen], uint64(len(meta)))
	buf = append(buf, meta...)
	buf = append(buf, m.Body...)
	return buf, nil
}

// PutManifest 写入 manifest 缓存。
func (s *S3Store) PutManifest(key string, m *Manifest) error {
	if m == nil {
		return errors.New("manifest 为空")
	}
	m.Key = key
	m.Size = int64(len(m.Body))
	blob, err := encodeManifestObject(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	_, err = s.cli.PutObject(ctx, s.bucket, s.manifestKey(key), bytes.NewReader(blob), int64(len(blob)),
		minio.PutObjectOptions{ContentType: s3MetaContentType})
	if err != nil {
		return fmt.Errorf("写入 manifest 到 S3 失败: %w", err)
	}
	return nil
}

// ---------- blob ----------

// HasBlob 判断 blob 是否已缓存，返回其大小。
func (s *S3Store) HasBlob(digest string) (int64, bool) {
	key, err := s.blobKey(digest)
	if err != nil {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	info, err := s.cli.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil || info.Size <= 0 {
		return 0, false
	}
	return info.Size, true
}

// OpenBlob 打开缓存的 blob。minio 的 Object 自带 Seek，Range 请求可用。
func (s *S3Store) OpenBlob(digest string) (io.ReadSeekCloser, int64, error) {
	key, err := s.blobKey(digest)
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	info, err := s.cli.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	obj, err := s.cli.GetObject(s.ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	return obj, info.Size, nil
}

// TouchBlob 记录访问时间用于淘汰排序。
func (s *S3Store) TouchBlob(digest string) {
	if key, err := s.blobKey(digest); err == nil {
		s.lastUse.Store(key, time.Now())
	}
}

// s3Writer 先把数据写到本地临时文件，校验通过后再异步上传。
// 这样 S3 的上传速度不会拖慢客户端，摘要不符也不会污染对象存储。
type s3Writer struct {
	s       *S3Store
	digest  string
	alg     string
	tmpPath string
	file    *os.File
	hasher  hash.Hash
	written int64
	closed  bool
}

// NewBlobWriter 创建 blob 写入器。
func (s *S3Store) NewBlobWriter(digest string) (Writer, error) {
	alg, _, err := parseDigest(digest)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(s.tmpDir, "docker-mirror-gateway-s3-*.part")
	if err != nil {
		return nil, fmt.Errorf("创建上传临时文件失败: %w", err)
	}
	var hasher hash.Hash
	if alg == "sha512" {
		hasher = sha512.New()
	} else {
		hasher = sha256.New()
	}
	return &s3Writer{
		s:       s,
		digest:  digest,
		alg:     alg,
		tmpPath: f.Name(),
		file:    f,
		hasher:  hasher,
	}, nil
}

func (w *s3Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("blob writer 已关闭")
	}
	n, err := w.file.Write(p)
	if n > 0 {
		w.hasher.Write(p[:n])
		w.written += int64(n)
	}
	return n, err
}

func (w *s3Writer) Written() int64 { return w.written }

// Commit 校验摘要，通过后把上传交给后台，立即返回。
func (w *s3Writer) Commit() error {
	if w.closed {
		return errors.New("blob writer 已关闭")
	}
	w.closed = true
	actual := w.alg + ":" + hex.EncodeToString(w.hasher.Sum(nil))
	if actual != w.digest {
		w.file.Close()
		os.Remove(w.tmpPath)
		return fmt.Errorf("digest 校验失败: 期望 %s，实际 %s", w.digest, actual)
	}
	if err := w.file.Close(); err != nil {
		os.Remove(w.tmpPath)
		return err
	}
	size := w.written
	w.s.enqueueUpload(w.tmpPath, w.digest, size)
	return nil
}

// Abort 放弃写入并清理临时文件。
func (w *s3Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.file.Close()
	_ = os.Remove(w.tmpPath)
}

// enqueueUpload 在后台完成上传，最多 4 个并发。
// 与 Close 互斥：WaitGroup 的 Add 不允许在 Wait 已开始后进行，
// 关停后到达的提交直接丢弃临时文件。
func (s *S3Store) enqueueUpload(tmpPath, digest string, size int64) {
	s.enqMu.Lock()
	if s.stopping {
		s.enqMu.Unlock()
		os.Remove(tmpPath)
		return
	}
	s.wg.Add(1)
	s.enqMu.Unlock()
	go func() {
		defer s.wg.Done()
		select {
		case s.uploadSem <- struct{}{}:
			defer func() { <-s.uploadSem }()
		case <-s.ctx.Done():
			os.Remove(tmpPath)
			return
		}
		key, err := s.blobKey(digest)
		if err != nil {
			os.Remove(tmpPath)
			return
		}
		f, err := os.Open(tmpPath)
		if err != nil {
			os.Remove(tmpPath)
			s.log.Warn("打开待上传的 blob 失败", "digest", digest, "error", err)
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, s3UploadTimeout)
		defer cancel()
		_, err = s.cli.PutObject(ctx, s.bucket, key, f, size, minio.PutObjectOptions{ContentType: s3MetaContentType})
		f.Close()
		if err != nil {
			// 上传失败就丢弃临时文件：留在磁盘上要等 30 天后才会被
			// cleanStaleTempFiles 清掉，期间每次失败都白占一块 blob 大小的空间。
			os.Remove(tmpPath)
			s.log.Warn("上传 blob 到 S3 失败", "digest", digest, "sizeMB", float64(size)/(1<<20), "error", err)
			return
		}
		os.Remove(tmpPath)
		s.size.Add(size)
		s.objects.Add(1)
		s.log.Debug("blob 已上传到 S3", "digest", digest, "sizeMB", float64(size)/(1<<20))
	}()
}

// ---------- 统计与回收 ----------

// refreshStatsLoop 启动时和之后每 10 分钟校准一次总量。
func (s *S3Store) refreshStatsLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s3StatRefreshTTL)
	defer ticker.Stop()
	for {
		s.recount()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// recount 遍历 bucket 统计真实占用。
func (s *S3Store) recount() (int64, int64) {
	ctx, cancel := context.WithTimeout(s.ctx, s3ListTimeout)
	defer cancel()
	var total, objects int64
	for obj := range s.cli.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
		if obj.Err != nil {
			s.log.Debug("列举 S3 对象失败", "error", obj.Err)
			return s.size.Load(), s.objects.Load()
		}
		total += obj.Size
		objects++
	}
	s.size.Store(total)
	s.objects.Store(objects)
	return total, objects
}

type s3Item struct {
	key  string
	size int64
	used time.Time
}

// EnforceLimit 清理过期 manifest，并在超出容量时按最久未使用淘汰。
func (s *S3Store) EnforceLimit() (GCResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s3ListTimeout)
	defer cancel()

	var (
		items   []s3Item
		total   int64
		objects int64
		now     = time.Now()
		removed int
		freed   int64
	)
	for obj := range s.cli.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
		if obj.Err != nil {
			return GCResult{}, obj.Err
		}
		// manifestMaxAge 只作用于 manifest：blob 按 digest 内容寻址、永不变化，
		// 删了只会导致下次重复回源；与本地磁盘后端的行为保持一致。
		if s.maxAge > 0 && strings.HasPrefix(obj.Key, s.prefix+"manifests") &&
			now.Sub(obj.LastModified) > s.maxAge {
			if err := s.cli.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}); err == nil {
				removed++
				freed += obj.Size
				s.lastUse.Delete(obj.Key)
				continue
			}
		}
		used := obj.LastModified
		if v, ok := s.lastUse.Load(obj.Key); ok {
			if t, ok := v.(time.Time); ok && t.After(used) {
				used = t
			}
		}
		items = append(items, s3Item{key: obj.Key, size: obj.Size, used: used})
		total += obj.Size
		objects++
	}

	if s.maxBytes > 0 && total > s.maxBytes {
		sort.Slice(items, func(i, j int) bool { return items[i].used.Before(items[j].used) })
		target := int64(float64(s.maxBytes) * 0.85)
		for _, it := range items {
			if total <= target {
				break
			}
			// 刚写入的对象可能正在被读取，跳过。
			if now.Sub(it.used) < 2*time.Minute {
				continue
			}
			if err := s.cli.RemoveObject(ctx, s.bucket, it.key, minio.RemoveObjectOptions{}); err == nil {
				total -= it.size
				objects--
				removed++
				freed += it.size
				s.lastUse.Delete(it.key)
			}
		}
	}

	s.size.Store(total)
	s.objects.Store(objects)
	res := GCResult{Removed: removed, FreedBytes: freed, TotalBytes: total, TotalObjects: int(objects)}
	if removed > 0 {
		s.log.Info("S3 缓存清理完成", "removed", removed, "freedMB", float64(freed)/(1<<20), "totalMB", float64(total)/(1<<20))
	}
	return res, nil
}

// Purge 清空本前缀下的全部对象。
func (s *S3Store) Purge() error {
	ctx, cancel := context.WithTimeout(s.ctx, s3ListTimeout)
	defer cancel()
	objects := s.cli.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true})
	for err := range s.cli.RemoveObjects(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			return fmt.Errorf("删除 S3 对象失败: %w", err.Err)
		}
	}
	// 逐条清空访问时间表，避免旧条目无限累积；整体重赋值会与并发的
	// TouchBlob/Load 产生数据竞争。
	s.lastUse.Range(func(key, _ any) bool {
		s.lastUse.Delete(key)
		return true
	})
	s.size.Store(0)
	s.objects.Store(0)
	return nil
}

// cleanStaleTempFiles 清理上次进程异常退出留下的临时文件。
func (s *S3Store) cleanStaleTempFiles() {
	entries, err := os.ReadDir(s.tmpDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-s3TempFileMaxAge)
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "docker-mirror-gateway-s3-") {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(s.tmpDir, e.Name()))
	}
}
