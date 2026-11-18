// Package cache 实现基于本地文件系统的镜像内容缓存。
//
// 目录结构：
//
//	<dir>/blobs/<alg>/<hex[:2]>/<hex>         blob 内容（按 digest 内容寻址）
//	<dir>/manifests/<hash[:2]>/<hash>.bin     manifest 原始字节
//	<dir>/manifests/<hash[:2]>/<hash>.json    manifest 元数据
package cache

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidDigest 表示 digest 格式非法。
var ErrInvalidDigest = errors.New("非法的 digest 格式")

// ErrDisabled 表示缓存未启用。
var ErrDisabled = errors.New("缓存未启用")

// ErrNotFound 表示缓存中没有该对象。
var ErrNotFound = errors.New("缓存未命中")

// Config 是缓存配置。
type Config struct {
	Dir            string
	MaxBytes       int64
	TagTTL         time.Duration
	ManifestMaxAge time.Duration
	Logger         *slog.Logger
}

// Manifest 是一条 manifest 缓存记录。
type Manifest struct {
	Body      []byte    `json:"-"`
	Key       string    `json:"key"`
	Digest    string    `json:"digest"`
	MediaType string    `json:"mediaType"`
	Size      int64     `json:"size"`
	Mutable   bool      `json:"mutable"` // tag 指向的内容会变化，需要 TTL
	FetchedAt time.Time `json:"fetchedAt"`
	Upstream  string    `json:"upstream"`
}

// GCResult 描述一次清理的结果。
type GCResult struct {
	Removed      int   `json:"removed"`
	FreedBytes   int64 `json:"freedBytes"`
	TotalBytes   int64 `json:"totalBytes"`
	TotalObjects int   `json:"totalObjects"`
}

// Cache 是线程安全的本地缓存。
type Cache struct {
	dir            string
	blobDir        string
	manifestDir    string
	maxBytes       int64
	tagTTL         time.Duration
	manifestMaxAge time.Duration
	log            *slog.Logger

	size    atomic.Int64
	objects atomic.Int64

	touchMu  sync.Mutex
	lastSeen map[string]time.Time
}

// New 创建缓存目录并统计当前占用。
func New(cfg Config) (*Cache, error) {
	if cfg.Dir == "" {
		return nil, errors.New("缓存目录不能为空")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	c := &Cache{
		dir:            cfg.Dir,
		blobDir:        filepath.Join(cfg.Dir, "blobs"),
		manifestDir:    filepath.Join(cfg.Dir, "manifests"),
		maxBytes:       cfg.MaxBytes,
		tagTTL:         cfg.TagTTL,
		manifestMaxAge: cfg.ManifestMaxAge,
		log:            logger,
		lastSeen:       make(map[string]time.Time),
	}
	for _, dir := range []string{c.blobDir, c.manifestDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("创建缓存目录 %s 失败: %w", dir, err)
		}
	}
	if _, err := c.scan(); err != nil {
		return nil, err
	}
	return c, nil
}

// Backend 返回后端名称。
func (c *Cache) Backend() string { return "disk" }

// Close 对本地缓存无需清理。
func (c *Cache) Close() error { return nil }

// Dir 返回缓存根目录。
func (c *Cache) Dir() string { return c.dir }

// MaxBytes 返回配置的容量上限。
func (c *Cache) MaxBytes() int64 { return c.maxBytes }

// Size 返回当前占用字节数。
func (c *Cache) Size() int64 { return c.size.Load() }

// Objects 返回当前缓存文件数量。
func (c *Cache) Objects() int64 { return c.objects.Load() }

// TagTTL 返回 tag 缓存有效期。
func (c *Cache) TagTTL() time.Duration { return c.tagTTL }

func parseDigest(digest string) (alg, hexPart string, err error) {
	alg, hexPart, ok := strings.Cut(digest, ":")
	if !ok || alg == "" || hexPart == "" {
		return "", "", ErrInvalidDigest
	}
	if alg != "sha256" && alg != "sha512" {
		return "", "", fmt.Errorf("%w: 不支持的算法 %q", ErrInvalidDigest, alg)
	}
	for _, r := range hexPart {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f':
		default:
			return "", "", ErrInvalidDigest
		}
	}
	if alg == "sha256" && len(hexPart) != 64 {
		return "", "", fmt.Errorf("%w: sha256 长度应为 64", ErrInvalidDigest)
	}
	if alg == "sha512" && len(hexPart) != 128 {
		return "", "", fmt.Errorf("%w: sha512 长度应为 128", ErrInvalidDigest)
	}
	return alg, hexPart, nil
}

func (c *Cache) blobPath(digest string) (string, error) {
	alg, h, err := parseDigest(digest)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.blobDir, alg, h[:2], h), nil
}

func (c *Cache) manifestBase(key string) string {
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	return filepath.Join(c.manifestDir, h[:2], h)
}

// ---------- manifest ----------

// GetManifest 读取 manifest 缓存，未命中或已过期返回 false。
func (c *Cache) GetManifest(key string) (*Manifest, bool) {
	base := c.manifestBase(key)
	metaRaw, err := os.ReadFile(base + ".json")
	if err != nil {
		return nil, false
	}
	var m Manifest
	if err := json.Unmarshal(metaRaw, &m); err != nil {
		return nil, false
	}
	if m.Mutable && c.tagTTL > 0 && time.Since(m.FetchedAt) > c.tagTTL {
		return nil, false
	}
	body, err := os.ReadFile(base + ".bin")
	if err != nil {
		return nil, false
	}
	if int64(len(body)) != m.Size {
		return nil, false
	}
	if m.Digest != "" {
		sum := sha256.Sum256(body)
		if "sha256:"+hex.EncodeToString(sum[:]) != m.Digest {
			return nil, false
		}
	}
	m.Body = body
	m.Key = key
	return &m, true
}

// PutManifest 写入 manifest 缓存。
func (c *Cache) PutManifest(key string, m *Manifest) error {
	if m == nil {
		return errors.New("manifest 为空")
	}
	base := c.manifestBase(key)
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		return err
	}

	// 统计旧文件占用，用于增量更新总大小。
	var previous int64
	if st, err := os.Stat(base + ".bin"); err == nil {
		previous += st.Size()
	}
	if st, err := os.Stat(base + ".json"); err == nil {
		previous += st.Size()
	}

	if err := writeFileAtomic(base+".bin", m.Body, 0o644); err != nil {
		return err
	}
	m.Key = key
	m.Size = int64(len(m.Body))
	meta, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(base+".json", meta, 0o644); err != nil {
		return err
	}
	c.size.Add(int64(len(m.Body)) + int64(len(meta)) - previous)
	if previous == 0 {
		c.objects.Add(1)
	}
	return nil
}

// writeFileAtomic 先写唯一命名的临时文件再 rename，保证读者永远看到完整内容。
// 临时文件名带纳秒时间戳：并发写同一个 key 时各自持有独立临时文件，
// 不会出现互相 O_TRUNC 截断对方写到一半的内容。
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := fmt.Sprintf("%s.tmp-%d", path, time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// ---------- blob ----------

// HasBlob 判断 blob 是否已缓存，返回其大小。
func (c *Cache) HasBlob(digest string) (int64, bool) {
	path, err := c.blobPath(digest)
	if err != nil {
		return 0, false
	}
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return 0, false
	}
	return st.Size(), true
}

// OpenBlob 打开缓存的 blob。
func (c *Cache) OpenBlob(digest string) (io.ReadSeekCloser, int64, error) {
	path, err := c.blobPath(digest)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// TouchBlob 刷新 blob 的访问时间，用于近似 LRU 淘汰。
// 锁内只更新内存表，os.Chtimes 放到锁外，避免文件系统 IO 拖住并发读。
func (c *Cache) TouchBlob(digest string) {
	now := time.Now()
	c.touchMu.Lock()
	if last, ok := c.lastSeen[digest]; ok && now.Sub(last) < time.Minute {
		c.touchMu.Unlock()
		return
	}
	if len(c.lastSeen) > 10000 {
		c.lastSeen = make(map[string]time.Time, 128)
	}
	c.lastSeen[digest] = now
	c.touchMu.Unlock()

	if path, err := c.blobPath(digest); err == nil {
		_ = os.Chtimes(path, now, now)
	}
}

// BlobWriter 在下载过程中把数据写入临时文件，完成后校验 digest 并原子落盘。
type BlobWriter struct {
	c         *Cache
	digest    string
	alg       string
	tmpPath   string
	finalPath string
	file      *os.File
	hasher    hash.Hash
	written   int64
	closed    bool
}

// NewBlobWriter 创建 blob 写入器。
func (c *Cache) NewBlobWriter(digest string) (Writer, error) {
	alg, _, err := parseDigest(digest)
	if err != nil {
		return nil, err
	}
	finalPath, err := c.blobPath(digest)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return nil, err
	}
	tmp := fmt.Sprintf("%s.tmp-%d", finalPath, time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	var hasher hash.Hash
	if alg == "sha512" {
		hasher = sha512.New()
	} else {
		hasher = sha256.New()
	}
	return &BlobWriter{
		c:         c,
		digest:    digest,
		alg:       alg,
		tmpPath:   tmp,
		finalPath: finalPath,
		file:      f,
		hasher:    hasher,
	}, nil
}

// Write 实现 io.Writer。
func (w *BlobWriter) Write(p []byte) (int, error) {
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

// Written 返回已写入字节数。
func (w *BlobWriter) Written() int64 { return w.written }

// Commit 校验 digest 并把临时文件重命名为最终文件。
func (w *BlobWriter) Commit() error {
	if w.closed {
		return errors.New("blob writer 已关闭")
	}
	w.closed = true
	sum := hex.EncodeToString(w.hasher.Sum(nil))
	actual := w.alg + ":" + sum
	if actual != w.digest {
		w.file.Close()
		os.Remove(w.tmpPath)
		return fmt.Errorf("digest 校验失败: 期望 %s，实际 %s", w.digest, actual)
	}
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		os.Remove(w.tmpPath)
		return err
	}
	if err := w.file.Close(); err != nil {
		os.Remove(w.tmpPath)
		return err
	}
	if _, err := os.Stat(w.finalPath); err == nil {
		// 已有相同内容（并发下载），直接丢弃临时文件。
		os.Remove(w.tmpPath)
		return nil
	}
	if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
		os.Remove(w.tmpPath)
		return err
	}
	w.c.size.Add(w.written)
	w.c.objects.Add(1)
	return nil
}

// Abort 放弃写入并清理临时文件。
func (w *BlobWriter) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.file.Close()
	_ = os.Remove(w.tmpPath)
}

// ---------- 统计与回收 ----------

func (c *Cache) scan() (GCResult, error) {
	var res GCResult
	err := filepath.WalkDir(c.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // 忽略无权限等问题
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		res.TotalBytes += info.Size()
		if countsAsObject(path) {
			res.TotalObjects++
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	c.size.Store(res.TotalBytes)
	c.objects.Store(int64(res.TotalObjects))
	return res, nil
}

// countsAsObject 判断文件是否算一个缓存对象（manifest 的 .json 元数据不计）。
func countsAsObject(path string) bool {
	return !strings.HasSuffix(path, ".json")
}

type cacheFile struct {
	path       string
	size       int64
	modTime    time.Time
	isManifest bool
}

// EnforceLimit 清理过期 manifest、残留临时文件，并在超过容量上限时按最久未访问淘汰。
func (c *Cache) EnforceLimit() (GCResult, error) {
	var (
		files      []cacheFile
		total      int64
		objects    int64
		removed    int
		freedBytes int64
		now        = time.Now()
	)

	walkErr := filepath.WalkDir(c.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if strings.Contains(filepath.Base(path), ".tmp") {
			if now.Sub(info.ModTime()) > time.Hour {
				if os.Remove(path) == nil {
					removed++
					freedBytes += info.Size()
				}
			}
			return nil
		}
		isManifest := strings.HasPrefix(path, c.manifestDir+string(os.PathSeparator))
		if isManifest && c.manifestMaxAge > 0 && now.Sub(info.ModTime()) > c.manifestMaxAge {
			if os.Remove(path) == nil {
				removed++
				freedBytes += info.Size()
				return nil
			}
		}
		files = append(files, cacheFile{path: path, size: info.Size(), modTime: info.ModTime(), isManifest: isManifest})
		total += info.Size()
		if countsAsObject(path) {
			objects++
		}
		return nil
	})
	if walkErr != nil {
		return GCResult{}, walkErr
	}

	if c.maxBytes > 0 && total > c.maxBytes {
		sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })
		target := int64(float64(c.maxBytes) * 0.85)
		for _, f := range files {
			if total <= target {
				break
			}
			// 避免删除刚刚写入、可能仍在使用的文件。
			if now.Sub(f.modTime) < 2*time.Minute {
				continue
			}
			if err := os.Remove(f.path); err == nil {
				// manifest 的 .bin 与 .json 成对删除，避免留下指向已删内容的孤儿元数据。
				if strings.HasPrefix(f.path, c.manifestDir+string(os.PathSeparator)) {
					if strings.HasSuffix(f.path, ".bin") {
						_ = os.Remove(strings.TrimSuffix(f.path, ".bin") + ".json")
					} else if strings.HasSuffix(f.path, ".json") {
						_ = os.Remove(strings.TrimSuffix(f.path, ".json") + ".bin")
					}
				}
				total -= f.size
				if countsAsObject(f.path) {
					objects--
				}
				removed++
				freedBytes += f.size
			}
		}
	}

	c.size.Store(total)
	c.objects.Store(objects)
	res := GCResult{Removed: removed, FreedBytes: freedBytes, TotalBytes: total, TotalObjects: int(objects)}
	if removed > 0 {
		c.log.Info("缓存清理完成", "removed", removed, "freedMB", float64(freedBytes)/(1<<20), "totalMB", float64(total)/(1<<20))
	}
	return res, nil
}

// Purge 清空整个缓存目录。
func (c *Cache) Purge() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(c.dir, e.Name())); err != nil {
			return err
		}
	}
	for _, dir := range []string{c.blobDir, c.manifestDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	c.size.Store(0)
	c.objects.Store(0)
	return nil
}
