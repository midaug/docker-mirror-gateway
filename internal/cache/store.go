package cache

import (
	"io"
	"time"
)

// Store 是缓存后端的统一接口。目前有两种实现：
//   - Cache：本地文件系统（backend = "disk"）
//   - S3Store：任意 S3 兼容对象存储（backend = "s3"）
//
// 关闭缓存时使用 Noop，调用方无需到处判空。
type Store interface {
	// Backend 返回后端名称：disk / s3 / none。
	Backend() string
	// Dir 返回本地目录；对象存储后端返回描述性地址。
	Dir() string
	// MaxBytes 返回容量上限，0 表示不限制。
	MaxBytes() int64
	// Size 返回当前占用字节数。
	Size() int64
	// Objects 返回当前缓存对象数量。
	Objects() int64
	// TagTTL 返回按 tag 缓存的 manifest 有效期。
	TagTTL() time.Duration

	GetManifest(key string) (*Manifest, bool)
	PutManifest(key string, m *Manifest) error

	HasBlob(digest string) (int64, bool)
	OpenBlob(digest string) (io.ReadSeekCloser, int64, error)
	TouchBlob(digest string)
	NewBlobWriter(digest string) (Writer, error)

	// EnforceLimit 清理过期内容并在超出容量时淘汰。
	EnforceLimit() (GCResult, error)
	// Purge 清空全部缓存。
	Purge() error
	// Close 释放后端资源（S3 需要停止后台任务）。
	Close() error
}

// Writer 是 blob 的写入器：边写边算摘要，Commit 时校验后才真正入库。
type Writer interface {
	io.Writer
	// Written 返回已写入字节数。
	Written() int64
	// Commit 校验 digest 并提交；校验失败返回错误且不留残渣。
	Commit() error
	// Abort 放弃写入并清理中间状态。
	Abort()
}

// ---------- 关闭缓存时的空实现 ----------

// Noop 是一个不存任何东西的 Store，用于 cache.enabled=false。
type Noop struct{}

// NewNoop 返回空缓存实现。
func NewNoop() Noop { return Noop{} }

func (Noop) Backend() string                      { return "none" }
func (Noop) Dir() string                          { return "" }
func (Noop) MaxBytes() int64                      { return 0 }
func (Noop) Size() int64                          { return 0 }
func (Noop) Objects() int64                       { return 0 }
func (Noop) TagTTL() time.Duration                { return 0 }
func (Noop) GetManifest(string) (*Manifest, bool) { return nil, false }
func (Noop) PutManifest(string, *Manifest) error  { return nil }
func (Noop) HasBlob(string) (int64, bool)         { return 0, false }
func (Noop) OpenBlob(string) (io.ReadSeekCloser, int64, error) {
	return nil, 0, ErrDisabled
}
func (Noop) TouchBlob(string)                     {}
func (Noop) NewBlobWriter(string) (Writer, error) { return nil, ErrDisabled }
func (Noop) EnforceLimit() (GCResult, error)      { return GCResult{}, nil }
func (Noop) Purge() error                         { return nil }
func (Noop) Close() error                         { return nil }
