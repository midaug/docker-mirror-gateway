// Package activity 记录最近的镜像拉取活动，供控制台实时展示。
//
// 粒度是一次 HTTP 传输：manifest 一条、blob（层）一条。一条记录从请求开始
// 一直存活到传输结束，期间持续累加字节数，控制台据此算出实时速度。
//
// 只保留最近 limit 条。新增时如果已满，优先淘汰最旧的「已结束」记录——
// 正在传输的大层不会因为后面来了新请求就从列表里消失。
package activity

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// errAborted 表示请求在传输完成前就结束了（客户端断开、上下文取消等）。
var errAborted = errors.New("传输未完成（客户端断开或请求取消）")

// 传输类型。
const (
	KindManifest = "manifest"
	KindBlob     = "blob"
)

// 传输状态。
const (
	StateActive = "active" // 正在传输
	StateDone   = "done"   // 传输完成
	StateCached = "cached" // 命中本地缓存，未回源
	StateFailed = "failed" // 失败或中断
)

// DefaultLimit 是列表默认保留的条数。
const DefaultLimit = 100

// minSpeedWindow / minSpeedBytes 是计算速度的下限。低于这个量级的样本
// （几 KB 的镜像配置层、几十毫秒的小清单）算出来的数字没有参考价值，
// 直接按「无数据」处理，避免出现几百 MB/s 的假峰值。
const (
	minSpeedWindow = time.Millisecond
	minSpeedBytes  = 4096
)

// Record 是一条拉取记录的对外快照，直接序列化给控制台。
type Record struct {
	ID         uint64     `json:"id"`
	Kind       string     `json:"kind"`
	Repo       string     `json:"repo"`
	Reference  string     `json:"reference,omitempty"`
	Group      string     `json:"group,omitempty"`
	Upstream   string     `json:"upstream,omitempty"`
	State      string     `json:"state"`
	Cache      string     `json:"cache,omitempty"`
	Bytes      int64      `json:"bytes"`
	Total      int64      `json:"total,omitempty"`
	SpeedBPS   float64    `json:"speedBps"`
	StartedAt  time.Time  `json:"startedAt"`
	EndedAt    *time.Time `json:"endedAt,omitempty"`
	DurationMS int64      `json:"durationMs"`
	Error      string     `json:"error,omitempty"`
}

// Entry 是一条进行中的记录。所有方法都允许 nil 接收者，
// 这样调用方不必到处判空（activity 未启用时返回 nil 即可）。
type Entry struct {
	id        uint64
	kind      string
	repo      string
	reference string
	group     string
	startedAt time.Time

	bytes atomic.Int64
	once  sync.Once // 首次写入字节时记录传输起点

	mu         sync.Mutex
	upstream   string
	state      string
	cache      string
	total      int64
	errMsg     string
	endedAt    time.Time
	transferAt time.Time
}

// Tracker 维护最近 N 条记录。零值不可用，请用 New。
type Tracker struct {
	mu      sync.Mutex
	entries []*Entry
	limit   int
	nextID  uint64
}

// New 创建记录器。limit <= 0 时使用 DefaultLimit。
func New(limit int) *Tracker {
	if limit <= 0 {
		limit = DefaultLimit
	}
	return &Tracker{limit: limit}
}

// Begin 开始一条记录。group 可以为空（例如命中缓存时还不知道归属分组）。
func (t *Tracker) Begin(kind, repo, ref, group string) *Entry {
	if t == nil {
		return nil
	}
	e := &Entry{
		kind:      kind,
		repo:      repo,
		reference: ref,
		group:     group,
		state:     StateActive,
		startedAt: time.Now(),
	}
	t.mu.Lock()
	t.nextID++
	e.id = t.nextID
	t.entries = append(t.entries, e)
	t.evictLocked()
	t.mu.Unlock()
	return e
}

// evictLocked 淘汰超出上限的记录，优先淘汰最旧的已结束记录。
func (t *Tracker) evictLocked() {
	for len(t.entries) > t.limit {
		victim := -1
		for i, e := range t.entries {
			if e.State() != StateActive {
				victim = i
				break
			}
		}
		if victim < 0 {
			victim = 0
		}
		t.entries = append(t.entries[:victim], t.entries[victim+1:]...)
	}
}

// List 返回按时间倒序（最新在前）的快照，limit <= 0 表示不限。
func (t *Tracker) List(limit int) []Record {
	if t == nil {
		return []Record{}
	}
	t.mu.Lock()
	entries := make([]*Entry, len(t.entries))
	copy(entries, t.entries)
	t.mu.Unlock()

	out := make([]Record, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		out = append(out, entries[i].Snapshot())
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// Summary 是列表的汇总信息。
type Summary struct {
	Active   int     `json:"active"`
	Done     int     `json:"done"`
	Cached   int     `json:"cached"`
	Failed   int     `json:"failed"`
	Total    int     `json:"total"`
	SpeedBPS float64 `json:"speedBps"`
	Bytes    int64   `json:"bytes"`
}

// Summary 汇总当前列表。
func (t *Tracker) Summary() Summary {
	var s Summary
	for _, r := range t.List(0) {
		s.Total++
		s.Bytes += r.Bytes
		switch r.State {
		case StateActive:
			s.Active++
			s.SpeedBPS += r.SpeedBPS
		case StateCached:
			s.Cached++
		case StateFailed:
			s.Failed++
		default:
			s.Done++
		}
	}
	return s
}

// ---------- Entry ----------

// SetUpstream 记录本次尝试使用的上游；失败重试时会覆盖成最新的那个。
func (e *Entry) SetUpstream(name string) {
	if e == nil || name == "" {
		return
	}
	e.mu.Lock()
	e.upstream = name
	e.mu.Unlock()
}

// SetGroup 允许在拿到路由后再补分组。
func (e *Entry) SetGroup(name string) {
	if e == nil || name == "" {
		return
	}
	e.mu.Lock()
	if e.group == "" {
		e.group = name
	}
	e.mu.Unlock()
}

// SetCache 记录缓存状态：hit / miss / off。
func (e *Entry) SetCache(state string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.cache = state
	e.mu.Unlock()
}

// SetTotal 记录对象总大小（未知时传 -1）。
func (e *Entry) SetTotal(n int64) {
	if e == nil || n < 0 {
		return
	}
	e.mu.Lock()
	e.total = n
	e.mu.Unlock()
}

// SetBytes 一次性写入总字节（用于 manifest 这类已知大小、无需计时的场景）。
func (e *Entry) SetBytes(n int64) {
	if e == nil || n < 0 {
		return
	}
	e.bytes.Store(n)
}

// AddBytes 累加已传输字节，首次调用时开始计算速度。
func (e *Entry) AddBytes(n int64) {
	if e == nil || n <= 0 {
		return
	}
	e.once.Do(func() {
		e.mu.Lock()
		e.transferAt = time.Now()
		e.mu.Unlock()
	})
	e.bytes.Add(n)
}

// Finish 标记传输完成。
func (e *Entry) Finish() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != StateActive {
		return
	}
	e.state = StateDone
	e.endedAt = time.Now()
}

// FinishCached 标记为命中缓存。
func (e *Entry) FinishCached() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != StateActive {
		return
	}
	e.state = StateCached
	e.cache = "hit"
	e.endedAt = time.Now()
}

// Fail 标记失败。已结束的记录不会被覆盖。
func (e *Entry) Fail(err error) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != StateActive {
		return
	}
	e.state = StateFailed
	e.endedAt = time.Now()
	if err != nil {
		e.errMsg = err.Error()
	}
}

// Close 兜底：处理函数提前返回（例如客户端断开）时，避免记录永远停在「进行中」。
func (e *Entry) Close() {
	if e == nil {
		return
	}
	e.mu.Lock()
	active := e.state == StateActive
	e.mu.Unlock()
	if active {
		e.Fail(errAborted)
	}
}

// State 返回当前状态，用于淘汰时的判断。
func (e *Entry) State() string {
	if e == nil {
		return ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

// Snapshot 生成对外快照。
func (e *Entry) Snapshot() Record {
	if e == nil {
		return Record{}
	}
	e.mu.Lock()
	upstream, state, cache, group := e.upstream, e.state, e.cache, e.group
	total, errMsg, endedAt, transferAt := e.total, e.errMsg, e.endedAt, e.transferAt
	e.mu.Unlock()

	now := time.Now()
	end := endedAt
	if state == StateActive || end.IsZero() {
		end = now
	}
	var endedPtr *time.Time
	if !endedAt.IsZero() {
		t := endedAt
		endedPtr = &t
	}
	rec := Record{
		ID:         e.id,
		Kind:       e.kind,
		Repo:       e.repo,
		Reference:  e.reference,
		Group:      group,
		Upstream:   upstream,
		State:      state,
		Cache:      cache,
		Bytes:      e.bytes.Load(),
		Total:      total,
		StartedAt:  e.startedAt,
		EndedAt:    endedPtr,
		DurationMS: end.Sub(e.startedAt).Milliseconds(),
		Error:      errMsg,
	}
	// 速度只统计实际 body 传输阶段，避免把建连和 TTFB 算成慢速。
	if !transferAt.IsZero() && rec.Bytes >= minSpeedBytes {
		if d := end.Sub(transferAt); d >= minSpeedWindow {
			rec.SpeedBPS = float64(rec.Bytes) / d.Seconds()
		}
	}
	return rec
}
