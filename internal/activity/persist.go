package activity

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// persistDelay 是记录变化后延迟写盘的节流窗口：窗口内的多次变化合并成
// 一次写入，避免大层传输时每条记录都触发磁盘 IO。
const persistDelay = 2 * time.Second

// NewPersistent 创建带文件持久化的记录器。
//
// path 为空、写目录失败或文件损坏时都退化为纯内存记录器（不报错），
// 保证持久化失败绝不影响镜像拉取本身。limit 是内存/文件保留条数，
// <=0 时用 DefaultStoreLimit。
//
// 调用方在退出前应调用 Close，把最后一次变化写入文件。
func NewPersistent(limit int, path string, log *slog.Logger) *Tracker {
	if limit <= 0 {
		limit = DefaultStoreLimit
	}
	t := New(limit)
	if path == "" {
		return t
	}
	t.storePath = path
	t.log = log
	t.dirty = make(chan struct{}, 1)
	t.stop = make(chan struct{})
	t.done = make(chan struct{})

	t.load()
	go t.saveLoop()
	return t
}

// Close 停止后台写盘并做最后一次保存。可重复调用。
func (t *Tracker) Close() error {
	if t == nil || t.stop == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		close(t.stop)
		<-t.done
		t.closeErr = t.save()
	})
	return t.closeErr
}

// load 从文件读取历史记录。任何错误都按「无历史」处理，记录一条警告。
func (t *Tracker) load() {
	data, err := os.ReadFile(t.storePath)
	if err != nil {
		if !os.IsNotExist(err) {
			t.warn("读取拉取记录失败，从空记录开始", "error", err)
		}
		return
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		t.warn("拉取记录文件损坏，从空记录开始", "error", err)
		return
	}
	// 文件里按时间正序保存（最旧在前）；按顺序还原后由 evictLocked 裁到上限。
	for _, r := range records {
		if r.State == StateActive {
			continue // 上次异常退出遗留的进行中记录没有意义
		}
		e := &Entry{
			id:        r.ID,
			kind:      r.Kind,
			repo:      r.Repo,
			reference: r.Reference,
			group:     r.Group,
			startedAt: r.StartedAt,
			state:     r.State,
			upstream:  r.Upstream,
			cache:     r.Cache,
			total:     r.Total,
			errMsg:    r.Error,
		}
		e.bytes.Store(r.Bytes)
		if r.EndedAt != nil {
			e.endedAt = *r.EndedAt
		}
		if r.ID > t.nextID {
			t.nextID = r.ID
		}
		t.entries = append(t.entries, e)
	}
	t.evictLocked()
}

// saveLoop 在收到变化通知后延迟写盘，窗口内合并多次变化。
func (t *Tracker) saveLoop() {
	defer close(t.done)
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-t.dirty:
			if timer == nil {
				timer = time.NewTimer(persistDelay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(persistDelay)
			}
		case <-timerC(timer):
			if err := t.save(); err != nil {
				t.warn("写入拉取记录失败", "error", err)
			}
		case <-t.stop:
			return // 最终写入由 Close 完成
		}
	}
}

// timerC 把可为 nil 的 timer 转成永远不就绪的 channel，
// 让 select 里无需对 nil timer 特判。
func timerC(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

// save 把当前已结束的记录原子写入文件。
func (t *Tracker) save() error {
	t.mu.Lock()
	records := make([]Record, 0, len(t.entries))
	for _, e := range t.entries {
		if e.State() == StateActive {
			continue
		}
		records = append(records, e.Snapshot())
	}
	t.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(t.storePath), 0o755); err != nil {
		return fmt.Errorf("创建数据目录失败: %w", err)
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	// 临时文件与目标同目录，保证 rename 是同一文件系统内的原子操作。
	tmp := t.storePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, t.storePath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (t *Tracker) warn(msg string, args ...any) {
	if t.log != nil {
		t.log.Warn(msg, args...)
	}
}

// signalDirty 通知后台需要写盘；非阻塞，满了说明已有变化在排队。
func (t *Tracker) signalDirty() {
	if t.dirty == nil {
		return
	}
	select {
	case t.dirty <- struct{}{}:
	default:
	}
}
