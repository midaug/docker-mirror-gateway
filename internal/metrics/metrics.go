// Package metrics 提供一个极简的 Prometheus 文本格式指标收集器，避免引入外部依赖。
package metrics

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Collector 保存所有指标序列。
type Collector struct {
	mu     sync.RWMutex
	series map[string]*series
}

type series struct {
	name   string
	help   string
	typ    string
	labels string
	val    atomic.Int64
	fn     func() float64
}

// Counter 是一个单调递增的计数器句柄。
type Counter struct {
	s *series
}

// Inc 计数加一。
func (c *Counter) Inc() {
	if c != nil && c.s != nil {
		c.s.val.Add(1)
	}
}

// Add 增加 n。
func (c *Counter) Add(n int64) {
	if c != nil && c.s != nil {
		c.s.val.Add(n)
	}
}

// Value 返回当前计数。
func (c *Counter) Value() int64 {
	if c == nil || c.s == nil {
		return 0
	}
	return c.s.val.Load()
}

// New 创建一个 Collector。
func New() *Collector {
	return &Collector{series: make(map[string]*series)}
}

func labelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escape(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func escape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

func (c *Collector) getOrCreate(name, help, typ, labels string, fn func() float64) *series {
	key := name + labels
	// 先走读锁快路径：series 一旦创建就不会消失，运行期的绝大多数
	// 调用都是命中，避免所有并发请求挤在同一把写锁上。
	c.mu.RLock()
	if s, ok := c.series[key]; ok {
		c.mu.RUnlock()
		return s
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.series[key]; ok {
		return s
	}
	s := &series{name: name, help: help, typ: typ, labels: labels, fn: fn}
	c.series[key] = s
	return s
}

// Counter 注册（或复用）一个计数器。
func (c *Collector) Counter(name, help string, labels map[string]string) *Counter {
	return &Counter{s: c.getOrCreate(name, help, "counter", labelString(labels), nil)}
}

// GaugeFunc 注册一个在采集时求值的 gauge。
func (c *Collector) GaugeFunc(name, help string, labels map[string]string, fn func() float64) {
	c.getOrCreate(name, help, "gauge", labelString(labels), fn)
}

// WritePrometheus 以 Prometheus 文本格式输出全部指标。
func (c *Collector) WritePrometheus(w io.Writer) error {
	c.mu.RLock()
	items := make([]*series, 0, len(c.series))
	for _, s := range c.series {
		items = append(items, s)
	}
	c.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		if items[i].name != items[j].name {
			return items[i].name < items[j].name
		}
		return items[i].labels < items[j].labels
	})

	var buf bytes.Buffer
	lastName := ""
	for _, s := range items {
		if s.name != lastName {
			fmt.Fprintf(&buf, "# HELP %s %s\n# TYPE %s %s\n", s.name, s.help, s.name, s.typ)
			lastName = s.name
		}
		if s.typ == "gauge" && s.fn != nil {
			fmt.Fprintf(&buf, "%s%s %s\n", s.name, s.labels, strconv.FormatFloat(s.fn(), 'f', -1, 64))
			continue
		}
		fmt.Fprintf(&buf, "%s%s %d\n", s.name, s.labels, s.val.Load())
	}
	_, err := w.Write(buf.Bytes())
	return err
}
