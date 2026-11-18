// Package notify 实现分组健康监控通知。
//
// 通知由健康探测循环驱动：每轮探测结束后评估各分组的健康镜像源数量，
// 跌破阈值时向配置的通道（webhook、SMTP 邮件）发送告警，恢复后同样
// 发送一条恢复通知。
//
// webhook 是通用模板协议：URL 与 body 都支持变量替换（text/template
// 语法），GET / POST 两种方法，因此飞书、企业微信或任何自定义协议
// 都能通过模板适配，无需为每个 IM 单独写死格式。
//
// 频率控制：状态变化（触发、恢复）始终立即通知；持续异常期间按
// notify.interval 重发提醒，避免消息轰炸。所有发送都在后台 goroutine
// 里完成，失败只记日志，绝不阻塞或拖垮探测循环。
package notify

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
)

// Alert 描述一次分组健康评估事件。
type Alert struct {
	Group     string   `json:"group"`             // 分组名
	State     string   `json:"state"`             // alert（触发）/ remind（持续异常提醒）/ recover（恢复）
	Healthy   int      `json:"healthy"`           // 当前健康镜像源数量
	Total     int      `json:"total"`             // 镜像源总数
	Threshold int      `json:"threshold"`         // 配置的通知阈值（minHealthy）
	Failing   []string `json:"failing,omitempty"` // 不可用镜像源的名称与最近错误
	Time      string   `json:"time"`              // 评估时间（RFC3339）
}

// 通知状态取值。
const (
	StateAlert   = "alert"
	StateRemind  = "remind"
	StateRecover = "recover"
)

// Notifier 是通知发送通道的统一接口。
type Notifier interface {
	Send(ctx context.Context, alert Alert) error
}

// groupState 记录一个分组的告警状态，用于状态变化判断与节流。
type groupState struct {
	open     bool      // 当前是否处于告警中
	lastSent time.Time // 最近一次发送（任意状态）的时间
}

// Manager 汇总所有通道并维护各分组的告警状态机。
type Manager struct {
	interval time.Duration
	channels []Notifier
	log      logger

	mu     sync.Mutex
	states map[string]*groupState
}

// logger 只依赖 slog 的最小接口，方便测试替身。
type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Debug(msg string, args ...any)
}

// New 按配置构建通知管理器。未启用或没有可用通道时返回 nil,
// 调用方对 nil 的一切调用都是安全的 no-op。
// 模板非法等配置问题在这里返回错误，启动时即可发现。
func New(cfg config.NotifyConfig, log logger) (*Manager, error) {
	if !cfg.Enabled || (len(cfg.Webhooks) == 0 && !cfg.Email.Enabled) {
		return nil, nil
	}
	m := &Manager{
		interval: cfg.Interval.D(),
		log:      log,
		states:   make(map[string]*groupState),
	}
	if m.interval <= 0 {
		m.interval = 30 * time.Minute
	}
	for i, w := range cfg.Webhooks {
		s, err := newWebhook(w)
		if err != nil {
			return nil, fmt.Errorf("notify.webhooks[%d] 配置无效: %w", i, err)
		}
		if s != nil {
			m.channels = append(m.channels, s)
		}
	}
	if e := newEmail(cfg.Email); e != nil {
		m.channels = append(m.channels, e)
	}
	if len(m.channels) == 0 {
		return nil, nil
	}
	log.Info("监控通知已启用", "channels", len(m.channels), "resendInterval", m.interval.String())
	return m, nil
}

// Enabled 报告是否有可用的通知通道。
func (m *Manager) Enabled() bool { return m != nil && len(m.channels) > 0 }

// Evaluate 评估一个分组的健康数量，按状态机决定是否发送通知。
//
//   - healthy < threshold：进入告警（首次立即发，之后按 interval 重发提醒）
//   - healthy >= threshold 且此前处于告警：发送恢复通知并复位
//
// 该方法本身不做任何网络 IO，发送全部转后台，可以在探测循环里直接调用。
func (m *Manager) Evaluate(group string, healthy, total, threshold int, failing []string) {
	if m == nil || len(m.channels) == 0 || threshold <= 0 {
		return
	}

	m.mu.Lock()
	st := m.states[group]
	if st == nil {
		st = &groupState{}
		m.states[group] = st
	}
	bad := healthy < threshold

	var state string
	switch {
	case bad && !st.open:
		state = StateAlert
	case bad && st.open && time.Since(st.lastSent) >= m.interval:
		state = StateRemind
	case !bad && st.open:
		state = StateRecover
	}
	if state == "" {
		m.mu.Unlock()
		return
	}
	st.open = bad
	st.lastSent = time.Now()
	m.mu.Unlock()

	alert := Alert{
		Group:     group,
		State:     state,
		Healthy:   healthy,
		Total:     total,
		Threshold: threshold,
		Failing:   failing,
		Time:      time.Now().Format(time.RFC3339),
	}
	m.dispatch(alert)
}

// dispatch 把一条通知发往所有通道；每个通道独立超时、独立记错。
func (m *Manager) dispatch(alert Alert) {
	m.log.Info("发送监控通知", "group", alert.Group, "state", alert.State,
		"healthy", alert.Healthy, "total", alert.Total, "threshold", alert.Threshold)

	for _, ch := range m.channels {
		ch := ch
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := ch.Send(ctx, alert); err != nil {
				m.log.Warn("监控通知发送失败", "channel", fmt.Sprintf("%T", ch), "group", alert.Group, "error", err)
			}
		}()
	}
}

// render 生成人类可读的标题与正文，也是 text 类消息的内容。
func render(a Alert) (string, string) {
	state := "告警"
	switch a.State {
	case StateRecover:
		state = "恢复"
	case StateRemind:
		state = "持续告警"
	}
	head := fmt.Sprintf("[镜像网关·%s] 分组 %s 可用镜像源 %d/%d（阈值 %d）", state, a.Group, a.Healthy, a.Total, a.Threshold)
	var b strings.Builder
	b.WriteString(head)
	if a.Time != "" {
		b.WriteString("\n时间: " + a.Time)
	}
	if len(a.Failing) > 0 {
		b.WriteString("\n不可用源:")
		for _, f := range a.Failing {
			b.WriteString("\n  - " + f)
		}
	}
	return head, b.String()
}

// ---------- webhook 通道 ----------

type webhook struct {
	method      string
	urlTpl      *template.Template
	bodyTpl     *template.Template // nil 表示 POST 默认发结构化 JSON
	contentType string
	client      *http.Client
	timeout     time.Duration
}

func newWebhook(cfg config.WebhookConfig) (Notifier, error) {
	if cfg.URL == "" {
		return nil, nil
	}
	method := strings.ToUpper(strings.TrimSpace(cfg.Method))
	if method == "" {
		method = http.MethodPost // config 校验已兜底，这里防测试/外部直调漏归一化
	}
	urlTpl, err := template.New("url").Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("url 模板非法: %w", err)
	}
	var bodyTpl *template.Template
	if method == http.MethodPost && strings.TrimSpace(cfg.Body) != "" {
		bodyTpl, err = template.New("body").Parse(cfg.Body)
		if err != nil {
			return nil, fmt.Errorf("body 模板非法: %w", err)
		}
	}
	timeout := cfg.Timeout.D()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &webhook{
		method:      method,
		urlTpl:      urlTpl,
		bodyTpl:     bodyTpl,
		contentType: cfg.ContentType,
		client:      &http.Client{},
		timeout:     timeout,
	}, nil
}

// tplData 构造模板可用的变量表。
//
// escape 决定 string 值是否做 URL query 转义：URL 模板用 true
// （空格、& 等特殊字符安全进入 query），body 模板用 false（原样嵌入）。
// TextJSON 始终是 JSON 字符串转义后的文本，方便直接嵌进 JSON 的引号里。
func tplData(a Alert, escape bool) map[string]any {
	stateText := "告警"
	switch a.State {
	case StateRecover:
		stateText = "恢复"
	case StateRemind:
		stateText = "持续告警"
	}
	_, text := render(a)
	// TextJSON 输出 JSON 字符串转义后的文本（不带外层引号），
	// 供 "…{{.TextJSON}}…" 直接嵌入模板里写好的引号中。
	textEscaped, _ := json.Marshal(text)
	textJSON := string(textEscaped[1 : len(textEscaped)-1])
	failing := strings.Join(a.Failing, "\n")

	get := func(s string) string {
		if escape {
			return url.QueryEscape(s)
		}
		return s
	}
	return map[string]any{
		"Title":     get(renderTitle(a)), // 单行标题
		"Message":   get(renderTitle(a)),
		"Text":      get(text),        // 完整多行文本（含时间与不可用源）
		"TextJSON":  string(textJSON), // JSON 转义后的完整文本
		"State":     a.State,
		"StateText": get(stateText),
		"Group":     get(a.Group),
		"Healthy":   a.Healthy,
		"Total":     a.Total,
		"Threshold": a.Threshold,
		"Failing":   get(failing), // 多行文本，每行一条
		"Time":      get(a.Time),
	}
}

// renderTitle 生成单行标题（render 返回的第一行）。
func renderTitle(a Alert) string {
	head, _ := render(a)
	return head
}

func execTpl(t *template.Template, data map[string]any) (string, error) {
	var b bytes.Buffer
	if err := t.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

// Send 渲染 URL（与 POST body）模板后推送。模板写错或渲染失败只影响
// 本通道本次发送，返回错误由 Manager 记日志。
func (w *webhook) Send(ctx context.Context, a Alert) error {
	rawURL, err := execTpl(w.urlTpl, tplData(a, true))
	if err != nil {
		return fmt.Errorf("渲染 url 模板失败: %w", err)
	}

	var body []byte
	if w.method == http.MethodPost {
		if w.bodyTpl != nil {
			s, err := execTpl(w.bodyTpl, tplData(a, false))
			if err != nil {
				return fmt.Errorf("渲染 body 模板失败: %w", err)
			}
			body = []byte(s)
		} else {
			body, _ = json.Marshal(a) // 未配置 body 模板时发送结构化 JSON
		}
	}

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, w.method, rawURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if w.method == http.MethodPost {
		req.Header.Set("Content-Type", w.contentType)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("webhook 返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return nil
}

// ---------- email 通道 ----------

type email struct {
	cfg     config.EmailConfig
	timeout time.Duration
}

func newEmail(cfg config.EmailConfig) Notifier {
	if !cfg.Enabled {
		return nil
	}
	return &email{cfg: cfg, timeout: cfg.Timeout.D()}
}

// Send 通过 SMTP 投递邮件。465 端口使用隐式 TLS，其余端口先明文连接
// 再尝试 STARTTLS（服务器不支持时保持明文，凭据是否安全由用户自行权衡）。
// 整个会话受连接 deadline 约束，不会因服务器无响应而挂起。
func (e *email) Send(ctx context.Context, a Alert) error {
	subject, body := render(a)
	addr := net.JoinHostPort(e.cfg.Host, strconv.Itoa(e.cfg.Port))
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	d := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	var err error
	if e.cfg.Port == 465 {
		conn, err = tls.DialWithDialer(d, "tcp", addr, &tls.Config{ServerName: e.cfg.Host})
	} else {
		conn, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("连接 SMTP %s 失败: %w", addr, err)
	}
	defer conn.Close()
	// net/smtp 不感知 context，用连接 deadline 兜底整个会话的超时：
	// SMTP 服务器卡住时最多占用 timeout 的挂起时间。
	_ = conn.SetDeadline(time.Now().Add(timeout))

	c, err := smtp.NewClient(conn, e.cfg.Host)
	if err != nil {
		return fmt.Errorf("SMTP 会话建立失败: %w", err)
	}

	if e.cfg.Port != 465 {
		if ok, _ := c.Extension("STARTTLS"); ok {
			if err := c.StartTLS(&tls.Config{ServerName: e.cfg.Host}); err != nil {
				return fmt.Errorf("STARTTLS 失败: %w", err)
			}
		}
	}
	if e.cfg.Username != "" {
		auth := smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, e.cfg.Host)
		if err := c.Auth(auth); err != nil {
			return fmt.Errorf("SMTP 认证失败: %w", err)
		}
	}
	if err := c.Mail(e.cfg.From); err != nil {
		return fmt.Errorf("设置发件人失败: %w", err)
	}
	for _, to := range e.cfg.To {
		if err := c.Rcpt(to); err != nil {
			return fmt.Errorf("设置收件人 %s 失败: %w", to, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(buildMessage(e.cfg.From, e.cfg.To, subject, body)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

// buildMessage 组装一封最简 RFC 5322 文本邮件（中文主题按 RFC 2047 编码）。
func buildMessage(from string, to []string, subject, body string) []byte {
	var b bytes.Buffer
	b.WriteString("From: " + from + "\r\n")
	b.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	return b.Bytes()
}
