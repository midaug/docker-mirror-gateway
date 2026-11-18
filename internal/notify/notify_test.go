package notify

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&strings.Builder{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

// reqRecord 记录一个收到的 webhook 请求。
type reqRecord struct {
	Method string
	URL    string
	Body   string
}

// hookServer 记录收到的全部 webhook 请求，供断言消息格式。
type hookServer struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []reqRecord
}

func newHookServer(t *testing.T) *hookServer {
	t.Helper()
	h := &hookServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64<<10)
		n, _ := r.Body.Read(buf)
		h.mu.Lock()
		h.reqs = append(h.reqs, reqRecord{Method: r.Method, URL: r.URL.String(), Body: string(buf[:n])})
		h.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":1}`))
	})
	h.Server = httptest.NewServer(mux)
	t.Cleanup(h.Server.Close)
	return h
}

func (h *hookServer) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.reqs)
}

func (h *hookServer) last() reqRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.reqs) == 0 {
		return reqRecord{}
	}
	return h.reqs[len(h.reqs)-1]
}

// waitForEvents 等待异步发送完成（dispatch 走后台 goroutine）。
func waitForEvents(t *testing.T, h *hookServer, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待 %d 条通知超时，实际收到 %d 条", want, h.count())
}

func newTestManager(t *testing.T, h *hookServer, mutate func(*config.NotifyConfig)) *Manager {
	t.Helper()
	// 默认 POST + body 模板输出完整文本，方便对消息内容做文本断言。
	cfg := config.NotifyConfig{
		Enabled:  true,
		Interval: config.Duration(30 * time.Minute),
		Webhooks: []config.WebhookConfig{{Method: "POST", URL: h.URL, Body: "{{.Text}}"}},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	m, err := New(cfg, testLogger())
	if err != nil {
		t.Fatalf("构建 Manager 失败: %v", err)
	}
	return m
}

func TestNewDisabledReturnsNil(t *testing.T) {
	if m, _ := New(config.NotifyConfig{Enabled: false}, testLogger()); m != nil {
		t.Fatal("notify.enabled=false 时应返回 nil")
	}
	if m, _ := New(config.NotifyConfig{Enabled: true}, testLogger()); m != nil {
		t.Fatal("enabled=true 但没有任何通道时应返回 nil")
	}
}

func TestNewInvalidTemplate(t *testing.T) {
	cfg := config.NotifyConfig{
		Enabled:  true,
		Webhooks: []config.WebhookConfig{{Method: "POST", URL: "https://example.com/hook", Body: `{"text":"{{.Text"`}},
	}
	if _, err := New(cfg, testLogger()); err == nil || !strings.Contains(err.Error(), "body 模板非法") {
		t.Fatalf("非法 body 模板应返回错误，实际: %v", err)
	}

	cfg.Webhooks[0].Body = ""
	cfg.Webhooks[0].URL = "https://example.com/{{.Bad..}}"
	if _, err := New(cfg, testLogger()); err == nil || !strings.Contains(err.Error(), "url 模板非法") {
		t.Fatalf("非法 url 模板应返回错误，实际: %v", err)
	}
}

func TestEvaluateNilManagerIsNoop(t *testing.T) {
	var m *Manager
	m.Evaluate("g", 0, 3, 1, nil) // 不应 panic
}

func TestWebhookBodyTemplate(t *testing.T) {
	h := newHookServer(t)
	// 用飞书消息格式验证模板能力：嵌 JSON 时用 TextJSON 保证引号安全。
	m := newTestManager(t, h, func(c *config.NotifyConfig) {
		c.Webhooks = []config.WebhookConfig{{
			Method: "POST",
			URL:    h.URL,
			Body:   `{"msg_type":"text","content":{"text":"{{.TextJSON}}"}}`,
		}}
	})

	m.Evaluate("dockerhub", 0, 5, 1, []string{`dockerhub（"连接"超时）`})
	waitForEvents(t, h, 1)

	var payload struct {
		MsgType string `json:"msg_type"`
		Content struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(h.last().Body), &payload); err != nil {
		t.Fatalf("飞书格式消息不是合法 JSON: %v\n%s", err, h.last().Body)
	}
	if payload.MsgType != "text" {
		t.Errorf("msg_type = %q, 期望 text", payload.MsgType)
	}
	if !strings.Contains(payload.Content.Text, "告警") ||
		!strings.Contains(payload.Content.Text, "dockerhub") ||
		!strings.Contains(payload.Content.Text, "0/5") ||
		!strings.Contains(payload.Content.Text, `"连接"超时`) {
		t.Errorf("消息内容缺少关键信息: %q", payload.Content.Text)
	}
}

func TestWebhookBodyTemplateVariables(t *testing.T) {
	h := newHookServer(t)
	m := newTestManager(t, h, func(c *config.NotifyConfig) {
		// 企微消息格式 + 状态/阈值等结构化变量，验证变量替换的覆盖面。
		c.Webhooks = []config.WebhookConfig{{
			Method: "POST",
			URL:    h.URL,
			Body:   `{"msgtype":"text","text":{"content":"{{.StateText}} {{.Group}} {{.Healthy}}/{{.Total}} 阈值{{.Threshold}} 时间{{.Time}}"}}`,
		}}
	})

	m.Evaluate("ghcr", 2, 4, 3, nil)
	waitForEvents(t, h, 1)

	var payload struct {
		Text struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.Unmarshal([]byte(h.last().Body), &payload); err != nil {
		t.Fatalf("消息不是合法 JSON: %v\n%s", err, h.last().Body)
	}
	for _, want := range []string{"告警", "ghcr", "2/4", "阈值3"} {
		if !strings.Contains(payload.Text.Content, want) {
			t.Errorf("消息缺少 %q: %q", want, payload.Text.Content)
		}
	}
}

func TestWebhookDefaultJSONBody(t *testing.T) {
	h := newHookServer(t)
	// POST 未配置 body 模板：默认发送结构化 JSON（与 API 字段一致）。
	m := newTestManager(t, h, func(c *config.NotifyConfig) {
		c.Webhooks = []config.WebhookConfig{{URL: h.URL}} // method 缺省为 POST
	})

	m.Evaluate("quay", 1, 4, 2, nil)
	waitForEvents(t, h, 1)

	rec := h.last()
	if rec.Method != http.MethodPost {
		t.Errorf("method = %q, 期望 POST", rec.Method)
	}
	var alert Alert
	if err := json.Unmarshal([]byte(rec.Body), &alert); err != nil {
		t.Fatalf("默认消息不是合法 JSON: %v\n%s", err, rec.Body)
	}
	if alert.Group != "quay" || alert.State != StateAlert || alert.Healthy != 1 || alert.Threshold != 2 {
		t.Errorf("默认消息字段不符: %+v", alert)
	}
}

func TestWebhookGetURLTemplate(t *testing.T) {
	h := newHookServer(t)
	m := newTestManager(t, h, func(c *config.NotifyConfig) {
		c.Webhooks = []config.WebhookConfig{{
			Method: "GET",
			URL:    h.URL + "/notify?group={{.Group}}&state={{.State}}&healthy={{.Healthy}}&msg={{.Message}}",
		}}
	})

	m.Evaluate("mcr", 1, 3, 2, []string{"mcr（超时 & 重试失败）"})
	waitForEvents(t, h, 1)

	rec := h.last()
	if rec.Method != http.MethodGet {
		t.Fatalf("method = %q, 期望 GET", rec.Method)
	}
	if rec.Body != "" {
		t.Errorf("GET 不应携带消息体: %q", rec.Body)
	}
	u, err := url.ParseRequestURI("http://x" + rec.URL)
	if err != nil {
		t.Fatalf("渲染后的 URL 非法: %v (%q)", err, rec.URL)
	}
	q := u.Query()
	if q.Get("group") != "mcr" || q.Get("state") != StateAlert || q.Get("healthy") != "1" {
		t.Errorf("query 变量替换不符: %v", q)
	}
	// Message 含 & 与空格，QueryEscape 后还原应完整无损。
	if q.Get("msg") != "[镜像网关·告警] 分组 mcr 可用镜像源 1/3（阈值 2）" {
		t.Errorf("msg 转义还原不符: %q", q.Get("msg"))
	}
}

func TestEvaluateStateMachine(t *testing.T) {
	h := newHookServer(t)
	// 重发间隔 100ms，方便测试持续告警的 remind 路径。
	m := newTestManager(t, h, func(c *config.NotifyConfig) {
		c.Interval = config.Duration(100 * time.Millisecond)
	})

	// 1. 首次跌破阈值：触发 alert。
	m.Evaluate("g", 0, 3, 1, nil)
	waitForEvents(t, h, 1)
	if !strings.Contains(h.last().Body, "告警") {
		t.Errorf("首次应触发告警，实际: %q", h.last().Body)
	}

	// 2. 间隔内再次评估：不重复发送。
	m.Evaluate("g", 0, 3, 1, nil)
	time.Sleep(60 * time.Millisecond)
	if n := h.count(); n != 1 {
		t.Fatalf("节流期内不应重发，实际 %d 条", n)
	}

	// 3. 超过重发间隔：remind。
	time.Sleep(80 * time.Millisecond)
	m.Evaluate("g", 0, 3, 1, nil)
	waitForEvents(t, h, 2)
	if !strings.Contains(h.last().Body, "持续告警") {
		t.Errorf("持续异常应发提醒，实际: %q", h.last().Body)
	}

	// 4. 恢复：立即发送恢复通知。
	m.Evaluate("g", 2, 3, 1, nil)
	waitForEvents(t, h, 3)
	if !strings.Contains(h.last().Body, "恢复") {
		t.Errorf("恢复后应发恢复通知，实际: %q", h.last().Body)
	}

	// 5. 恢复状态下继续评估：静默。
	m.Evaluate("g", 2, 3, 1, nil)
	time.Sleep(60 * time.Millisecond)
	if n := h.count(); n != 3 {
		t.Fatalf("正常状态不应发送，实际 %d 条", n)
	}
}

func TestEvaluateSkipsDisabledGroup(t *testing.T) {
	h := newHookServer(t)
	m := newTestManager(t, h, nil)

	// threshold=0 表示该分组未启用通知，任何数量都不发。
	m.Evaluate("g", 0, 3, 0, nil)
	time.Sleep(100 * time.Millisecond)
	if n := h.count(); n != 0 {
		t.Fatalf("threshold=0 时不应发送，实际 %d 条", n)
	}
}

func TestBuildMessageEncoding(t *testing.T) {
	msg := string(buildMessage("from@example.com", []string{"to@example.com"}, "告警标题", "正文内容"))
	if !strings.Contains(msg, "From: from@example.com") {
		t.Errorf("缺少 From 头: %q", msg)
	}
	if !strings.Contains(msg, "To: to@example.com") {
		t.Errorf("缺少 To 头: %q", msg)
	}
	if !strings.Contains(msg, "charset=utf-8") {
		t.Errorf("缺少字符集声明: %q", msg)
	}
	if !strings.Contains(msg, "=?utf-8?") {
		t.Errorf("中文主题应按 RFC 2047 编码: %q", msg)
	}
	if strings.Count(msg, "\r\n\r\n") != 1 || !strings.HasSuffix(msg, "正文内容") {
		t.Errorf("邮件头与正文分隔异常: %q", msg)
	}
}

func TestEmailChannelDisabled(t *testing.T) {
	if newEmail(config.EmailConfig{Enabled: false}) != nil {
		t.Fatal("email.enabled=false 时不应创建通道")
	}
}

func TestWebhookErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"err": "sign mismatch"}`))
	}))
	defer srv.Close()

	n, err := newWebhook(config.WebhookConfig{URL: srv.URL, Timeout: config.Duration(time.Second)})
	if err != nil {
		t.Fatalf("构建 webhook 失败: %v", err)
	}
	w := n.(Notifier)
	err = w.Send(context.Background(), Alert{Group: "g", State: StateAlert})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("非 2xx 响应应报错，实际: %v", err)
	}
}
