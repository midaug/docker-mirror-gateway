package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// hubLoginTimeout 是换取 Hub API token 的超时。要小于搜索总超时，
	// 给真正的搜索请求留出时间。
	hubLoginTimeout = 5 * time.Second
	// hubTokenLifetime 是 token 的保守缓存时长。Hub 签发的 JWT 有效期更长，
	// 这里按较短时间缓存、过期重换，避免用到失效 token。
	hubTokenLifetime = 30 * time.Minute
	// hubTokenRetry 是换取失败后的重试间隔，避免每次搜索都白跑一次登录。
	hubTokenRetry = time.Minute
)

// hubLoginURL 是变量，便于测试指向本地假服务。
var hubLoginURL = "https://hub.docker.com/v2/users/login/"

// hubTokenProvider 用配置的 Docker Hub 账号换取搜索 API 的 bearer token。
//
// 换 token 是"尽力而为"：失败时降级为匿名请求，不影响搜索本身。
// token 带过期与失败冷却，避免每次搜索都重复登录。
type hubTokenProvider struct {
	username string
	password string
	client   *http.Client // 裸客户端（带代理），用于登录请求本身
	log      *slog.Logger

	mu      sync.Mutex
	token   string
	expires time.Time
	retryAt time.Time
}

func newHubTokenProvider(username, password string, client *http.Client, log *slog.Logger) *hubTokenProvider {
	if client == nil {
		client = &http.Client{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &hubTokenProvider{username: username, password: password, client: client, log: log}
}

// refresh 在需要时换取新 token。用搜索的总 ctx 调用，给登录留足预算。
//
// 返回 true 表示本次调用确实登录换到了新 token。调用方据此判断：如果
// token 是刚换来的还被判 401，重试没有意义（凭据本身有问题），不该再登一次。
func (p *hubTokenProvider) refresh(ctx context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if p.token != "" && now.Before(p.expires) {
		return false
	}
	if now.Before(p.retryAt) {
		return false
	}
	token, err := p.login(ctx)
	if err != nil {
		p.token = ""
		p.retryAt = now.Add(hubTokenRetry)
		// 不打印凭据，只记录错误类型。
		p.log.Warn("Docker Hub 账号认证失败，搜索降级为匿名", "error", err)
		return false
	}
	p.token = token
	p.expires = now.Add(hubTokenLifetime)
	p.retryAt = time.Time{}
	p.log.Info("Docker Hub 搜索已启用账号认证", "user", p.username)
	return true
}

// bearer 返回当前可用的 Authorization 值；无有效 token 时返回空串。
func (p *hubTokenProvider) bearer() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token == "" || !time.Now().Before(p.expires) {
		return ""
	}
	return "Bearer " + p.token
}

// invalidate 在 Hub 明确拒绝当前 token（401）时清掉缓存，下次搜索重新登录。
// 典型场景是账号被禁用或 PAT 被吊销：不清理的话，失效 token 会被继续
// 带上最长 30 分钟。凭据本身有问题时登录会失败并走退避，不会形成登录风暴。
func (p *hubTokenProvider) invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.token = ""
	p.expires = time.Time{}
}

func (p *hubTokenProvider) login(ctx context.Context) (string, error) {
	body, err := json.Marshal(map[string]string{
		"username": p.username,
		"password": p.password,
	})
	if err != nil {
		return "", err
	}
	loginCtx, cancel := context.WithTimeout(ctx, hubLoginTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(loginCtx, http.MethodPost, hubLoginURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", &statusError{code: resp.StatusCode, msg: strings.TrimSpace(string(data))}
	}
	var payload struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("解析 Docker Hub 登录响应失败: %w", err)
	}
	if payload.Token == "" {
		return "", fmt.Errorf("Docker Hub 登录响应缺少 token")
	}
	return payload.Token, nil
}

// hubAuthTransport 给发往 Hub 的请求自动补上 Authorization。
type hubAuthTransport struct {
	base     http.RoundTripper
	provider *hubTokenProvider
	// host 是唯一允许携带 token 的主机名（hub.docker.com）。
	// 限定主机，避免上游重定向把 Bearer token 带到第三方域名。
	host string
}

func (t *hubAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.host != "" && req.URL.Hostname() == t.host {
		if auth := t.provider.bearer(); auth != "" {
			req = req.Clone(req.Context())
			req.Header.Set("Authorization", auth)
		}
	}
	return t.base.RoundTrip(req)
}

// hostOf 提取 URL 的主机名，解析失败时返回空串（此时不会附加 token）。
func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
