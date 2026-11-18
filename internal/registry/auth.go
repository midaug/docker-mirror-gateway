package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// challenge 是 WWW-Authenticate 响应头的解析结果。
type challenge struct {
	Scheme  string
	Realm   string
	Service string
	Scope   string
}

// parseChallenge 解析形如：
//
//	Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
func parseChallenge(header string) challenge {
	header = strings.TrimSpace(header)
	if header == "" {
		return challenge{}
	}
	scheme, rest, _ := strings.Cut(header, " ")
	c := challenge{Scheme: strings.ToLower(strings.TrimSpace(scheme))}
	if rest == "" {
		return c
	}
	for _, part := range splitParams(rest) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"`)
		switch key {
		case "realm":
			c.Realm = value
		case "service":
			c.Service = value
		case "scope":
			c.Scope = value
		}
	}
	return c
}

// splitParams 按逗号切分参数，忽略引号内的逗号。
func splitParams(s string) []string {
	var (
		parts   []string
		current strings.Builder
		quoted  bool
	)
	for _, r := range s {
		switch {
		case r == '"':
			quoted = !quoted
			current.WriteRune(r)
		case r == ',' && !quoted:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if strings.TrimSpace(current.String()) != "" {
		parts = append(parts, current.String())
	}
	return parts
}

type tokenEntry struct {
	token  string
	expiry time.Time
}

// tokenStore 按 scope 缓存上游访问令牌。
type tokenStore struct {
	mu    sync.Mutex
	items map[string]tokenEntry
}

func newTokenStore() *tokenStore {
	return &tokenStore{items: make(map[string]tokenEntry)}
}

func (s *tokenStore) get(scope string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[scope]; ok {
		if time.Now().Before(e.expiry) {
			return e.token
		}
		delete(s.items, scope)
	}
	return ""
}

func (s *tokenStore) put(scope, token string, ttl time.Duration) {
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 超过上限时随机剔除一半，保留多数仍有效的 token（scope 数通常远
	// 达不到上限，这里只是防御恶意构造的超多仓库请求）。
	if len(s.items) > 4096 {
		for k := range s.items {
			if len(s.items) <= 2048 {
				break
			}
			delete(s.items, k)
		}
	}
	s.items[scope] = tokenEntry{token: token, expiry: time.Now().Add(ttl)}
}

func (s *tokenStore) invalidate(scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, scope)
}

// ErrUnauthorized 表示上游拒绝访问且无法通过令牌流程恢复。
var ErrUnauthorized = errors.New("上游镜像源认证失败")

// fetchToken 通过 realm 换取 Bearer token。
func (u *Upstream) fetchToken(ctx context.Context, ch challenge, scope string) (string, time.Duration, error) {
	if ch.Realm == "" {
		return "", 0, fmt.Errorf("%w: 缺少认证 realm", ErrUnauthorized)
	}
	q := url.Values{}
	q.Set("client_id", "docker-mirror-gateway")
	if ch.Service != "" {
		q.Set("service", ch.Service)
	}
	effectiveScope := scope
	if effectiveScope == "" {
		effectiveScope = ch.Scope
	}
	if effectiveScope != "" {
		q.Set("scope", effectiveScope)
	}

	realm := ch.Realm
	if strings.Contains(realm, "?") {
		realm += "&" + q.Encode()
	} else {
		realm += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Accept", "application/json")
	if u.username != "" {
		req.SetBasicAuth(u.username, u.password)
	}

	resp, err := u.tokenClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("请求令牌失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("%w: 令牌接口返回 %d: %s", ErrUnauthorized, resp.StatusCode, snippet(body))
	}

	var payload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("解析令牌响应失败: %w", err)
	}
	token := payload.Token
	if token == "" {
		token = payload.AccessToken
	}
	if token == "" {
		return "", 0, fmt.Errorf("%w: 令牌响应为空", ErrUnauthorized)
	}

	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if ttl > 30*time.Second {
		ttl -= 30 * time.Second
	}
	return token, ttl, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
