// Package access 实现基于来源 IP 的拉取访问控制。
//
// 规则很简单：deny 优先于 allow；allow 为空表示"除 deny 外都放行"。
// 只作用于镜像拉取面，管理接口由 adminToken 与端口分离保护。
package access

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Control 是编译好的访问控制规则，创建后只读，可并发使用。
type Control struct {
	allow          []netip.Prefix
	deny           []netip.Prefix
	trustForwarded bool
}

// New 编译规则。allow / deny 支持单个 IP 与 CIDR，IPv4 与 IPv6 均可。
func New(allow, deny []string, trustForwarded bool) (*Control, error) {
	c := &Control{trustForwarded: trustForwarded}
	var err error
	if c.allow, err = parseList("access.allow", allow); err != nil {
		return nil, err
	}
	if c.deny, err = parseList("access.deny", deny); err != nil {
		return nil, err
	}
	return c, nil
}

func parseList(field string, items []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(items))
	for _, raw := range items {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		var p netip.Prefix
		if strings.Contains(s, "/") {
			v, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("%s 中的 %q 不是合法的网段: %w", field, raw, err)
			}
			// IPv4-mapped IPv6 网段（::ffff:10.0.0.0/104）转回 IPv4 表示，
			// 位长同步折算（-96），否则 Contains 永远匹配不上 IPv4 来源。
			if v.Addr().Is4In6() {
				bits := v.Bits() - 96
				if bits < 0 {
					bits = 0
				}
				v = netip.PrefixFrom(v.Addr().Unmap(), bits)
			}
			p = v
		} else {
			a, err := netip.ParseAddr(s)
			if err != nil {
				return nil, fmt.Errorf("%s 中的 %q 不是合法的 IP: %w", field, raw, err)
			}
			a = a.Unmap()
			p = netip.PrefixFrom(a, a.BitLen())
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

// Enabled 表示是否配置了任何规则。没有规则时不做限制。
func (c *Control) Enabled() bool {
	return c != nil && (len(c.allow) > 0 || len(c.deny) > 0)
}

// Allowed 判断请求来源是否放行，同时返回识别到的客户端 IP。
func (c *Control) Allowed(r *http.Request) (bool, string) {
	if !c.Enabled() {
		return true, ""
	}
	addr := c.ClientIP(r)
	if !addr.IsValid() {
		return false, ""
	}
	ip := addr.String()
	for _, p := range c.deny {
		if p.Contains(addr) {
			return false, ip
		}
	}
	if len(c.allow) == 0 {
		return true, ip
	}
	for _, p := range c.allow {
		if p.Contains(addr) {
			return true, ip
		}
	}
	return false, ip
}

// ClientIP 取客户端地址。
//
// 默认只看 TCP 对端地址：X-Forwarded-For 可以被任意客户端伪造，
// 只有明确配置了 trustForwardedFor 时才会采信。
func (c *Control) ClientIP(r *http.Request) netip.Addr {
	if c != nil && c.trustForwarded {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first := strings.TrimSpace(strings.Split(xff, ",")[0])
			if a, err := netip.ParseAddr(first); err == nil {
				return a.Unmap()
			}
		}
		if xr := strings.TrimSpace(r.Header.Get("X-Real-IP")); xr != "" {
			if a, err := netip.ParseAddr(xr); err == nil {
				return a.Unmap()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	a, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return netip.Addr{}
	}
	return a.Unmap()
}

// Describe 返回用于状态展示的规则摘要。
func (c *Control) Describe() (allow []string, deny []string, trustForwarded bool) {
	if c == nil {
		return nil, nil, false
	}
	for _, p := range c.allow {
		allow = append(allow, p.String())
	}
	for _, p := range c.deny {
		deny = append(deny, p.String())
	}
	return allow, deny, c.trustForwarded
}
