package access

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newReq(remote string, headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v2/library/alpine/manifests/latest", nil)
	r.RemoteAddr = remote
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestNoRulesAllowsEverything(t *testing.T) {
	c, err := New(nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if c.Enabled() {
		t.Fatal("没有规则时不该认为已启用")
	}
	if ok, _ := c.Allowed(newReq("203.0.113.9:5000", nil)); !ok {
		t.Error("没有规则时应放行")
	}
}

func TestAllowSingleIP(t *testing.T) {
	c, err := New([]string{"192.168.1.10"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok, ip := c.Allowed(newReq("192.168.1.10:5000", nil)); !ok || ip != "192.168.1.10" {
		t.Errorf("同 IP 应放行，got ok=%v ip=%q", ok, ip)
	}
	if ok, _ := c.Allowed(newReq("192.168.1.11:5000", nil)); ok {
		t.Error("不同 IP 应拒绝")
	}
}

func TestAllowCIDRAndIPv6(t *testing.T) {
	c, err := New([]string{"10.0.0.0/8", "2001:db8::/32"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		remote string
		want   bool
	}{
		{"10.1.2.3:1000", true},
		{"10.255.255.254:1000", true},
		{"11.0.0.1:1000", false},
		{"[2001:db8::1]:1000", true},
		{"[2001:dead::1]:1000", false},
	}
	for _, tc := range cases {
		if ok, _ := c.Allowed(newReq(tc.remote, nil)); ok != tc.want {
			t.Errorf("%s: ok=%v, 期望 %v", tc.remote, ok, tc.want)
		}
	}
}

func TestDenyWinsOverAllow(t *testing.T) {
	c, err := New([]string{"10.0.0.0/8"}, []string{"10.1.2.3"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Allowed(newReq("10.1.2.3:1000", nil)); ok {
		t.Error("命中了 deny 应拒绝，即使同时命中 allow")
	}
	if ok, _ := c.Allowed(newReq("10.4.5.6:1000", nil)); !ok {
		t.Error("只命中 allow 应放行")
	}
}

func TestDenyOnlyAllowsOthers(t *testing.T) {
	c, err := New(nil, []string{"192.0.2.0/24"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Allowed(newReq("192.0.2.5:1000", nil)); ok {
		t.Error("deny 段内应拒绝")
	}
	if ok, _ := c.Allowed(newReq("198.51.100.7:1000", nil)); !ok {
		t.Error("allow 为空时，deny 之外的来源应放行")
	}
}

func TestForwardedForIgnoredByDefault(t *testing.T) {
	c, err := New([]string{"10.0.0.0/8"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	// 真实对端不在白名单里，伪造 XFF 也不该放行。
	ok, ip := c.Allowed(newReq("203.0.113.7:1000", map[string]string{"X-Forwarded-For": "10.1.1.1"}))
	if ok {
		t.Error("未开启 trustForwardedFor 时不应采信 X-Forwarded-For")
	}
	if ip != "203.0.113.7" {
		t.Errorf("应取 TCP 对端地址，got %q", ip)
	}
}

func TestForwardedForTrusted(t *testing.T) {
	c, err := New([]string{"10.0.0.0/8"}, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	ok, ip := c.Allowed(newReq("127.0.0.1:1000", map[string]string{"X-Forwarded-For": "10.1.1.1, 172.16.0.1"}))
	if !ok {
		t.Error("开启信任后应使用 X-Forwarded-For 的第一个地址")
	}
	if ip != "10.1.1.1" {
		t.Errorf("ip = %q, 期望 10.1.1.1", ip)
	}
}

func TestIPv4MappedIPv6(t *testing.T) {
	c, err := New([]string{"10.0.0.0/8"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	// 双栈监听时对端可能是 IPv4-mapped 形式。
	if ok, _ := c.Allowed(newReq("[::ffff:10.1.2.3]:1000", nil)); !ok {
		t.Error("IPv4-mapped IPv6 地址应能匹配 IPv4 网段")
	}
}

func TestInvalidRulesRejected(t *testing.T) {
	if _, err := New([]string{"10.0.0.0/33"}, nil, false); err == nil {
		t.Error("非法 CIDR 应报错")
	}
	if _, err := New(nil, []string{"not-an-ip"}, false); err == nil {
		t.Error("非法 IP 应报错")
	}
}
