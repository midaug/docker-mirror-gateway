package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// stubSources 把两个来源地址指到本地假服务，返回恢复函数。
func stubSources(t *testing.T, hub, quay *httptest.Server) {
	t.Helper()
	oldHub, oldQuay, oldTag := hubSearchURL, quaySearchURL, quayRepositoryTagURL
	oldHubTag, oldMCRTag := hubTagURL, mcrTagURL
	if hub != nil {
		hubSearchURL = hub.URL + "/v2/search/repositories/"
		hubTagURL = hub.URL + "/v2/repositories/"
	}
	if quay != nil {
		quaySearchURL = quay.URL + "/api/v1/find/repositories"
		quayRepositoryTagURL = quay.URL + "/api/v1/repository/%s/%s/tag/?limit=50&onlyActiveTags=true&page=1"
	}
	t.Cleanup(func() {
		hubSearchURL, quaySearchURL, quayRepositoryTagURL = oldHub, oldQuay, oldTag
		hubTagURL, mcrTagURL = oldHubTag, oldMCRTag
	})
}

func TestSearchMergesAndSorts(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "nginx" {
			t.Errorf("透传的查询参数 = %q", r.URL.Query().Get("query"))
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"count":2,"results":[
			{"repo_name":"nginxinc/nginx-unprivileged","short_description":"unprivileged build","star_count":120,"pull_count":1000},
			{"repo_name":"library/nginx","short_description":"Official build of Nginx.","star_count":19000,"pull_count":9000000,"is_official":true}
		]}`)
	}))
	defer hub.Close()

	quay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"results":[
			{"namespace":{"name":"prometheus"},"name":"node-exporter","description":"Prometheus exporter","is_public":true},
			{"namespace":{"name":"internal"},"name":"secret","description":"私有仓库","is_public":false}
		]}`)
	}))
	defer quay.Close()

	stubSources(t, hub, quay)
	svc := New(nil, 3*time.Second, []string{"hub", "quay"}, false, discardLogger())
	resp := svc.Search(context.Background(), "nginx", 10)

	if len(resp.Errors) != 0 {
		t.Fatalf("不该有错误: %v", resp.Errors)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("结果数 = %d, 期望 3（私有仓库应被过滤）", len(resp.Results))
	}
	if resp.Results[0].Name != "library/nginx" || !resp.Results[0].Official {
		t.Errorf("官方镜像应排第一，got %+v", resp.Results[0])
	}
	if resp.Results[0].Group != "dockerhub" {
		t.Errorf("Hub 结果的分组 = %q", resp.Results[0].Group)
	}
	var foundQuay bool
	for _, r := range resp.Results {
		if r.Source == "quay" {
			foundQuay = true
			if r.Group != "quay" || r.Name != "prometheus/node-exporter" {
				t.Errorf("Quay 结果格式不对: %+v", r)
			}
		}
	}
	if !foundQuay {
		t.Error("缺少 Quay 来源的结果")
	}
}

func TestSearchNormalizesLibraryPrefix(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 只给 name 字段，且不带命名空间，应补成 library/。
		io.WriteString(w, `{"results":[{"name":"redis","short_description":"Redis","star_count":5}]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger())
	resp := svc.Search(context.Background(), "redis", 5)
	if len(resp.Results) != 1 || resp.Results[0].Name != "library/redis" {
		t.Fatalf("期望 library/redis, got %+v", resp.Results)
	}
}

func TestSearchPartialFailure(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer hub.Close()
	quay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"results":[{"namespace":{"name":"quay"},"name":"app","is_public":true}]}`)
	}))
	defer quay.Close()

	stubSources(t, hub, quay)
	svc := New(nil, 3*time.Second, []string{"hub", "quay"}, false, discardLogger())
	resp := svc.Search(context.Background(), "app", 10)

	if len(resp.Errors) != 1 {
		t.Errorf("应记录 1 个来源失败，got %v", resp.Errors)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("另一个来源的结果应保留，got %d", len(resp.Results))
	}
}

func TestSearchRespectsLimit(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"results":[
			{"repo_name":"a/one"},{"repo_name":"a/two"},{"repo_name":"a/three"}
		]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger())
	resp := svc.Search(context.Background(), "a", 2)
	if len(resp.Results) != 2 {
		t.Errorf("limit=2 时结果数 = %d", len(resp.Results))
	}
}

func TestSearchEmptyQuery(t *testing.T) {
	svc := New(nil, time.Second, []string{"hub"}, false, discardLogger())
	resp := svc.Search(context.Background(), "   ", 10)
	if len(resp.Results) != 0 {
		t.Error("空查询不该发请求")
	}
}

func TestCleanDescription(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"  多行\n描述\t带空白  ", "多行 描述 带空白"},
		{"[Alertmanager](https://example.com) 是告警组件", "Alertmanager 是告警组件"},
		{"<img src=\"x\">带 HTML 的描述", "带 HTML 的描述"},
		{"![CircleCI][circleci] [Docker Pulls][hub] 构建状态", "Docker Pulls 构建状态"},
		{"# 标题 内容", "标题 内容"},
		{"`code` 说明", "code 说明"},
	}
	for _, tc := range cases {
		if got := cleanDescription(tc.in); got != tc.want {
			t.Errorf("cleanDescription(%q) = %q, 期望 %q", tc.in, got, tc.want)
		}
	}

	long := strings.Repeat("很长的描述", 100)
	got := cleanDescription(long)
	if len([]rune(got)) != maxDescriptionLen+1 {
		t.Errorf("超长描述应截断到 %d 字符（含省略号），实际 %d", maxDescriptionLen+1, len([]rune(got)))
	}
}

func TestSearchHubNormalizesUpdated(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"results":[{"repo_name":"library/nginx","last_updated":"2026-11-16T04:29:28.123456Z"}]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger())
	resp := svc.Search(context.Background(), "nginx", 5)
	if len(resp.Results) != 1 {
		t.Fatalf("结果数 = %d", len(resp.Results))
	}
	if got := resp.Results[0].Updated; got != "2026-11-16T04:29:28Z" {
		t.Errorf("更新时间 = %q, 期望 RFC3339 归一化结果", got)
	}
}

func TestSearchQuayEnrichesUpdated(t *testing.T) {
	var tagHits int
	quay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/find/repositories"):
			io.WriteString(w, `{"results":[{"namespace":{"name":"nginx"},"name":"app","is_public":true},
				{"namespace":{"name":"other"},"name":"app2","is_public":true}]}`)
		case strings.HasPrefix(r.URL.Path, "/api/v1/repository/nginx/app/tag/"):
			tagHits++
			io.WriteString(w, `{"tags":[{"name":"latest","last_modified":"Mon, 16 Nov 2026 04:29:28 -0000"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer quay.Close()
	stubSources(t, nil, quay)

	svc := New(nil, 3*time.Second, []string{"quay"}, true, discardLogger())
	resp := svc.Search(context.Background(), "app", 10)
	if len(resp.Results) != 2 {
		t.Fatalf("结果数 = %d", len(resp.Results))
	}
	if tagHits == 0 {
		t.Fatal("没有补查标签时间")
	}
	var nginxUpdated string
	for _, r := range resp.Results {
		if r.Name == "nginx/app" {
			nginxUpdated = r.Updated
		}
	}
	if nginxUpdated != "2026-11-16T04:29:28Z" {
		t.Errorf("Quay 更新时间 = %q", nginxUpdated)
	}
}

func TestSearchMCRCatalog(t *testing.T) {
	var hits int
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		io.WriteString(w, `{"repositories":[
			"dotnet/runtime","dotnet/sdk","azure/dotnet/debug","mcr.microsoft.com/dotnet/aspnet","other/nginx"
		]}`)
	}))
	defer catalog.Close()
	old := mcrCatalogURL
	mcrCatalogURL = catalog.URL + "/v2/_catalog"
	t.Cleanup(func() { mcrCatalogURL = old })

	svc := New(nil, 3*time.Second, []string{"mcr"}, false, discardLogger())
	resp := svc.Search(context.Background(), "dotnet", 10)
	if len(resp.Errors) != 0 {
		t.Fatalf("不该有错误: %v", resp.Errors)
	}
	if len(resp.Results) != 4 {
		t.Fatalf("结果数 = %d, 期望 4: %+v", len(resp.Results), resp.Results)
	}
	// 前缀匹配的 dotnet/* 应该排在 azure/dotnet/debug 前面。
	if resp.Results[0].Source != "mcr" || !strings.HasPrefix(resp.Results[0].Name, "dotnet/") {
		t.Errorf("排序不对: %+v", resp.Results[0])
	}
	if resp.Results[0].Group != "mcr" {
		t.Errorf("分组 = %q", resp.Results[0].Group)
	}

	// 目录应被缓存，第二次搜索不再请求。
	resp2 := svc.Search(context.Background(), "runtime", 10)
	if len(resp2.Results) != 1 || resp2.Results[0].Name != "dotnet/runtime" {
		t.Errorf("第二次搜索 = %+v", resp2.Results)
	}
	if hits != 1 {
		t.Errorf("目录请求次数 = %d, 期望 1（应命中缓存）", hits)
	}
}

// recordingTransport 记录请求地址并返回固定响应。
type recordingTransport struct {
	urls []string
	body string
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.urls = append(r.urls, req.URL.String())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(r.body)),
		Request:    req,
	}, nil
}

// Hub 搜索应走 WithHubClient 指定的客户端，Quay 仍用默认客户端。
func TestSearchHubUsesDedicatedClient(t *testing.T) {
	oldHub, oldQuay := hubSearchURL, quaySearchURL
	hubSearchURL = "http://hub.invalid/v2/search/repositories/"
	quaySearchURL = "http://quay.invalid/api/v1/find/repositories"
	t.Cleanup(func() { hubSearchURL, quaySearchURL = oldHub, oldQuay })

	hubRec := &recordingTransport{body: `{"count":1,"results":[
		{"repo_name":"library/nginx","short_description":"Official build","star_count":10,"pull_count":100,"is_official":true}
	]}`}
	quayRec := &recordingTransport{body: `{"results":[
		{"namespace":{"name":"prometheus"},"name":"node-exporter","description":"exporter","is_public":true}
	]}`}

	svc := New(&http.Client{Transport: quayRec}, 3*time.Second, []string{"hub", "quay"}, false, discardLogger(),
		WithHubClient(&http.Client{Transport: hubRec}))

	resp := svc.Search(context.Background(), "nginx", 10)
	if len(resp.Errors) != 0 {
		t.Fatalf("不该有错误: %v", resp.Errors)
	}
	if len(hubRec.urls) != 1 || !strings.HasPrefix(hubRec.urls[0], hubSearchURL) {
		t.Fatalf("Hub 请求应走专用客户端，实际 %v", hubRec.urls)
	}
	if len(quayRec.urls) != 1 || !strings.HasPrefix(quayRec.urls[0], quaySearchURL) {
		t.Fatalf("Quay 应继续使用默认客户端，实际 %v", quayRec.urls)
	}
}

// 未指定 hubClient 时退回默认客户端。
func TestSearchHubFallsBackToDefaultClient(t *testing.T) {
	oldHub := hubSearchURL
	hubSearchURL = "http://hub.invalid/v2/search/repositories/"
	t.Cleanup(func() { hubSearchURL = oldHub })

	rec := &recordingTransport{body: `{"count":0,"results":[]}`}
	svc := New(&http.Client{Transport: rec}, 3*time.Second, []string{"hub"}, false, discardLogger())

	if resp := svc.Search(context.Background(), "nginx", 5); len(resp.Errors) != 0 {
		t.Fatalf("不该有错误: %v", resp.Errors)
	}
	if len(rec.urls) != 1 {
		t.Fatalf("应使用默认客户端发一次请求，实际 %v", rec.urls)
	}
}

// pickNewestTag 应挑出版本号最大的标签，跳过 latest、浮动标签与日期标签。
func TestPickNewestTag(t *testing.T) {
	cases := []struct {
		name string
		tags []string
		want string
	}{
		{"取最大版本", []string{"latest", "1.27.2", "1.27.3", "mainline"}, "1.27.3"},
		{"多段优先", []string{"latest", "1.27", "1.27.3", "1.26.9"}, "1.27.3"},
		{"v 前缀", []string{"latest", "v2.0.9", "v2.1.0"}, "v2.1.0"},
		{"同版本优先不带后缀", []string{"1.27.3-alpine", "1.27.3", "1.27.3-bookworm"}, "1.27.3"},
		{"没有版本号", []string{"latest", "stable", "mainline"}, ""},
		{"日期标签不算版本", []string{"2024-01-01", "2023-12-31"}, ""},
		{"空列表", nil, ""},
	}
	for _, c := range cases {
		if got := pickNewestTag(c.tags); got != c.want {
			t.Errorf("%s: pickNewestTag(%v) = %q, 期望 %q", c.name, c.tags, got, c.want)
		}
	}
}

// Hub 搜索应补查标签，同时拿到最新版本号与更新时间（v2 搜索接口已不带时间）。
func TestSearchEnrichesHubTags(t *testing.T) {
	var tagHits int
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v2/search/repositories/"):
			io.WriteString(w, `{"count":1,"results":[
				{"repo_name":"library/nginx","short_description":"Official build","star_count":10,"pull_count":100,"is_official":true}
			]}`)
		case strings.HasPrefix(r.URL.Path, "/v2/repositories/library/nginx/tags/"):
			tagHits++
			io.WriteString(w, `{"results":[
				{"name":"latest","last_updated":"2026-11-13T13:52:24.937743Z"},
				{"name":"mainline","last_updated":"2026-11-13T13:52:24.937743Z"},
				{"name":"1.27.3","last_updated":"2026-11-12T09:00:00Z"},
				{"name":"1.27.2","last_updated":"2026-11-01T09:00:00Z"}
			]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, true, discardLogger(), WithTags(true))
	resp := svc.Search(context.Background(), "nginx", 10)
	if len(resp.Results) != 1 {
		t.Fatalf("结果数 = %d", len(resp.Results))
	}
	got := resp.Results[0]
	if got.LatestTag != "1.27.3" {
		t.Errorf("最新版本 = %q, 期望 1.27.3", got.LatestTag)
	}
	if got.Updated != "2026-11-13T13:52:24Z" {
		t.Errorf("更新时间 = %q", got.Updated)
	}
	if tagHits != 1 {
		t.Errorf("标签接口调用 %d 次，期望 1 次", tagHits)
	}

	// 同一个仓库再次搜索应命中缓存，不再请求标签接口。
	svc.Search(context.Background(), "nginx", 10)
	if tagHits != 1 {
		t.Errorf("命中缓存后不该重复请求，实际 %d 次", tagHits)
	}
}

// 截断时应为没有热度数据的来源保留席位，不能被 Hub 长尾全部挤出。
func TestTruncateKeepsSourceFloor(t *testing.T) {
	results := make([]Result, 0, 22)
	for i := 0; i < 20; i++ {
		results = append(results, Result{Source: "hub", Name: fmt.Sprintf("user/repo%d", i), Pulls: int64(1000 - i), order: i, sourceRank: 0})
	}
	results = append(results, Result{Source: "mcr", Name: "dotnet/runtime", order: 0, sourceRank: 2})
	results = append(results, Result{Source: "mcr", Name: "dotnet/aspnet", order: 1, sourceRank: 2})

	got := truncateWithSourceFloor(results, 20)
	if len(got) != 20 {
		t.Fatalf("长度 = %d, 期望 20", len(got))
	}
	var mcr int
	for _, r := range got {
		if r.Source == "mcr" {
			mcr++
		}
	}
	if mcr != 2 {
		t.Errorf("mcr 入选 %d 条, 期望保底 2 条", mcr)
	}
	// 头部高热度结果不被挤掉。
	if got[0].Name != "user/repo0" {
		t.Errorf("头部第一条 = %s, 期望 user/repo0", got[0].Name)
	}
}

// 已有足够席位的来源不再补位。
func TestTruncateNoFloorWhenRepresented(t *testing.T) {
	results := make([]Result, 0, 25)
	for i := 0; i < 20; i++ {
		results = append(results, Result{Source: "hub", Name: fmt.Sprintf("user/repo%d", i), Pulls: int64(1000 - i), order: i, sourceRank: 0})
	}
	for i := 0; i < 5; i++ {
		results = append(results, Result{Source: "quay", Name: fmt.Sprintf("org/q%d", i), order: i, sourceRank: 1})
	}
	got := truncateWithSourceFloor(results, 20)
	var quay, hubTail int
	for i, r := range got {
		switch r.Source {
		case "quay":
			quay++
		case "hub":
			if i >= 17 { // 尾部补位区
				hubTail++
			}
		}
	}
	if quay != 3 {
		t.Errorf("quay 入选 %d 条, 期望保底 3 条", quay)
	}
	if hubTail != 0 {
		t.Errorf("hub 不该被补位条目替换 %d 条", hubTail)
	}
}

// splitQueryTag 只拆「名字:合法标签」，其余原样返回。
func TestSplitQueryTag(t *testing.T) {
	cases := []struct{ in, name, tag string }{
		{"nginx:1.31.8", "nginx", "1.31.8"},
		{"bitnami/nginx:1.31.8-alpine", "bitnami/nginx", "1.31.8-alpine"},
		{"nginx", "nginx", ""},
		{"nginx:", "nginx:", ""},                         // 空标签不拆
		{"nginx:不是tag", "nginx:不是tag", ""},               // 非法字符不拆
		{"localhost:5000/app", "localhost:5000/app", ""}, // 端口形式不拆
	}
	for _, c := range cases {
		name, tag := splitQueryTag(c.in)
		if name != c.name || tag != c.tag {
			t.Errorf("splitQueryTag(%q) = (%q, %q), 期望 (%q, %q)", c.in, name, tag, c.name, c.tag)
		}
	}
}

// tagMatches 精确相等或同版本变体（前缀+分隔符）都算命中。
func TestTagMatches(t *testing.T) {
	tags := []string{"latest", "1.31.8", "1.31.8-alpine3.24", "1.30.6-perl", "mainline"}
	for _, want := range []string{"1.31.8", "1.31.8-alpine3.24", "latest", "1.30.6", "1.31"} {
		if !tagMatches(tags, want) {
			t.Errorf("tagMatches(%v, %q) 应命中", tags, want)
		}
	}
	for _, want := range []string{"1.32", "nginx", "1.12"} {
		if tagMatches(tags, want) {
			t.Errorf("tagMatches(%v, %q) 不该命中", tags, want)
		}
	}
}

// displayTags 跳过滚动别名，只给真正的版本标签。
func TestDisplayTags(t *testing.T) {
	tags := []string{"latest", "mainline", "1.31.8-alpine3.24", "1.31.8", "1.30.6"}
	got := displayTags(tags, 10)
	want := []string{"1.31.8-alpine3.24", "1.31.8", "1.30.6"}
	if len(got) != len(want) {
		t.Fatalf("displayTags = %v, 期望 %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("displayTags = %v, 期望 %v", got, want)
		}
	}
	if got := displayTags(tags, 2); len(got) != 2 {
		t.Errorf("n=2 时应只取 2 个, got %v", got)
	}
}

// sortTagsByVersion 把 MCR 的字母序列表重排为版本从新到旧。
func TestSortTagsByVersion(t *testing.T) {
	tags := []string{"10.0-alpine", "9.0", "10.0", "8.0.19", "latest"}
	sortTagsByVersion(tags)
	if tags[0] != "10.0" {
		t.Errorf("第一个应是 10.0（同版本短的在前）, got %v", tags)
	}
	if tags[1] != "10.0-alpine" || tags[2] != "9.0" {
		t.Errorf("顺序错误: %v", tags)
	}
}

// 带 tag 搜索时只保留确实拥有该标签的仓库。
func TestSearchFiltersByTag(t *testing.T) {
	// 并发 handler 都会自增，必须用原子类型（-race 会检出普通 int 的竞态）。
	var tagHits atomic.Int64
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v2/search/repositories/"):
			if q := r.URL.Query().Get("query"); q != "nginx" {
				t.Errorf("搜索词应去掉 tag 部分, got %q", q)
			}
			io.WriteString(w, `{"count":2,"results":[
				{"repo_name":"library/nginx","short_description":"Official","star_count":10,"pull_count":100,"is_official":true},
				{"repo_name":"other/nginx","short_description":"Fork","star_count":1,"pull_count":2}
			]}`)
		case strings.HasPrefix(r.URL.Path, "/v2/repositories/library/nginx/tags/"):
			tagHits.Add(1)
			io.WriteString(w, `{"results":[
				{"name":"latest","last_updated":"2026-11-13T13:52:24Z"},
				{"name":"1.31.8-alpine3.24","last_updated":"2026-11-13T13:52:24Z"},
				{"name":"1.31.8","last_updated":"2026-11-12T09:00:00Z"}
			]}`)
		case strings.HasPrefix(r.URL.Path, "/v2/repositories/other/nginx/tags/"):
			tagHits.Add(1)
			io.WriteString(w, `{"results":[{"name":"latest","last_updated":"2026-11-13T13:52:24Z"},{"name":"0.9"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, true, discardLogger(), WithTags(true))
	resp := svc.Search(context.Background(), "nginx:1.31.8", 10)
	if resp.Query != "nginx:1.31.8" {
		t.Errorf("Query 应保留原始输入, got %q", resp.Query)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("应只剩拥有 1.31.8 的仓库, got %d 条: %+v", len(resp.Results), resp.Results)
	}
	r := resp.Results[0]
	if r.Name != "library/nginx" {
		t.Errorf("留下的应是 library/nginx, got %s", r.Name)
	}
	if r.URL != "https://hub.docker.com/_/nginx" {
		t.Errorf("官方镜像 URL = %q", r.URL)
	}
	if len(r.RecentTags) != 2 || r.RecentTags[0] != "1.31.8-alpine3.24" {
		t.Errorf("RecentTags = %v", r.RecentTags)
	}
	if got := tagHits.Load(); got != 2 {
		t.Errorf("标签接口应调用 2 次（每个候选仓库各一次）, 实际 %d", got)
	}
}

// 普通搜索应带上各来源的仓库页面地址。
func TestSearchResultURL(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v2/search/repositories/") {
			io.WriteString(w, `{"count":2,"results":[
				{"repo_name":"library/nginx","is_official":true},
				{"repo_name":"linuxserver/nginx"}
			]}`)
			return
		}
		// 普通搜索默认不开标签补查，不会被请求到。
		http.NotFound(w, r)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger())
	resp := svc.Search(context.Background(), "nginx", 10)
	if len(resp.Results) != 2 {
		t.Fatalf("结果数 = %d", len(resp.Results))
	}
	if resp.Results[0].URL != "https://hub.docker.com/_/nginx" {
		t.Errorf("官方 URL = %q", resp.Results[0].URL)
	}
	if resp.Results[1].URL != "https://hub.docker.com/r/linuxserver/nginx" {
		t.Errorf("普通仓库 URL = %q", resp.Results[1].URL)
	}
}

// 滚动别名的组合形态（stable-alpine、mainline-perl）也不进悬浮列表。
func TestIsRollingTag(t *testing.T) {
	for _, tag := range []string{"latest", "stable", "stable-alpine3.24", "mainline-perl", "Nightly", "edge_2026"} {
		if !isRollingTag(tag) {
			t.Errorf("isRollingTag(%q) 应为 true", tag)
		}
	}
	for _, tag := range []string{"1.31.8", "1.31.8-alpine3.24", "v2.0", "alpine", "stablex"} {
		if isRollingTag(tag) {
			t.Errorf("isRollingTag(%q) 应为 false", tag)
		}
	}
}

// 错误分级：限流/鉴权走短冷却，5xx 走中等冷却，网络错误走长冷却。
// 关键回归：一次偶发 403 不能把来源静默下线五分钟。
func TestCooldownForErrorClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want time.Duration
	}{
		{"401", &statusError{code: http.StatusUnauthorized}, rateLimitCooldown},
		{"403", &statusError{code: http.StatusForbidden}, rateLimitCooldown},
		{"429", &statusError{code: http.StatusTooManyRequests}, rateLimitCooldown},
		{"403 带响应体", &statusError{code: http.StatusForbidden, msg: "rate limit exceeded"}, rateLimitCooldown},
		{"500", &statusError{code: http.StatusInternalServerError}, serverErrorCooldown},
		{"503", &statusError{code: http.StatusServiceUnavailable}, serverErrorCooldown},
		{"404 按默认", &statusError{code: http.StatusNotFound}, sourceCooldown},
		{"网络错误", fmt.Errorf("dial tcp: timeout"), sourceCooldown},
		{"包装后的 403", fmt.Errorf("fetch: %w", &statusError{code: http.StatusForbidden}), rateLimitCooldown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := cooldownFor(c.err); got != c.want {
				t.Errorf("cooldownFor = %v, 期望 %v", got, c.want)
			}
		})
	}
}

// statusError 把上游响应片段带进错误信息，便于从日志判断 403/429 的原因。
func TestStatusErrorMessageIncludesBody(t *testing.T) {
	err := &statusError{code: http.StatusTooManyRequests, msg: "you have exceeded your rate limit"}
	got := err.Error()
	if !strings.Contains(got, "429") || !strings.Contains(got, "rate limit") {
		t.Errorf("错误信息 = %q，应包含状态码与响应片段", got)
	}
}

// markDown 后应按错误类型使用各自的冷却时长；短冷却到期后来源恢复可查。
func TestCooldownUsesPerErrorDuration(t *testing.T) {
	svc := New(nil, time.Second, []string{"hub"}, false, discardLogger())
	svc.markDown("hub", &statusError{code: http.StatusForbidden})
	if !svc.coolingDown("hub") {
		t.Fatal("403 后应进入冷却")
	}
	if got := svc.downCool["hub"]; got != rateLimitCooldown {
		t.Fatalf("403 冷却时长 = %v, 期望 %v", got, rateLimitCooldown)
	}
	// 把失败时间往前挪到刚好超过短冷却，模拟 30 秒后重试。
	svc.mu.Lock()
	svc.down["hub"] = time.Now().Add(-rateLimitCooldown - time.Second)
	svc.mu.Unlock()
	if svc.coolingDown("hub") {
		t.Fatal("短冷却到期后不应再跳过该来源")
	}
}

// 启用账号认证后：先换 token，Hub 搜索/标签请求都带上 Authorization。
func TestHubAuthAttachesBearerToken(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	var loginHits, searchAuthHits, tagAuthHits int
	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginHits++
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("登录请求体解析失败: %v", err)
		}
		if body.Username != "alice" || body.Password != "pat-token" {
			t.Errorf("登录凭据不对: %+v", body)
		}
		io.WriteString(w, `{"token":"jwt-token-abc"}`)
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got == "Bearer jwt-token-abc" {
			if strings.Contains(r.URL.Path, "/tags/") {
				tagAuthHits++
			} else {
				searchAuthHits++
			}
		}
		io.WriteString(w, `{"count":1,"results":[{"repo_name":"library/nginx","short_description":"x","star_count":1,"pull_count":1}]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()),
		WithHubAuth("alice", "pat-token"),
		WithTags(true),
	)
	// 标签接口在真实实现里指向 hubTagURL，stubSources 已一并改到 hub。
	resp := svc.Search(context.Background(), "nginx", 5)
	if len(resp.Results) != 1 || resp.Errors != nil {
		t.Fatalf("搜索失败: results=%+v errors=%v", resp.Results, resp.Errors)
	}
	if loginHits != 1 {
		t.Errorf("登录次数 = %d, 期望 1", loginHits)
	}
	if searchAuthHits != 1 {
		t.Errorf("带 token 的搜索请求数 = %d, 期望 1", searchAuthHits)
	}
	if tagAuthHits < 1 {
		t.Errorf("带 token 的标签请求数 = %d, 期望 >=1", tagAuthHits)
	}
}

// token 在多次搜索间复用，不重复登录。
func TestHubAuthTokenCached(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	var loginHits int
	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginHits++
		io.WriteString(w, `{"token":"cached-token"}`)
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"count":0,"results":[]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()), WithHubAuth("alice", "pat"))
	for i := 0; i < 3; i++ {
		svc.Search(context.Background(), "nginx", 5)
	}
	if loginHits != 1 {
		t.Errorf("三次搜索后登录次数 = %d, 期望 1（token 应缓存）", loginHits)
	}
}

// 换 token 失败时降级匿名：搜索照常发起、不报错。
func TestHubAuthFailureFallsBackToAnonymous(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"message":"bad credentials"}`)
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	var authHeaders []string
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		io.WriteString(w, `{"count":1,"results":[{"repo_name":"library/nginx","short_description":"x","star_count":1,"pull_count":1}]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()), WithHubAuth("alice", "wrong"))
	resp := svc.Search(context.Background(), "nginx", 5)
	if len(resp.Results) != 1 || resp.Errors != nil {
		t.Fatalf("认证失败不应影响搜索: results=%d errors=%v", len(resp.Results), resp.Errors)
	}
	for _, h := range authHeaders {
		if h != "" {
			t.Errorf("降级匿名后不应带 Authorization, got %q", h)
		}
	}
}

// Hub 用缓存的 token 被明确拒绝（401）时应清缓存重登录，
// 而不是带着失效 token 继续请求最长 30 分钟。
func TestHubAuthInvalidatesOnUnauthorized(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	var loginHits int
	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginHits++
		io.WriteString(w, `{"token":"revoked-soon"}`)
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"message":"token revoked"}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()), WithHubAuth("alice", "pat"))

	svc.Search(context.Background(), "nginx", 5) // 登录 1 次，hub 请求 401
	if loginHits != 1 {
		t.Fatalf("第一次搜索登录次数 = %d, 期望 1", loginHits)
	}
	// 手动把限流冷却拨到已过期，模拟 30 秒后的下一次搜索。
	svc.mu.Lock()
	svc.down["hub"] = time.Now().Add(-rateLimitCooldown - time.Second)
	svc.mu.Unlock()

	svc.Search(context.Background(), "nginx", 5) // token 已失效，应重新登录
	if loginHits != 2 {
		t.Errorf("第二次搜索登录次数 = %d, 期望 2（401 后应清缓存重登录）", loginHits)
	}
}

// 缓存的 token 失效时（Hub 返回 401），应换一次新 token 重试，
// 而不是让这次搜索直接缺 Hub 结果、并把 Hub 打进 30 秒冷却。
func TestHubAuthRetriesOnceAfterTokenRejected(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	var loginHits atomic.Int64
	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := loginHits.Add(1)
		io.WriteString(w, fmt.Sprintf(`{"token":"token-%d"}`, n))
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	var rejectFirst atomic.Bool
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tags/") {
			io.WriteString(w, `{"results":[]}`)
			return
		}
		if rejectFirst.Load() && r.Header.Get("Authorization") == "Bearer token-1" {
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"detail":"token expired"}`)
			return
		}
		io.WriteString(w, `{"count":1,"results":[{"repo_name":"library/nginx","short_description":"x","star_count":1,"pull_count":1}]}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()), WithHubAuth("alice", "pat"))

	// 第一次搜索：登录拿到 token-1，Hub 正常返回。
	resp := svc.Search(context.Background(), "nginx", 5)
	if resp.Errors != nil || len(resp.Results) != 1 {
		t.Fatalf("第一次搜索失败: errors=%v results=%d", resp.Errors, len(resp.Results))
	}
	// 之后 token-1 被 Hub 拒绝（过期/吊销），缓存的 token 还在。
	rejectFirst.Store(true)
	resp = svc.Search(context.Background(), "nginx", 5)
	if resp.Errors != nil || len(resp.Results) != 1 {
		t.Fatalf("401 后应换新 token 重试成功: errors=%v results=%d", resp.Errors, len(resp.Results))
	}
	if got := loginHits.Load(); got != 2 {
		t.Errorf("登录次数 = %d, 期望 2（首次 + 401 后重登）", got)
	}
	if svc.coolingDown("hub") {
		t.Error("重试成功后 Hub 不应进入冷却")
	}
}

// 刚换到的新 token 仍被拒绝，说明凭据或账号有问题，不该无限重登。
func TestHubAuthDoesNotRetryFreshToken(t *testing.T) {
	oldLogin := hubLoginURL
	t.Cleanup(func() { hubLoginURL = oldLogin })

	var loginHits atomic.Int64
	login := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginHits.Add(1)
		io.WriteString(w, `{"token":"fresh-token"}`)
	}))
	defer login.Close()
	hubLoginURL = login.URL + "/v2/users/login/"

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"detail":"account disabled"}`)
	}))
	defer hub.Close()
	stubSources(t, hub, nil)

	svc := New(nil, 3*time.Second, []string{"hub"}, false, discardLogger(),
		WithHubClient(hub.Client()), WithHubAuth("alice", "pat"))
	svc.Search(context.Background(), "nginx", 5)
	if got := loginHits.Load(); got != 1 {
		t.Errorf("刚换的 token 被拒后登录次数 = %d, 期望 1（不重复重登）", got)
	}
	if !svc.coolingDown("hub") {
		t.Error("凭据无效时 Hub 应进入冷却，避免每次搜索都重登")
	}
}
