package search

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	var tagHits int
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
			tagHits++
			io.WriteString(w, `{"results":[
				{"name":"latest","last_updated":"2026-11-13T13:52:24Z"},
				{"name":"1.31.8-alpine3.24","last_updated":"2026-11-13T13:52:24Z"},
				{"name":"1.31.8","last_updated":"2026-11-12T09:00:00Z"}
			]}`)
		case strings.HasPrefix(r.URL.Path, "/v2/repositories/other/nginx/tags/"):
			tagHits++
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
	if tagHits != 2 {
		t.Errorf("标签接口应调用 2 次（每个候选仓库各一次）, 实际 %d", tagHits)
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
