package registry

import "testing"

func TestParseV2Path(t *testing.T) {
	tests := []struct {
		rest string
		kind PathKind
		name string
		ref  string
	}{
		{"library/nginx/manifests/latest", PathManifest, "library/nginx", "latest"},
		{"nginx/manifests/1.25", PathManifest, "nginx", "1.25"},
		{"library/nginx/blobs/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", PathBlob,
			"library/nginx", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{"library/nginx/blobs/uploads/", PathUpload, "library/nginx", ""},
		{"library/nginx/tags/list", PathTags, "library/nginx", ""},
		{"_catalog", PathUnknown, "", ""},
		{"", PathUnknown, "", ""},
	}
	for _, tt := range tests {
		kind, name, ref := ParseV2Path(tt.rest)
		if kind != tt.kind || name != tt.name || ref != tt.ref {
			t.Errorf("ParseV2Path(%q) = (%v,%q,%q), 期望 (%v,%q,%q)", tt.rest, kind, name, ref, tt.kind, tt.name, tt.ref)
		}
	}
}

func TestAcceptClass(t *testing.T) {
	cases := map[string]string{
		"": "any",
		"application/vnd.docker.distribution.manifest.v2+json":      "single",
		"application/vnd.oci.image.manifest.v1+json":                "single",
		"application/vnd.docker.distribution.manifest.list.v2+json": "multi",
		"application/vnd.oci.image.index.v1+json":                   "multi",
	}
	for accept, want := range cases {
		if got := AcceptClass(accept); got != want {
			t.Errorf("AcceptClass(%q) = %q, 期望 %q", accept, got, want)
		}
	}
}

func TestValidRepositoryName(t *testing.T) {
	valid := []string{"nginx", "library/nginx", "my-org/app_v2", "a.b/c.d", "team/sub/app"}
	for _, name := range valid {
		if !ValidRepositoryName(name) {
			t.Errorf("%q 应被视为合法仓库名", name)
		}
	}
	invalid := []string{"", "/nginx", "nginx/", "a//b", "../etc", "a/../b", "na me", "UPPER/x_y.z"}
	for _, name := range invalid {
		if ValidRepositoryName(name) {
			t.Errorf("%q 应被视为非法仓库名", name)
		}
	}
}

func TestValidReference(t *testing.T) {
	if !ValidReference("latest") || !ValidReference("v1.2.3-rc.1") {
		t.Error("合法 tag 被拒绝")
	}
	if ValidReference("") || ValidReference("bad tag") || ValidReference("sha256:xyz") {
		t.Error("非法引用被接受")
	}
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !IsDigest(digest) || !ValidReference(digest) {
		t.Error("合法 digest 被拒绝")
	}
}

func TestNormalizeRepositoryName(t *testing.T) {
	if got := NormalizeRepositoryName("nginx", "library"); got != "library/nginx" {
		t.Errorf("got %q", got)
	}
	if got := NormalizeRepositoryName("library/nginx", "library"); got != "library/nginx" {
		t.Errorf("got %q", got)
	}
	if got := NormalizeRepositoryName("nginx", ""); got != "nginx" {
		t.Errorf("got %q", got)
	}
}

func TestParseChallenge(t *testing.T) {
	h := `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"`
	c := parseChallenge(h)
	if c.Scheme != "bearer" {
		t.Errorf("Scheme = %q", c.Scheme)
	}
	if c.Realm != "https://auth.docker.io/token" {
		t.Errorf("Realm = %q", c.Realm)
	}
	if c.Service != "registry.docker.io" {
		t.Errorf("Service = %q", c.Service)
	}
	if c.Scope != "repository:library/nginx:pull" {
		t.Errorf("Scope = %q", c.Scope)
	}

	basic := parseChallenge(`Basic realm="Registry"`)
	if basic.Scheme != "basic" || basic.Realm != "Registry" {
		t.Errorf("Basic 解析错误: %+v", basic)
	}

	if c := parseChallenge(""); c.Scheme != "" {
		t.Errorf("空头解析错误: %+v", c)
	}
}

func TestGuessManifestMediaType(t *testing.T) {
	if got := GuessManifestMediaType([]byte(`{"mediaType":"application/vnd.oci.image.index.v1+json"}`)); got != MediaTypeOCIIndex {
		t.Errorf("got %q", got)
	}
	if got := GuessManifestMediaType([]byte(`{"manifests":[{"digest":"sha256:x"}]}`)); got != MediaTypeOCIIndex {
		t.Errorf("got %q", got)
	}
	if got := GuessManifestMediaType([]byte(`{"config":{}}`)); got != MediaTypeDockerManifest {
		t.Errorf("got %q", got)
	}
}
