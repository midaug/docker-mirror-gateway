package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// Docker / OCI 相关的媒体类型。
const (
	MediaTypeDockerManifest     = "application/vnd.docker.distribution.manifest.v2+json"
	MediaTypeDockerManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaTypeDockerSchema1      = "application/vnd.docker.distribution.manifest.v1+prettyjws"
	MediaTypeOCIManifest        = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeOCIIndex           = "application/vnd.oci.image.index.v1+json"
	MediaTypeOctetStream        = "application/octet-stream"
)

// DefaultManifestAccept 是客户端未携带 Accept 时向上游声明的媒体类型列表。
var DefaultManifestAccept = strings.Join([]string{
	MediaTypeDockerManifestList,
	MediaTypeOCIIndex,
	MediaTypeDockerManifest,
	MediaTypeOCIManifest,
	MediaTypeDockerSchema1,
}, ", ")

// PathKind 表示 /v2/ 路径的类型。
type PathKind int

const (
	PathUnknown PathKind = iota
	PathManifest
	PathBlob
	PathTags
	PathUpload
)

var (
	tagRe      = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)
	repoPartRe = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
)

// ParseV2Path 解析 /v2/ 之后的路径。
func ParseV2Path(rest string) (kind PathKind, name, ref string) {
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		return PathUnknown, "", ""
	}
	if strings.HasSuffix(rest, "/tags/list") {
		return PathTags, strings.TrimSuffix(rest, "/tags/list"), ""
	}
	if i := strings.LastIndex(rest, "/manifests/"); i > 0 {
		return PathManifest, rest[:i], rest[i+len("/manifests/"):]
	}
	if i := strings.LastIndex(rest, "/blobs/uploads/"); i > 0 {
		return PathUpload, rest[:i], ""
	}
	if i := strings.LastIndex(rest, "/blobs/"); i > 0 {
		return PathBlob, rest[:i], rest[i+len("/blobs/"):]
	}
	return PathUnknown, "", ""
}

// ValidRepositoryName 校验仓库名（不含 registry 前缀）。
func ValidRepositoryName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.Contains(name, "//") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "." || part == ".." || !repoPartRe.MatchString(part) {
			return false
		}
	}
	return true
}

// IsDigest 判断引用是否形如 sha256:<64 hex>。
func IsDigest(ref string) bool {
	alg, h, ok := strings.Cut(ref, ":")
	if !ok {
		return false
	}
	if alg != "sha256" || len(h) != 64 {
		return false
	}
	for _, r := range h {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// ValidReference 校验 tag 或 digest 形式的引用。
func ValidReference(ref string) bool {
	if IsDigest(ref) {
		return true
	}
	return tagRe.MatchString(ref)
}

// NormalizeRepositoryName 为单段名称补上官方命名空间（如 nginx -> library/nginx）。
func NormalizeRepositoryName(name, libraryNamespace string) string {
	if libraryNamespace != "" && !strings.Contains(name, "/") {
		return libraryNamespace + "/" + name
	}
	return name
}

// AcceptClass 把 Accept 头归类，用于区分 manifest 列表与单平台 manifest 的缓存。
func AcceptClass(accept string) string {
	accept = strings.TrimSpace(accept)
	if accept == "" {
		return "any"
	}
	if strings.Contains(accept, "manifest.list") || strings.Contains(accept, "image.index") {
		return "multi"
	}
	return "single"
}

// GuessManifestMediaType 在缺少 Content-Type 时根据内容推断媒体类型。
func GuessManifestMediaType(body []byte) string {
	var probe struct {
		MediaType     string            `json:"mediaType"`
		SchemaVersion int               `json:"schemaVersion"`
		Manifests     []json.RawMessage `json:"manifests"`
		Config        json.RawMessage   `json:"config"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return MediaTypeDockerManifest
	}
	if probe.MediaType != "" {
		return probe.MediaType
	}
	switch {
	case len(probe.Manifests) > 0:
		return MediaTypeOCIIndex
	case probe.SchemaVersion == 1:
		return MediaTypeDockerSchema1
	default:
		return MediaTypeDockerManifest
	}
}

// registryErrorBody 是 Docker Registry 规范定义的错误响应。
type registryErrorBody struct {
	Errors []registryError `json:"errors"`
}

type registryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  any    `json:"detail,omitempty"`
}

// WriteError 以 Docker Registry 规范格式输出错误响应。
func WriteError(w http.ResponseWriter, status int, code, message string, detail any) {
	writeRegistryError(w, status, code, message, detail)
}

func writeRegistryError(w http.ResponseWriter, status int, code, message string, detail any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(registryErrorBody{Errors: []registryError{{Code: code, Message: message, Detail: detail}}})
}

func writeMethodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeRegistryError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "docker-mirror-gateway 仅支持只读拉取", nil)
}

func manifestPath(name, ref string) string {
	return fmt.Sprintf("/v2/%s/manifests/%s", name, ref)
}

func blobPath(name, digest string) string {
	return fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
}

func tagsPath(name string) string {
	return fmt.Sprintf("/v2/%s/tags/list", name)
}
