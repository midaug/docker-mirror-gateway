// Package config 负责 docker-mirror-gateway 的配置加载、默认值与校验。
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 支持两种 YAML 写法：字符串（"30s"、"5m"）或数字（单位：秒）。
type Duration time.Duration

// D 转换为标准库 time.Duration。
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	// 数字写法（timeout: 30）按秒解释。yaml.v3 会把 !!int/!!float 节点
	// decode 成任意标量（包括 string），所以必须先看 Tag 分派，
	// 否则纯数字会掉进字符串分支并报错。
	if node.Tag == "!!int" || node.Tag == "!!float" {
		var secs float64
		if err := node.Decode(&secs); err != nil {
			return fmt.Errorf("无法解析时长 %q: %w", node.Value, err)
		}
		*d = Duration(time.Duration(secs * float64(time.Second)))
		return nil
	}
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("时长字段仅支持字符串或数字: %w", err)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("无法解析时长 %q（示例：30s、5m、1h）: %w", raw, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Config 是 docker-mirror-gateway 的完整配置。
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Cache     CacheConfig     `yaml:"cache"`
	Access    AccessConfig    `yaml:"access"`
	Search    SearchConfig    `yaml:"search"`
	Health    HealthConfig    `yaml:"health"`
	Selection SelectionConfig `yaml:"selection"`
	Routing   RoutingConfig   `yaml:"routing"`
	Notify    NotifyConfig    `yaml:"notify"`
	// Group 是单分组写法（兼容旧配置）；与 Groups 二选一。
	Group  GroupConfig   `yaml:"group"`
	Groups []GroupConfig `yaml:"groups"`
	Log    LogConfig     `yaml:"log"`

	// Path 记录实际加载的配置文件路径，仅用于日志。
	Path string `yaml:"-"`
}

// NotifyConfig 是监控通知的全局配置。默认关闭：enabled 为 true 且至少
// 配置一个通道（webhook 或邮箱）才会发送。
type NotifyConfig struct {
	// Enabled 是通知总开关，默认 false。
	Enabled bool `yaml:"enabled"`
	// Interval 是同一分组持续异常时重复提醒的间隔，避免消息轰炸。
	// 状态变化（触发告警、恢复）始终立即通知，不受它限制。默认 30m。
	Interval Duration `yaml:"interval"`
	// Webhooks 是通知通道列表，可同时配置多个。
	Webhooks []WebhookConfig `yaml:"webhooks"`
	// Email 是 SMTP 邮件通道。
	Email EmailConfig `yaml:"email"`
}

// WebhookConfig 是一个通用 webhook 通知通道。
//
// 不绑定任何具体 IM 协议：URL 与 POST body 都是模板，支持变量替换
// （text/template 语法，如 {{.Group}}），因此飞书、企业微信或任何
// 自定义协议都能通过模板适配。
type WebhookConfig struct {
	// Method 是 HTTP 方法：GET 或 POST。GET 只请求渲染后的 URL；
	// POST 额外携带渲染后的 body。默认 POST。
	Method string `yaml:"method"`
	// URL 是请求地址模板，GET/POST 都支持变量替换。
	URL string `yaml:"url"`
	// Body 是 POST 消息体模板，支持变量替换。留空时发送结构化 JSON
	// （与 API 字段一致：group/state/healthy/total/threshold/failing/time）。
	// GET 忽略该字段。
	Body string `yaml:"body"`
	// ContentType 是 POST 的 Content-Type。默认 application/json。
	ContentType string `yaml:"contentType"`
	// Timeout 是单次推送的超时。默认 10s。
	Timeout Duration `yaml:"timeout"`
}

// EmailConfig 是 SMTP 邮件通知通道。
type EmailConfig struct {
	Enabled bool `yaml:"enabled"`
	// Host 是 SMTP 服务器，例如 smtp.example.com。
	Host string `yaml:"host"`
	// Port 端口：465 走隐式 TLS，587/25 走 STARTTLS（服务器不支持时明文）。
	// 默认 465。
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	// From 是发件人地址；留空时使用 username。
	From string `yaml:"from"`
	// To 是收件人列表，至少一个。
	To []string `yaml:"to"`
	// Timeout 是单次投递的超时。默认 15s。
	Timeout Duration `yaml:"timeout"`
}

// RoutingConfig 控制「请求如何落到某个分组」。
type RoutingConfig struct {
	// AutoDetect 在没有域名/路径前缀线索时，并发探测各分组，自动判断仓库归属。
	AutoDetect bool `yaml:"autoDetect"`
	// PathAliases 启用仓库名首段的别名路由（如 /ghcr.io/owner/repo、/quay/org/repo）。
	PathAliases bool `yaml:"pathAliases"`
	// Strategy 决定自动探测的胜出规则：priority（分组优先级高者胜）或 fastest（最快响应者胜）。
	Strategy string `yaml:"strategy"`
	// DefaultGroup 是自动探测关闭时的兜底分组；留空则使用标记 default: true 的分组。
	DefaultGroup string `yaml:"defaultGroup"`
	// DetectTimeout 是单次自动探测的总超时。
	DetectTimeout Duration `yaml:"detectTimeout"`
	// RouteTTL 是「仓库 -> 分组」判定结果的缓存时长。
	RouteTTL Duration `yaml:"routeTTL"`
	// NegativeTTL 是「仓库不存在」的负缓存时长，避免反复探测。
	NegativeTTL Duration `yaml:"negativeTTL"`
}

// GroupConfig 是一组共享同一上游列表的仓库命名空间。
//
// 一个分组通常对应一个公共镜像仓库（Docker Hub、GHCR、Quay……），
// 组内可以配置多个镜像加速地址，它们互为镜像。
type GroupConfig struct {
	Name     string `yaml:"name"`
	Priority int    `yaml:"priority"`
	// Hosts 是访问本服务时使用的域名，例如 ghcr.mirror.example.com。
	Hosts []string `yaml:"hosts"`
	// Aliases 是仓库路径首段的别名，例如 ghcr.io、ghcr、quay。
	Aliases []string `yaml:"aliases"`
	// LibraryNamespace 为单段仓库名补全命名空间，Docker Hub 填 library。
	LibraryNamespace string `yaml:"libraryNamespace"`
	// ProbeRepo / ProbeTag 是健康探测用的真实镜像；留空则只探测 /v2/。
	ProbeRepo string `yaml:"probeRepo"`
	ProbeTag  string `yaml:"probeTag"`
	// Default 标记兜底分组（自动路由关闭或无法判定时使用）。
	Default bool `yaml:"default"`
	// Notify 是该分组的监控通知：健康镜像源数量跌破阈值时发送。
	// 需要全局 notify.enabled 为 true 才会真正发送。
	Notify GroupNotifyConfig `yaml:"notify"`
	// Upstreams 是组内的镜像源，priority 数值越小越优先。
	Upstreams []UpstreamConfig `yaml:"upstreams"`
}

// GroupNotifyConfig 是分组级监控通知。
type GroupNotifyConfig struct {
	// Enabled 在该分组启用监控通知（还要满足 notify.enabled）。
	Enabled bool `yaml:"enabled"`
	// MinHealthy 是通知阈值：健康镜像源少于该数量时告警，恢复后同样通知。
	// 默认 0 表示不通知。
	MinHealthy int `yaml:"minHealthy"`
}

// 上游认证方式。默认 config：使用本源配置的 username/password 自动完成
// 上游认证；passthrough：把客户端 docker login 的凭证原样用于该源。
const (
	UpstreamAuthConfig      = "config"
	UpstreamAuthPassthrough = "passthrough"
)

type UpstreamConfig struct {
	Name     string `yaml:"name"`
	URL      string `yaml:"url"`
	Priority int    `yaml:"priority"`
	// Auth 是该源的认证方式：config（默认）或 passthrough。
	// 认证方式严格属于单个源，不按分组或域名推断。
	Auth     string            `yaml:"auth"`
	Username string            `yaml:"username"`
	Password string            `yaml:"password"`
	Timeout  Duration          `yaml:"timeout"`
	Headers  map[string]string `yaml:"headers"`
	// Proxy 是该镜像源专用的访问代理，支持 http/https/socks5/socks5h；
	// 留空表示沿用环境变量（HTTP_PROXY / HTTPS_PROXY / NO_PROXY），
	// 填 direct 表示强制直连、忽略环境变量。
	// 凭据可以直接写在地址里（http://user:pass@host:port），
	// 也可以用下面的 proxyUsername / proxyPassword 单独配置。
	Proxy string `yaml:"proxy"`
	// ProxyUsername / ProxyPassword 是代理的认证信息。
	// 代理地址里已经带凭据时不要再填这两项。
	ProxyUsername string `yaml:"proxyUsername"`
	ProxyPassword string `yaml:"proxyPassword"`
}

// Passthrough 报告该源是否使用客户端凭证透传。
func (u UpstreamConfig) Passthrough() bool { return u.Auth == UpstreamAuthPassthrough }

type ServerConfig struct {
	// Addr 是镜像拉取端口（/v2/*）。
	Addr string `yaml:"addr"`
	// AdminAddr 是控制台与管理接口的独立监听地址，例如 ":8081" 或 "127.0.0.1:8081"。
	// 留空表示与 Addr 共用同一个端口。
	AdminAddr string `yaml:"adminAddr"`
	// PublicURL 是客户端实际访问镜像服务的地址，用于控制台生成接入示例；
	// 留空则按 Addr 的端口 + 当前页面域名推导。
	PublicURL         string   `yaml:"publicURL"`
	AdminToken        string   `yaml:"adminToken"`
	ReadHeaderTimeout Duration `yaml:"readHeaderTimeout"`
	IdleTimeout       Duration `yaml:"idleTimeout"`
	MaxManifestBytes  int64    `yaml:"maxManifestBytes"`
}

type CacheConfig struct {
	// Enabled 是缓存总开关，默认关闭：个人部署通常不需要额外占一块磁盘。
	// 开启后按 Backend 选择落到本地磁盘还是 S3 兼容对象存储。
	Enabled        bool     `yaml:"enabled"`
	Backend        string   `yaml:"backend"`
	Dir            string   `yaml:"dir"`
	MaxSizeGB      float64  `yaml:"maxSizeGB"`
	TagTTL         Duration `yaml:"tagTTL"`
	GCInterval     Duration `yaml:"gcInterval"`
	ManifestMaxAge Duration `yaml:"manifestMaxAge"`
	S3             S3Config `yaml:"s3"`
}

// S3Config 是 S3 兼容对象存储（MinIO / 阿里云 OSS / 腾讯云 COS / 华为云 OBS）的设置。
type S3Config struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"accessKey"`
	SecretKey string `yaml:"secretKey"`
	Region    string `yaml:"region"`
	Prefix    string `yaml:"prefix"`
	// UseSSL 默认 true；自建 MinIO 走明文时才需要显式关掉。
	UseSSL *bool `yaml:"useSSL"`
	// PathStyle 用 path-style 寻址，自建 MinIO 一般需要开启。
	PathStyle *bool  `yaml:"pathStyle"`
	TmpDir    string `yaml:"tmpDir"`
}

// AccessConfig 控制谁能通过本服务拉取镜像。
//
// 只作用于镜像拉取面；管理端口由 adminToken 与端口分离保护，
// 这里不做限制，避免把自己锁在门外。
type AccessConfig struct {
	// Allow 是允许的来源 IP 或 CIDR。为空表示不限制。
	Allow []string `yaml:"allow"`
	// Deny 是明确拒绝的来源，优先级高于 Allow。
	Deny []string `yaml:"deny"`
	// TrustForwardedFor 表示信任 X-Forwarded-For。
	// 只有前面确实挂了反向代理时才开启，否则客户端可以伪造。
	TrustForwardedFor bool `yaml:"trustForwardedFor"`
}

// SearchConfig 控制控制台里的镜像搜索。
type SearchConfig struct {
	Enabled bool `yaml:"enabled"`
	// Sources 指定搜索哪些仓库：hub / quay / mcr。默认三个都开。
	// 其余仓库（GHCR、GCR、registry.k8s.io、Elastic、NVCR）没有匿名的公共搜索接口，
	// 只能按已知名字拉取，无法按关键字搜索。
	Sources []string `yaml:"sources"`
	Timeout Duration `yaml:"timeout"`
	// EnrichUpdated 表示是否额外查询仓库的最近更新时间。
	// 搜索接口大多不返回时间，需要按仓库补查标签列表；关掉可以少发一些请求。
	EnrichUpdated *bool `yaml:"enrichUpdated"`
	// EnrichTags 表示是否补查「除 latest 外的最新版本号」。
	// 和 EnrichUpdated 共用同一次标签查询，两者都关才完全不发请求。
	EnrichTags *bool `yaml:"enrichTags"`
}

type HealthConfig struct {
	Interval      Duration `yaml:"interval"`
	Timeout       Duration `yaml:"timeout"`
	FailThreshold int      `yaml:"failThreshold"`
	// TrafficWindow 是「最近有真实拉取就跳过主动探测」的窗口。
	// 窗口内该上游处理过成功的真实业务流量（manifest/blob/tags），
	// 延迟与吞吐已有真实样本，健康状态也由真实流量的成败在维护，
	// 定时探测无需再发请求。默认 2m。控制台手动探测不受它影响。
	TrafficWindow Duration `yaml:"trafficWindow"`
	// ProbeRepo / ProbeTag 仅作为单分组（group:）配置的默认探针。
	ProbeRepo string `yaml:"probeRepo"`
	ProbeTag  string `yaml:"probeTag"`
}

type SelectionConfig struct {
	// Strategy 决定组内多个镜像源怎么排：
	//   speed    —— 按实测下载吞吐排（默认）。延迟只在吞吐接近时才参与比较。
	//   priority —— 完全按配置里的 priority。
	//   latency  —— 按实测延迟排。
	Strategy string `yaml:"strategy"`
	// SpeedProbeBytes 是吞吐探测每次下载的字节数。太小测不准（握手占比高），
	// 太大会浪费流量。
	SpeedProbeBytes int64 `yaml:"speedProbeBytes"`
	// SpeedProbeInterval 是「给每个上游重新测一次吞吐」的间隔。
	// 真实拉取产生的数据也会持续更新吞吐，这里主要用于给冷门源刷新数据。
	SpeedProbeInterval Duration `yaml:"speedProbeInterval"`
	// SpeedTolerance 是吞吐相差多少以内算持平（0.25 = 25%），
	// 持平的时候才用延迟、priority 做次级排序。设大一点可以避免频繁切换。
	SpeedTolerance float64 `yaml:"speedTolerance"`
	// PreferFresh 表示对 latest 这类可变标签先比较各源上的镜像新旧：
	// 构建时间更新的源优先，一样新时再按 Strategy 排序。
	// 镜像源同步有延迟时，快的源未必有最新的 latest，这个开关用于纠正。
	PreferFresh bool `yaml:"preferFresh"`
	// FreshTags 是参与新旧比较的标签，默认只有 latest。
	// 版本号标签（如 1.27）在各镜像之间通常一致，没必要为它们多花探测请求。
	FreshTags []string `yaml:"freshTags"`
	// FreshnessTolerance 是新旧比较的容差：构建时间相差小于该值时视为一样新，
	// 继续按 Strategy 选源。默认 0 表示严格优先更新的那个。
	// 如果不想为了几分钟的差异牺牲速度，可以设成 1h 之类。
	FreshnessTolerance Duration `yaml:"freshnessTolerance"`
	// FreshnessTTL 是「仓库@标签」新旧探测结果的缓存时长。
	FreshnessTTL Duration `yaml:"freshnessTTL"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default 返回开箱即用的默认配置（仅官方 Docker Hub）。
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:              ":8080",
			ReadHeaderTimeout: Duration(10 * time.Second),
			IdleTimeout:       Duration(120 * time.Second),
			MaxManifestBytes:  8 << 20,
		},
		Cache: CacheConfig{
			Enabled:        false,
			Backend:        "disk",
			Dir:            "./data/cache",
			MaxSizeGB:      50,
			TagTTL:         Duration(5 * time.Minute),
			GCInterval:     Duration(10 * time.Minute),
			ManifestMaxAge: Duration(30 * 24 * time.Hour),
		},
		Search: SearchConfig{
			Enabled: true,
			Sources: []string{"hub", "quay", "mcr"},
			Timeout: Duration(8 * time.Second),
		},
		Health: HealthConfig{
			Interval:      Duration(60 * time.Second),
			Timeout:       Duration(10 * time.Second),
			FailThreshold: 3,
			TrafficWindow: Duration(2 * time.Minute),
			ProbeRepo:     "library/alpine",
			ProbeTag:      "latest",
		},
		Selection: SelectionConfig{
			Strategy:           "speed",
			SpeedProbeBytes:    256 << 10,
			SpeedProbeInterval: Duration(30 * time.Minute),
			SpeedTolerance:     0.25,
			PreferFresh:        true,
			FreshTags:          []string{"latest"},
			FreshnessTTL:       Duration(10 * time.Minute),
		},
		Routing: RoutingConfig{
			AutoDetect:    true,
			PathAliases:   true,
			Strategy:      "priority",
			DetectTimeout: Duration(5 * time.Second),
			RouteTTL:      Duration(30 * time.Minute),
			NegativeTTL:   Duration(2 * time.Minute),
		},
		Group: GroupConfig{
			Name:             "dockerhub",
			Priority:         100,
			Default:          true,
			LibraryNamespace: "library",
			ProbeRepo:        "library/alpine",
			ProbeTag:         "latest",
			Upstreams: []UpstreamConfig{{
				Name:     "dockerhub",
				URL:      "https://registry-1.docker.io",
				Priority: 100,
			}},
		},
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

// Load 读取配置文件。path 为空时依次尝试 ./config.yaml、/etc/docker-mirror-gateway/config.yaml，
// 都不存在则使用默认配置。
func Load(path string) (*Config, error) {
	cfg := Default()

	explicit := strings.TrimSpace(path) != ""
	if !explicit {
		if p := os.Getenv("MIRROR_GATEWAY_CONFIG"); p != "" {
			path, explicit = p, true
		}
	}
	if !explicit {
		for _, candidate := range []string{"config.yaml", "/etc/docker-mirror-gateway/config.yaml"} {
			if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
				path = candidate
				break
			}
		}
	}

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			if !explicit && errors.Is(err, os.ErrNotExist) {
				// 找不到默认位置的配置时，退回内置默认值。
				path = ""
			} else {
				return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
			}
		} else {
			// 支持 ${ENV_VAR} 占位符，方便把凭据放进环境变量。
			expanded := os.ExpandEnv(string(raw))

			// 先探测用户写的是单分组 group 还是多分组 groups，两者不能混用。
			var keys struct {
				Group  yaml.Node `yaml:"group"`
				Groups yaml.Node `yaml:"groups"`
				Auth   yaml.Node `yaml:"auth"`
			}
			if err := yaml.Unmarshal([]byte(expanded), &keys); err != nil {
				return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
			}
			if keys.Auth.Kind != 0 {
				return nil, errors.New("全局 auth 配置已移除：请在每个 upstream 上设置 auth: config 或 auth: passthrough")
			}
			hasGroup := keys.Group.Kind != 0
			hasGroups := keys.Groups.Kind != 0
			if hasGroup && hasGroups {
				return nil, errors.New("配置中不能同时出现 group 与 groups，请改用 groups 多分组写法")
			}
			if hasGroups {
				// 丢弃内置的单分组默认值，避免默认的 dockerhub 混进来。
				cfg.Group = GroupConfig{}
			}

			if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
				return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
			}
			cfg.Path = path
		}
	}

	cfg.applyEnv()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("MIRROR_GATEWAY_ADDR"); v != "" {
		c.Server.Addr = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_ADMIN_ADDR"); v != "" {
		c.Server.AdminAddr = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_ADMIN_TOKEN"); v != "" {
		c.Server.AdminToken = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_CACHE_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.Cache.Enabled = b
		}
	}
	if v := os.Getenv("MIRROR_GATEWAY_CACHE_DIR"); v != "" {
		c.Cache.Dir = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_CACHE_MAX_GB"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.Cache.MaxSizeGB = f
		}
	}
	if v := os.Getenv("MIRROR_GATEWAY_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_LOG_FORMAT"); v != "" {
		c.Log.Format = v
	}
	if v := os.Getenv("MIRROR_GATEWAY_TAG_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Cache.TagTTL = Duration(d)
		}
	}
}

// Validate 校验并补全配置。
func (c *Config) Validate() error {
	c.Server.Addr = strings.TrimSpace(c.Server.Addr)
	if c.Server.Addr == "" {
		return errors.New("server.addr 不能为空")
	}
	c.Server.AdminAddr = strings.TrimSpace(c.Server.AdminAddr)
	if c.Server.AdminAddr == c.Server.Addr {
		// 写成同一个地址等价于不分离，避免启动两个抢同一端口的监听器。
		c.Server.AdminAddr = ""
	}
	if c.Server.PublicURL != "" {
		u, err := url.Parse(strings.TrimSpace(c.Server.PublicURL))
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("server.publicURL 必须是形如 https://mirror.example.com 的地址: %q", c.Server.PublicURL)
		}
		c.Server.PublicURL = strings.TrimRight(u.String(), "/")
	}
	if c.Server.MaxManifestBytes <= 0 {
		c.Server.MaxManifestBytes = 8 << 20
	}
	if c.Server.ReadHeaderTimeout.D() <= 0 {
		c.Server.ReadHeaderTimeout = Duration(10 * time.Second)
	}
	if c.Server.IdleTimeout.D() <= 0 {
		c.Server.IdleTimeout = Duration(120 * time.Second)
	}

	c.Cache.Backend = strings.ToLower(strings.TrimSpace(c.Cache.Backend))
	if c.Cache.Backend == "" {
		c.Cache.Backend = "disk"
	}
	switch c.Cache.Backend {
	case "disk":
		if c.Cache.Enabled && strings.TrimSpace(c.Cache.Dir) == "" {
			return errors.New("启用缓存时 cache.dir 不能为空")
		}
		if c.Cache.Enabled && c.Cache.MaxSizeGB <= 0 {
			return errors.New("启用磁盘缓存时 cache.maxSizeGB 必须大于 0")
		}
	case "s3":
		if c.Cache.Enabled {
			if strings.TrimSpace(c.Cache.S3.Endpoint) == "" {
				return errors.New("cache.backend=s3 时 cache.s3.endpoint 不能为空")
			}
			if strings.TrimSpace(c.Cache.S3.Bucket) == "" {
				return errors.New("cache.backend=s3 时 cache.s3.bucket 不能为空")
			}
			if c.Cache.MaxSizeGB < 0 {
				return errors.New("cache.maxSizeGB 不能为负数（0 表示不限制）")
			}
		}
	default:
		return fmt.Errorf("cache.backend 仅支持 disk 或 s3，当前为 %q", c.Cache.Backend)
	}
	if c.Cache.S3.UseSSL == nil {
		v := true
		c.Cache.S3.UseSSL = &v
	}
	if c.Cache.S3.PathStyle == nil {
		v := false
		c.Cache.S3.PathStyle = &v
	}
	if strings.TrimSpace(c.Cache.S3.Prefix) == "" {
		c.Cache.S3.Prefix = "docker-mirror-gateway"
	}
	if c.Cache.TagTTL.D() < 0 {
		return errors.New("cache.tagTTL 不能为负数")
	}
	if c.Cache.GCInterval.D() <= 0 {
		c.Cache.GCInterval = Duration(10 * time.Minute)
	}
	if c.Cache.ManifestMaxAge.D() <= 0 {
		c.Cache.ManifestMaxAge = Duration(30 * 24 * time.Hour)
	}

	if len(c.Search.Sources) == 0 {
		c.Search.Sources = []string{"hub", "quay", "mcr"}
	}
	seenSource := make(map[string]bool, len(c.Search.Sources))
	normalizedSources := make([]string, 0, len(c.Search.Sources))
	for _, src := range c.Search.Sources {
		src = strings.ToLower(strings.TrimSpace(src))
		switch src {
		case "hub", "quay", "mcr":
		default:
			return fmt.Errorf("search.sources 仅支持 hub、quay 或 mcr，当前为 %q", src)
		}
		if seenSource[src] {
			continue
		}
		seenSource[src] = true
		normalizedSources = append(normalizedSources, src)
	}
	c.Search.Sources = normalizedSources
	if c.Search.Timeout.D() <= 0 {
		c.Search.Timeout = Duration(8 * time.Second)
	}
	if c.Search.EnrichUpdated == nil {
		v := true
		c.Search.EnrichUpdated = &v
	}
	if c.Search.EnrichTags == nil {
		v := true
		c.Search.EnrichTags = &v
	}

	switch strings.ToLower(strings.TrimSpace(c.Selection.Strategy)) {
	case "", "speed":
		c.Selection.Strategy = "speed"
	case "priority":
		c.Selection.Strategy = "priority"
	case "latency":
		c.Selection.Strategy = "latency"
	default:
		return fmt.Errorf("selection.strategy 仅支持 speed、priority 或 latency，当前为 %q", c.Selection.Strategy)
	}

	if c.Selection.SpeedProbeBytes <= 0 {
		c.Selection.SpeedProbeBytes = 256 << 10
	}
	if c.Selection.SpeedProbeBytes < 32<<10 {
		return fmt.Errorf("selection.speedProbeBytes 不能小于 32768（32KB），当前为 %d", c.Selection.SpeedProbeBytes)
	}
	if c.Selection.SpeedProbeBytes > 8<<20 {
		return fmt.Errorf("selection.speedProbeBytes 不能大于 8388608（8MB），当前为 %d", c.Selection.SpeedProbeBytes)
	}
	if c.Selection.SpeedProbeInterval.D() <= 0 {
		c.Selection.SpeedProbeInterval = Duration(30 * time.Minute)
	}
	if c.Selection.SpeedProbeInterval.D() < time.Minute {
		return fmt.Errorf("selection.speedProbeInterval 不能小于 1m，当前为 %s", c.Selection.SpeedProbeInterval.D())
	}
	if c.Selection.SpeedTolerance < 0 || c.Selection.SpeedTolerance > 1 {
		return fmt.Errorf("selection.speedTolerance 必须在 0~1 之间，当前为 %v", c.Selection.SpeedTolerance)
	}
	if c.Selection.SpeedTolerance == 0 {
		c.Selection.SpeedTolerance = 0.25
	}

	// latest 的新旧比较。标签名统一小写，去重；不填就给默认的 latest。
	seenTag := make(map[string]struct{}, len(c.Selection.FreshTags))
	freshTags := make([]string, 0, len(c.Selection.FreshTags))
	for _, tag := range c.Selection.FreshTags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, dup := seenTag[tag]; dup {
			continue
		}
		seenTag[tag] = struct{}{}
		freshTags = append(freshTags, tag)
	}
	if len(freshTags) == 0 {
		freshTags = []string{"latest"}
	}
	c.Selection.FreshTags = freshTags
	if c.Selection.FreshnessTolerance.D() < 0 {
		return errors.New("selection.freshnessTolerance 不能为负数")
	}
	if c.Selection.FreshnessTTL.D() <= 0 {
		c.Selection.FreshnessTTL = Duration(10 * time.Minute)
	}
	if c.Selection.FreshnessTTL.D() < time.Minute {
		return fmt.Errorf("selection.freshnessTTL 不能小于 1m，当前为 %s", c.Selection.FreshnessTTL.D())
	}

	if c.Health.Interval.D() <= 0 {
		c.Health.Interval = Duration(60 * time.Second)
	}
	if c.Health.Timeout.D() <= 0 {
		c.Health.Timeout = Duration(10 * time.Second)
	}
	if c.Health.FailThreshold <= 0 {
		c.Health.FailThreshold = 3
	}
	if c.Health.TrafficWindow.D() <= 0 {
		c.Health.TrafficWindow = Duration(2 * time.Minute)
	}
	if c.Health.ProbeTag == "" {
		c.Health.ProbeTag = "latest"
	}

	if err := c.normalizeNotify(); err != nil {
		return err
	}

	if err := c.validateRouting(); err != nil {
		return err
	}
	if err := c.validateGroups(); err != nil {
		return err
	}

	switch strings.ToLower(strings.TrimSpace(c.Log.Level)) {
	case "", "debug", "info", "warn", "error":
		if c.Log.Level == "" {
			c.Log.Level = "info"
		}
	default:
		return fmt.Errorf("log.level 仅支持 debug/info/warn/error，当前为 %q", c.Log.Level)
	}
	switch strings.ToLower(strings.TrimSpace(c.Log.Format)) {
	case "", "text":
		c.Log.Format = "text"
	case "json":
		c.Log.Format = "json"
	default:
		return fmt.Errorf("log.format 仅支持 text 或 json，当前为 %q", c.Log.Format)
	}
	return nil
}

func (c *Config) validateRouting() error {
	switch strings.ToLower(strings.TrimSpace(c.Routing.Strategy)) {
	case "", "priority":
		c.Routing.Strategy = "priority"
	case "fastest":
		c.Routing.Strategy = "fastest"
	default:
		return fmt.Errorf("routing.strategy 仅支持 priority 或 fastest，当前为 %q", c.Routing.Strategy)
	}
	if c.Routing.DetectTimeout.D() <= 0 {
		c.Routing.DetectTimeout = Duration(5 * time.Second)
	}
	if c.Routing.RouteTTL.D() <= 0 {
		c.Routing.RouteTTL = Duration(30 * time.Minute)
	}
	if c.Routing.NegativeTTL.D() <= 0 {
		c.Routing.NegativeTTL = Duration(2 * time.Minute)
	}
	return nil
}

// normalizeNotify 补全通知配置的默认值并校验。
// 通知默认关闭：enabled=false 时其余字段不参与校验，允许只写占位。
func (c *Config) normalizeNotify() error {
	if c.Notify.Interval.D() <= 0 {
		c.Notify.Interval = Duration(30 * time.Minute)
	}
	if !c.Notify.Enabled {
		return nil
	}

	hasChannel := len(c.Notify.Webhooks) > 0 || c.Notify.Email.Enabled
	if !hasChannel {
		return errors.New("notify.enabled 为 true 时至少需要配置一个通道（notify.webhooks 或 notify.email）")
	}

	for i := range c.Notify.Webhooks {
		w := &c.Notify.Webhooks[i]
		w.Method = strings.ToUpper(strings.TrimSpace(w.Method))
		switch w.Method {
		case "", "POST":
			w.Method = "POST"
		case "GET":
		default:
			return fmt.Errorf("notify.webhooks[%d].method 仅支持 GET/POST，当前为 %q", i, w.Method)
		}
		if w.Method == "GET" {
			w.Body = "" // GET 无消息体，显式清空避免误用
		}
		w.URL = strings.TrimSpace(w.URL)
		if !strings.HasPrefix(w.URL, "http://") && !strings.HasPrefix(w.URL, "https://") {
			return fmt.Errorf("notify.webhooks[%d].url 必须是 http(s) 地址", i)
		}
		if w.Method == "POST" {
			w.ContentType = strings.TrimSpace(w.ContentType)
			if w.ContentType == "" {
				w.ContentType = "application/json"
			}
		}
		if w.Timeout.D() <= 0 {
			w.Timeout = Duration(10 * time.Second)
		}
	}

	e := &c.Notify.Email
	if e.Enabled {
		e.Host = strings.TrimSpace(e.Host)
		if e.Host == "" {
			return errors.New("notify.email.enabled 为 true 时 email.host 不能为空")
		}
		if e.Port <= 0 {
			e.Port = 465
		}
		e.Username = strings.TrimSpace(e.Username)
		if e.From == "" {
			e.From = e.Username
		}
		if e.From == "" {
			return errors.New("notify.email 缺少发件人（email.from 或 email.username）")
		}
		if len(e.To) == 0 {
			return errors.New("notify.email.enabled 为 true 时 email.to 至少需要一个收件人")
		}
		if e.Timeout.D() <= 0 {
			e.Timeout = Duration(15 * time.Second)
		}
	}
	return nil
}

func (c *Config) validateGroups() error {
	legacy := len(c.Groups) == 0
	if legacy {
		if len(c.Group.Upstreams) == 0 {
			return errors.New("groups 至少需要配置一个分组")
		}
		c.Groups = []GroupConfig{c.Group}
	}

	groupNames := make(map[string]struct{}, len(c.Groups))
	upstreamNames := make(map[string]string)
	defaults := 0

	for i := range c.Groups {
		g := &c.Groups[i]
		g.Name = strings.TrimSpace(g.Name)
		if g.Name == "" {
			g.Name = fmt.Sprintf("group-%d", i+1)
		}
		if _, dup := groupNames[g.Name]; dup {
			return fmt.Errorf("groups 中存在重复的分组名 %q", g.Name)
		}
		groupNames[g.Name] = struct{}{}

		if g.Priority <= 0 {
			g.Priority = 100 + i*10
		}
		if legacy && g.LibraryNamespace == "" {
			// 旧配置的单一分组默认就是 Docker Hub 语义。
			g.LibraryNamespace = "library"
		}
		g.LibraryNamespace = strings.Trim(strings.TrimSpace(g.LibraryNamespace), "/")

		g.Hosts = normalizeKeys(g.Hosts)
		g.Aliases = normalizeKeys(g.Aliases)
		for _, h := range g.Hosts {
			if !containsString(g.Aliases, h) {
				g.Aliases = append(g.Aliases, h)
			}
		}

		if g.ProbeTag == "" {
			g.ProbeTag = c.Health.ProbeTag
		}
		if g.ProbeRepo == "" && legacy {
			g.ProbeRepo = c.Health.ProbeRepo
		}
		g.ProbeRepo = strings.Trim(strings.TrimSpace(g.ProbeRepo), "/")

		if g.Default {
			defaults++
		}
		if len(g.Upstreams) == 0 {
			return fmt.Errorf("分组 %q 没有配置上游镜像源", g.Name)
		}
		if g.Notify.Enabled && g.Notify.MinHealthy > len(g.Upstreams) {
			return fmt.Errorf("分组 %q 的 notify.minHealthy（%d）不能大于上游镜像源数量（%d）",
				g.Name, g.Notify.MinHealthy, len(g.Upstreams))
		}

		for j := range g.Upstreams {
			u := &g.Upstreams[j]
			u.Name = strings.TrimSpace(u.Name)
			if u.Name == "" {
				u.Name = fmt.Sprintf("%s-%d", g.Name, j+1)
			}
			if owner, dup := upstreamNames[u.Name]; dup {
				return fmt.Errorf("上游名称 %q 重复（分组 %q 与 %q），请改为全局唯一", u.Name, owner, g.Name)
			}
			upstreamNames[u.Name] = g.Name

			u.Auth = strings.ToLower(strings.TrimSpace(u.Auth))
			switch u.Auth {
			case "":
				u.Auth = UpstreamAuthConfig
			case UpstreamAuthConfig, UpstreamAuthPassthrough:
			default:
				return fmt.Errorf("upstream %q 的 auth 仅支持 %s 或 %s，当前为 %q",
					u.Name, UpstreamAuthConfig, UpstreamAuthPassthrough, u.Auth)
			}
			if u.Auth == UpstreamAuthPassthrough && (u.Username != "" || u.Password != "") {
				return fmt.Errorf("upstream %q 的 auth=%s 时不能再配置 username/password（两种认证方式互斥）",
					u.Name, UpstreamAuthPassthrough)
			}

			u.URL = strings.TrimSpace(strings.TrimRight(u.URL, "/"))
			parsed, err := url.Parse(u.URL)
			if err != nil || parsed.Host == "" {
				return fmt.Errorf("upstream %q 的 url 无效: %q", u.Name, u.URL)
			}
			if parsed.Scheme != "http" && parsed.Scheme != "https" {
				return fmt.Errorf("upstream %q 的 url 仅支持 http/https: %q", u.Name, u.URL)
			}
			proxy, err := NormalizeProxy(u.Proxy)
			if err != nil {
				return fmt.Errorf("upstream %q 的 proxy 配置无效: %w", u.Name, err)
			}
			if _, err := EffectiveProxy(proxy, u.ProxyUsername, u.ProxyPassword); err != nil {
				return fmt.Errorf("upstream %q 的代理凭据无效: %w", u.Name, err)
			}
			u.Proxy = proxy
			if u.Priority <= 0 {
				u.Priority = 100
			}
			if u.Timeout.D() <= 0 {
				u.Timeout = Duration(30 * time.Second)
			}
		}
	}

	if defaults > 1 {
		return errors.New("只能有一个分组设置 default: true")
	}

	// 解析兜底分组。
	if name := strings.TrimSpace(c.Routing.DefaultGroup); name != "" {
		g := c.GroupByName(name)
		if g == nil {
			return fmt.Errorf("routing.defaultGroup 指向不存在的分组 %q", name)
		}
		c.Routing.DefaultGroup = g.Name
	} else if defaults == 1 {
		for i := range c.Groups {
			if c.Groups[i].Default {
				c.Routing.DefaultGroup = c.Groups[i].Name
			}
		}
	} else {
		c.Groups[0].Default = true
		c.Routing.DefaultGroup = c.Groups[0].Name
	}
	// 保证 Default 标记与 routing.defaultGroup 一致，便于控制台展示。
	for i := range c.Groups {
		c.Groups[i].Default = c.Groups[i].Name == c.Routing.DefaultGroup
	}
	return nil
}

// ProxyDirect 表示上游强制直连，忽略环境变量里的代理设置。
const ProxyDirect = "direct"

// NormalizeProxy 归一化上游代理配置。
//
//	""            沿用环境变量 HTTP_PROXY / HTTPS_PROXY / NO_PROXY
//	"direct"      强制直连（none、off 亦可）
//	"env"         显式沿用环境变量
//	http(s)/socks5 代理地址
func NormalizeProxy(spec string) (string, error) {
	s := strings.TrimSpace(spec)
	if s == "" {
		return "", nil
	}
	switch strings.ToLower(s) {
	case ProxyDirect, "none", "off":
		return ProxyDirect, nil
	case "env", "environment":
		return "", nil
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("无法解析 %q: %w", s, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%q 缺少主机名和端口", s)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks5", "socks5h":
		u.Scheme = strings.ToLower(u.Scheme)
	default:
		return "", fmt.Errorf("%q 仅支持 http/https/socks5/socks5h", s)
	}
	return u.String(), nil
}

// EffectiveProxy 把 proxy 与 proxyUsername/proxyPassword 合并成运行时使用的代理地址。
//
// 支持 http、https、socks5、socks5h 四种协议；Socks5 的认证信息会随地址一起
// 交给 Transport。凭据既可以直接写在地址里，也可以拆到两个独立字段，
// 但两种方式不能同时使用，避免出现「到底以哪个为准」的歧义。
func EffectiveProxy(spec, username, password string) (string, error) {
	s := strings.TrimSpace(spec)
	normalized, err := NormalizeProxy(s)
	if err != nil {
		return "", err
	}
	if username == "" && password == "" {
		return normalized, nil
	}
	if normalized == "" || normalized == ProxyDirect {
		return "", errors.New("只有配置了具体的代理地址（http/https/socks5）时才能使用 proxyUsername / proxyPassword")
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}
	if u.User != nil {
		return "", errors.New("代理地址里已经包含账号密码，请去掉 proxyUsername / proxyPassword，只保留一种写法")
	}
	if username == "" {
		return "", errors.New("配置了 proxyPassword 但没有 proxyUsername")
	}
	u.User = url.UserPassword(username, password)
	return u.String(), nil
}

// MaskProxy 返回可安全写入日志/接口的代理描述，凭据部分会被替换为 ***。
func MaskProxy(spec string) string {
	s := strings.TrimSpace(spec)
	if s == "" {
		return "env"
	}
	if s == ProxyDirect {
		return ProxyDirect
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return s
	}
	if u.User != nil {
		// 用户名和密码一起隐藏，避免日志里泄露代理账号。
		out := u.Scheme + "://***@" + u.Host
		if p := strings.TrimSuffix(u.Path, "/"); p != "" {
			out += p
		}
		return out
	}
	return u.String()
}

// GroupByName 按名称查找分组配置。
func (c *Config) GroupByName(name string) *GroupConfig {
	for i := range c.Groups {
		if c.Groups[i].Name == name {
			return &c.Groups[i]
		}
	}
	return nil
}

// CacheDir 返回展开后的缓存目录绝对路径。缓存关闭或使用 S3 时返回空。
func (c *Config) CacheDir() (string, error) {
	dir := strings.TrimSpace(c.Cache.Dir)
	if dir == "" {
		return "", nil
	}
	if strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(home, dir[2:])
	}
	return filepath.Abs(dir)
}

// normalizeKeys 归一化域名/别名：小写、去 scheme、去端口、去首尾斜杠。
func normalizeKeys(in []string) []string {
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.ToLower(strings.TrimSpace(raw))
		v = strings.TrimPrefix(v, "https://")
		v = strings.TrimPrefix(v, "http://")
		v = strings.Trim(v, "/")
		if v == "" {
			continue
		}
		if host, _, err := net.SplitHostPort(v); err == nil {
			v = host
		}
		if !containsString(out, v) {
			out = append(out, v)
		}
	}
	return out
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
