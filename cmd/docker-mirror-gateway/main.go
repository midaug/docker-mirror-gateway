// Command docker-mirror-gateway 启动一个 Docker 镜像聚合代理服务。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/midaug/docker-mirror-gateway/internal/activity"
	"github.com/midaug/docker-mirror-gateway/internal/cache"
	"github.com/midaug/docker-mirror-gateway/internal/config"
	"github.com/midaug/docker-mirror-gateway/internal/metrics"
	"github.com/midaug/docker-mirror-gateway/internal/registry"
	"github.com/midaug/docker-mirror-gateway/internal/search"
	"github.com/midaug/docker-mirror-gateway/internal/server"
)

// version 由构建脚本通过 -ldflags 注入。
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet("docker-mirror-gateway", flag.ExitOnError)
	cfgPath := flags.String("config", "", "配置文件路径（默认查找 ./config.yaml 或 /etc/docker-mirror-gateway/config.yaml）")
	showVersion := flags.Bool("version", false, "打印版本号后退出")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *showVersion {
		fmt.Printf("docker-mirror-gateway %s\n", version)
		return nil
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	c, err := newCacheStore(cfg, logger.With("component", "cache"))
	if err != nil {
		return err
	}
	defer c.Close()

	// 拉取记录持久化：默认关闭，本地直接运行不会在磁盘留下数据文件；
	// 服务器部署开启后写入统一数据目录，重启仍可查看历史。
	var tracker *activity.Tracker
	if cfg.Data.Persist {
		dataDir, err := cfg.DataDir()
		if err != nil {
			return fmt.Errorf("解析数据目录失败: %w", err)
		}
		path := filepath.Join(dataDir, "activity.json")
		tracker = activity.NewPersistent(activity.DefaultStoreLimit, path, logger.With("component", "activity"))
		defer tracker.Close()
		logger.Info("拉取记录持久化已启用", "path", path, "limit", activity.DefaultStoreLimit)
	}

	m := metrics.New()
	router, err := registry.NewRouter(cfg, logger.With("component", "router"), m)
	if err != nil {
		return err
	}
	proxy := registry.NewProxy(router, c, registry.Options{
		MaxManifestBytes: cfg.Server.MaxManifestBytes,
		Activity:         tracker,
	}, logger.With("component", "proxy"), m)
	enrichUpdated := true
	if cfg.Search.EnrichUpdated != nil {
		enrichUpdated = *cfg.Search.EnrichUpdated
	}
	enrichTags := true
	if cfg.Search.EnrichTags != nil {
		enrichTags = *cfg.Search.EnrichTags
	}
	hubUser, hubPass := dockerHubSearchCredentials(cfg)
	searchSvc := search.New(router.SearchClient(), cfg.Search.Timeout.D(), cfg.Search.Sources, enrichUpdated, logger.With("component", "search"),
		// hub.docker.com 在境内通常只有走代理才通，单独用挂了代理的上游客户端。
		search.WithHubClient(router.ProxiedSearchClient()),
		// 复用 Docker Hub 账号提升搜索 API 限额（换 token 失败自动降级匿名）。
		search.WithHubAuth(hubUser, hubPass),
		search.WithTags(enrichTags),
	)
	if hubUser != "" {
		logger.Info("Docker Hub 搜索将使用配置的账号认证")
	}

	api, err := server.New(server.Options{
		Config:  cfg,
		Router:  router,
		Proxy:   proxy,
		Cache:   c,
		Search:  searchSvc,
		Logger:  logger.With("component", "http"),
		Metrics: m,
		Version: version,
	})
	if err != nil {
		return err
	}

	httpSrv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.D(),
		IdleTimeout:       cfg.Server.IdleTimeout.D(),
	}
	servers := []*http.Server{httpSrv}
	if cfg.Server.AdminAddr != "" {
		servers = append(servers, &http.Server{
			Addr:              cfg.Server.AdminAddr,
			Handler:           api.AdminHandler(),
			ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.D(),
			IdleTimeout:       cfg.Server.IdleTimeout.D(),
		})
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Path != "" {
		logger.Info("已加载配置文件", "path", cfg.Path)
	} else {
		logger.Info("未找到配置文件，使用内置默认配置")
	}
	if dataDir, err := cfg.DataDir(); err == nil {
		logger.Info("数据目录", "dir", dataDir, "persist", cfg.Data.Persist)
	}
	adminAddr := cfg.Server.AdminAddr
	if adminAddr == "" {
		adminAddr = cfg.Server.Addr
	}
	logger.Info("Docker Mirror Gateway 启动中",
		"version", version,
		"pullAddr", cfg.Server.Addr,
		"adminAddr", adminAddr,
		"adminSplit", cfg.Server.AdminAddr != "",
		"cacheBackend", c.Backend(),
		"cacheDir", c.Dir(),
		"strategy", cfg.Selection.Strategy,
		"groups", len(cfg.Groups),
		"autoRoute", cfg.Routing.AutoDetect,
	)
	if cfg.Server.AdminAddr != "" && cfg.Server.AdminToken == "" {
		logger.Warn("管理端口未设置 adminToken，请只在内网或本机回环地址监听", "adminAddr", cfg.Server.AdminAddr)
	}
	for _, g := range cfg.Groups {
		logger.Info("分组",
			"group", g.Name,
			"priority", g.Priority,
			"default", g.Default,
			"aliases", strings.Join(g.Aliases, ","),
			"hosts", strings.Join(g.Hosts, ","),
			"upstreams", len(g.Upstreams),
		)
		for _, u := range g.Upstreams {
			logger.Info("上游镜像源",
				"group", g.Name,
				"name", u.Name,
				"url", u.URL,
				"priority", u.Priority,
				"proxy", config.MaskProxy(u.Proxy),
			)
		}
	}

	go router.Start(ctx)
	go gcLoop(ctx, c, cfg.Cache.GCInterval.D(), logger.With("component", "cache"))

	errCh := make(chan error, len(servers))
	for _, srv := range servers {
		srv := srv
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("监听 %s 失败: %w", srv.Addr, err)
			}
		}()
	}

	select {
	case err := <-errCh:
		return fmt.Errorf("HTTP 服务异常: %w", err)
	case <-ctx.Done():
		logger.Info("收到退出信号，正在优雅关闭")
	}
	// 立即恢复默认信号行为：关闭窗口内再次 Ctrl-C 可以强制退出，
	// 不必等满 20 秒的 Shutdown 超时。
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	shutdownErr := make(chan error, len(servers))
	for _, srv := range servers {
		wg.Add(1)
		go func(srv *http.Server) {
			defer wg.Done()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				shutdownErr <- fmt.Errorf("%s: %w", srv.Addr, err)
			}
		}(srv)
	}
	wg.Wait()
	close(shutdownErr)
	for err := range shutdownErr {
		return fmt.Errorf("优雅关闭失败: %w", err)
	}
	logger.Info("已退出")
	return nil
}

// newCacheStore 按配置构造缓存后端。缓存默认关闭，此时返回空实现。
func newCacheStore(cfg *config.Config, log *slog.Logger) (cache.Store, error) {
	if !cfg.Cache.Enabled {
		log.Info("缓存未启用，所有请求直接回源", "hint", "需要时把 cache.enabled 设为 true")
		return cache.NewNoop(), nil
	}
	maxBytes := int64(cfg.Cache.MaxSizeGB * float64(int64(1)<<30))

	if cfg.Cache.Backend == "s3" {
		useSSL, pathStyle := true, false
		if cfg.Cache.S3.UseSSL != nil {
			useSSL = *cfg.Cache.S3.UseSSL
		}
		if cfg.Cache.S3.PathStyle != nil {
			pathStyle = *cfg.Cache.S3.PathStyle
		}
		store, err := cache.NewS3(cache.S3Config{
			Endpoint:       cfg.Cache.S3.Endpoint,
			Bucket:         cfg.Cache.S3.Bucket,
			AccessKey:      cfg.Cache.S3.AccessKey,
			SecretKey:      cfg.Cache.S3.SecretKey,
			Region:         cfg.Cache.S3.Region,
			Prefix:         cfg.Cache.S3.Prefix,
			UseSSL:         useSSL,
			PathStyle:      pathStyle,
			MaxBytes:       maxBytes,
			TagTTL:         cfg.Cache.TagTTL.D(),
			ManifestMaxAge: cfg.Cache.ManifestMaxAge.D(),
			TmpDir:         cfg.Cache.S3.TmpDir,
			Logger:         log,
		})
		if err != nil {
			return nil, err
		}
		log.Info("S3 缓存已启用", "endpoint", cfg.Cache.S3.Endpoint, "bucket", cfg.Cache.S3.Bucket, "prefix", cfg.Cache.S3.Prefix)
		return store, nil
	}

	dir, err := cfg.CacheDir()
	if err != nil {
		return nil, fmt.Errorf("解析缓存目录失败: %w", err)
	}
	if dir == "" {
		return nil, fmt.Errorf("cache.dir 不能为空")
	}
	store, err := cache.New(cache.Config{
		Dir:            dir,
		MaxBytes:       maxBytes,
		TagTTL:         cfg.Cache.TagTTL.D(),
		ManifestMaxAge: cfg.Cache.ManifestMaxAge.D(),
		Logger:         log,
	})
	if err != nil {
		return nil, err
	}
	log.Info("磁盘缓存已启用", "dir", dir, "maxGB", cfg.Cache.MaxSizeGB)
	return store, nil
}

func gcLoop(ctx context.Context, c cache.Store, interval time.Duration, log *slog.Logger) {
	if interval <= 0 || c.Backend() == "none" {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.EnforceLimit(); err != nil {
				log.Warn("缓存清理失败", "error", err)
			}
		}
	}
}

func newLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.ToLower(cfg.Format) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}

// dockerHubSearchCredentials 从 dockerhub 分组里挑一组可用于搜索 API 的账号。
//
// 只取 auth=config（默认）且填了 username 的官方源；passthrough 源用的是
// 客户端凭证、没有配置账号，跳过。密码字段同时兼容密码与 PAT。
// 找不到时返回空，搜索服务降级为匿名。
func dockerHubSearchCredentials(cfg *config.Config) (string, string) {
	for _, g := range cfg.Groups {
		if g.Name != "dockerhub" && g.Name != "hub" {
			continue
		}
		for _, u := range g.Upstreams {
			if u.Passthrough() || strings.TrimSpace(u.Username) == "" {
				continue
			}
			return u.Username, u.Password
		}
	}
	return "", ""
}
