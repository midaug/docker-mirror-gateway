# 常见问题

**Docker 报 `server gave HTTP response to HTTPS client`**
地址是 HTTP，必须同时写进 `insecure-registries`；或者给代理配上 TLS。

**`docker pull ghcr.io/xxx` 没有走代理**
`registry-mirrors` 只作用于 Docker Hub。其他仓库用路径前缀 `mirror.example.com/ghcr.io/xxx` 或配置子域名。

**`manifest unknown`**
所有分组都没有这个镜像，或 tag 不存在。单段名称会自动补成 `library/xxx`。

**同名仓库落到了不是想要的分组**
自动判定按 `priority` 决胜。用路径前缀 `/quay.io/...` 精确指定，或调小目标分组的 `priority`。

**第一次拉取比较慢**
自动判定需要并发探测各分组（通常几百毫秒，最坏 3s），结果缓存 30 分钟。可用 `GET /api/v1/routes` 查看。

**`502 UNKNOWN` 且 `lastError` 是 `context deadline exceeded`**
上游不可达。把可用源的 `priority` 调小，或删掉不可达的源。

**私有镜像**
给对应分组的上游配置 `username` / `password`。同一个分组内的多个加速地址应属于同一仓库。

**配置了 adminAddr 后，拉取端口访问控制台是 404**
预期行为，控制台与管理接口只在管理端口。

**改了 `config.yaml` 但没生效**
配置只在启动时加载，改完要重启进程或容器；`/api/v1/status` 里每个上游的 `proxy` 字段可以直接确认当前生效的值。

**能推送吗**
不能。这是只读拉取代理，`POST /v2/.../blobs/uploads/` 返回 405。

## 说明

`config.yaml` 中的公共镜像源仅作示例，请替换为你有权使用、且服务条款允许的源；生产环境请自建或使用官方源，并遵守对应仓库的速率与使用政策。
