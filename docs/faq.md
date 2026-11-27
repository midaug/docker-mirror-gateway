# 常见问题

**Docker 报 `server gave HTTP response to HTTPS client`**
地址是 HTTP，必须同时写进 `insecure-registries`；或者给代理配上 TLS。

**`docker pull ghcr.io/xxx` 没有走代理**
`registry-mirrors` 只作用于 Docker Hub。其他仓库用路径前缀 `mirror.example.com/ghcr.io/xxx` 或配置子域名。

**`manifest unknown`**
所有分组都没有这个镜像，或 tag 不存在。单段名称会自动补成 `library/xxx`。

**报错 `这镜像不在白名单 / not in the allowlist`**
DaoCloud 等公共加速源按仓库白名单放行，不在白名单的仓库一律 403。网关会识别这类拒绝：换其他源继续尝试、不把它计入该源的健康降级（否则健康探测用的常用镜像在白名单内，源会在可用/不可用间反复震荡）。只有所有源都拒绝时才返回 404。

**报错 `401 authentication required`，但镜像源是公开的**
Elastic、部分官方源对不存在或私有的仓库返回 401 而非 404。网关把 401 与白名单 403 同样视为仓库级策略：换源重试、不降级健康度；所有源都拒绝时返回 404 `MANIFEST_UNKNOWN`（附拒绝源数量说明），而不是把 401 包成 502。

**有些源把 404 包成 500 返回**
dockerproxy 等源对不存在的 manifest/blob 返回 500，但 body 里带 `MANIFEST_UNKNOWN`/`BLOB_UNKNOWN` 错误码。网关从响应体还原 404 语义：正常换源重试，也不把这种响应计入健康降级。

**docker 25+ 拉取某些镜像报 404，但镜像明明存在**
新版 Docker 还会拉取 OCI attestation manifest（构建证明/SBOM，清单索引里 `platform: unknown` 的条目）。部分镜像站未同步 attestation，网关在所有源都没有这份清单时按规范返回 404；主镜像的清单与层不受影响，可用路径前缀指定一个有 attestation 的源。

**拉取监控里失败记录的「上游」列**
失败记录指向最后一个被尝试的源——返回给客户端的错误正是它产生的；重试链上每个失败源都会被逐个标记。

**同名仓库落到了不是想要的分组**
自动判定按 `priority` 决胜。用路径前缀 `/quay.io/...` 精确指定，或调小目标分组的 `priority`。

**第一次拉取比较慢**
自动判定需要并发探测各分组（通常几百毫秒，最坏 3s），结果缓存 30 分钟。可用 `GET /api/v1/routes` 查看。

**`502 UNKNOWN` 且 `lastError` 是 `context deadline exceeded`**
上游不可达。把可用源的 `priority` 调小，或删掉不可达的源。

**私有镜像**
给对应源配置 `username` / `password`（默认 `auth: config`）。同一个分组内的多个加速地址应属于同一仓库。

**`docker login` 网关会影响各分组的认证吗**
默认不会。网关的拉取端点不校验客户端凭证（`/v2/` 恒返回 200），`auth: config`（默认）的源会忽略客户端带来的 `Authorization`，只用配置文件里该源自己的账号。因此本地部署不需要 login 网关。

公网部署想让用户的 docker login 凭证生效（私有镜像、提升配额）时，给**官方源**单独设置 `auth: passthrough`：凭证只发给这一个源，且带凭证的拉取不写共享缓存。三方源也支持配置，但会收到用户凭证，**除非完全信任否则不要开**。多个分组都开透传时请按多域名部署，程序不校验 Host，安全性由部署者负责。详见[配置参考 · 上游认证](configuration.md#上游认证按源配置)。

**配置了 adminAddr 后，拉取端口访问控制台是 404**
预期行为，控制台与管理接口只在管理端口。

**群晖 Docker / NAS 面板里的镜像搜索没有结果**
群晖 Docker（DSM / Container Manager）的注册表搜索走的是 Docker Registry v1 的 `GET /v1/search?q=...`。网关已兼容该接口，结果来自与控制台相同的多源搜索（`search.sources`），返回 `num_results` / `results` 结构；`n`、`page` 参数照常生效。搜索没有结果时依次确认：`search.enabled` 没有被关掉（关掉时该接口返回 404）、客户端 IP 在 `access` 白名单内、网关日志里 `path=/v1/search` 的请求确实到达。

**`GET /v2/_catalog` 只列出部分仓库**
网关是拉取代理，无法枚举上游全部仓库。`_catalog` 返回的是拉取记录里出现过的仓库名（去重、按名称排序，支持 `n` / `last` 分页），够 NAS、面板类 UI 浏览；没拉过的仓库不会出现在列表里。

**改了 `config.yaml` 但没生效**
配置只在启动时加载，改完要重启进程或容器；`/api/v1/status` 里每个上游的 `proxy` 字段可以直接确认当前生效的值。

**能推送吗**
不能。这是只读拉取代理，`POST /v2/.../blobs/uploads/` 返回 405。

## 说明

`config.yaml` 中的公共镜像源仅作示例，请替换为你有权使用、且服务条款允许的源；生产环境请自建或使用官方源，并遵守对应仓库的速率与使用政策。
