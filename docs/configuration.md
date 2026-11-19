# 配置参考

完整带注释示例见项目根目录的 [`config.example.yaml`](../config.example.yaml)，复制为 `config.yaml` 后按需修改。所有字段都可被环境变量覆盖。配置只在启动时加载一次，修改后需要重启进程（或容器）才生效。

## groups

| 字段 | 说明 |
| --- | --- |
| `name` | 分组名，全局唯一 |
| `priority` | 分组优先级，数值越小越优先；自动判定歧义时按它决胜 |
| `aliases` | 路径首段别名，如 `ghcr`、`ghcr.io`；`hosts` 会自动追加 |
| `hosts` | 请求域名，如 `ghcr.mirror.example.com` |
| `libraryNamespace` | 单段仓库名补全，Docker Hub 填 `library` |
| `probeRepo` / `probeTag` | 健康探测用的真实镜像；留空只探测 `/v2/` |
| `default` | 兜底分组标记（只能有一个） |
| `notify.enabled` / `notify.minHealthy` | 分组监控通知，见[运行与运维](operations.md#监控通知) |
| `upstreams` | 组内镜像源列表，见下表 |

## groups[].upstreams

| 字段 | 说明 |
| --- | --- |
| `name` | 全局唯一的源名称 |
| `url` | 上游地址，如 `https://ghcr.io` |
| `priority` | 组内优先级，数值越小越优先 |
| `username` / `password` | 需要认证时填写，支持 `${ENV_VAR}` |
| `timeout` | 等待响应头超时，默认 `30s` |
| `headers` | 附加请求头 |
| `proxy` | 该源专用代理：`http(s)://`、`socks5://`，或 `direct`；留空沿用环境变量 |
| `proxyUsername` / `proxyPassword` | 代理认证信息；与地址里的 `user:pass@` 二选一 |

## routing

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `autoDetect` | `true` | 无前缀/域名线索时并发探测仓库归属 |
| `pathAliases` | `true` | 启用路径首段别名 |
| `strategy` | `priority` | 自动判定胜出规则：`priority` 或 `fastest` |
| `defaultGroup` | 第一个分组 | 兜底分组 |
| `detectTimeout` | `5s` | 单次自动探测总超时（各组并发） |
| `routeTTL` | `30m` | 「仓库 → 分组」缓存时长 |
| `negativeTTL` | `2m` | 「仓库不存在」负缓存时长 |

## server / cache / health / selection / access / search / notify

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `server.addr` | `:8080` | 镜像拉取监听地址，只提供 `/v2/*` |
| `server.adminAddr` | 空 | 管理后台独立监听地址；空表示与 `addr` 合并 |
| `server.publicURL` | 空 | 客户端访问地址，用于生成接入示例 |
| `server.adminToken` | 空 | 管理接口与 `/metrics` 的令牌；空表示不校验 |
| `server.maxManifestBytes` | `8MB` | 单个 manifest 大小上限 |
| `cache.enabled` | `false` | 缓存总开关 |
| `cache.backend` | `disk` | `disk` 或 `s3` |
| `cache.dir` | `./data/cache` | 缓存目录，仅 `backend=disk` |
| `cache.maxSizeGB` | `50` | 容量上限，超出后按最久未访问淘汰到 85% |
| `cache.tagTTL` | `5m` | tag → manifest 缓存有效期；digest 不受限 |
| `cache.gcInterval` | `10m` | 定期清理间隔 |
| `cache.manifestMaxAge` | `720h` | manifest 缓存自然过期时间 |
| `health.interval` | `1m` | 探测间隔 |
| `health.timeout` | `10s` | 单次探测超时 |
| `health.failThreshold` | `3` | 连续失败多少次判定不可用 |
| `health.trafficWindow` | `2m` | 窗口内有真实拉取的上游跳过定时探测 |
| `selection.strategy` | `speed` | 组内选源：`speed`、`priority` 或 `latency` |
| `selection.preferFresh` | `true` | 对 `latest` 等标签先比较构建时间 |
| `selection.freshTags` | `[latest]` | 参与新旧比较的标签 |
| `selection.freshnessTolerance` | `0s` | 差异小于该值视为一样新，继续按速度选 |
| `selection.freshnessTTL` | `10m` | 新旧结果缓存时长 |
| `notify.enabled` | `false` | 监控通知总开关 |
| `notify.interval` | `30m` | 持续异常时重复提醒的间隔（触发与恢复始终立即发） |
| `notify.webhooks[].method` | `POST` | `GET` / `POST`；GET 只替换 url 模板 |
| `notify.webhooks[].url` | - | 请求地址模板，支持变量替换 |
| `notify.webhooks[].body` | 空 | POST 消息体模板；留空发送结构化 JSON，GET 忽略 |
| `notify.webhooks[].contentType` | `application/json` | POST 的 Content-Type |
| `notify.email` | - | SMTP 邮件通道（465 隐式 TLS / 587 STARTTLS） |
| `groups[].notify.enabled` | `false` | 在该分组启用监控通知 |
| `groups[].notify.minHealthy` | `0` | 健康源少于该数量时告警；0 = 不通知 |
| `access.allow` | 空 | 允许拉取的 IP / CIDR；空表示不限制 |
| `access.deny` | 空 | 明确拒绝的来源，优先于 `allow` |
| `access.trustForwardedFor` | `false` | 是否采信 `X-Forwarded-For` |
| `search.enabled` | `true` | 控制台镜像搜索 |

| `search.sources` | `[hub, quay, mcr]` | 搜索哪些仓库 |
| `search.timeout` | `8s` | 单次搜索总超时 |
| `search.enrichUpdated` | `true` | 是否补查 Quay 的更新时间 |
| `search.enrichTags` | `true` | 是否补查除 latest 外的最新版本号 |

> `health.probeRepo` / `probeTag` 只对旧版单分组配置（`group:`）生效，多分组请用 `groups[].probeRepo`。

## 上游认证（按源配置）

认证方式属于**每个上游源**，不是全局开关，也不按分组推断。

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `groups[].upstreams[].auth` | `config` | `config`：使用下面 `username`/`password` 自动完成上游认证；`passthrough`：把客户端 `docker login` 的凭证原样用于该源 |
| `groups[].upstreams[].username` | 空 | 仅 `auth: config` 使用 |
| `groups[].upstreams[].password` | 空 | 仅 `auth: config` 使用；支持 `${ENV_VAR}` 占位符 |

`auth: passthrough` 与 `username`/`password` 互斥，同时配置会在启动时报错。透传源不使用配置账号，也不复用网关的令牌缓存；客户端凭证只会发给该源自身。透传源的拉取不写共享缓存，避免私有内容进入匿名缓存。

工作方式：透传源返回 401 挑战时，网关把挑战的 `realm` 改写指向自己的 `/token` 端点（`service`/`scope` 保留上游原值）转给客户端；docker 用登录凭证到 `/token` 换取**该上游签发**的令牌后重试。`/token` 只接受配置为 `auth: passthrough` 的上游名，网关会先探测该上游的真实挑战以获得 `realm`/`service`，再把凭证发给它自己的令牌服务，防止被当作任意地址的凭证跳板。挑战 realm 按当前请求的 Host 与 `X-Forwarded-Proto` 推导，反代后请确保传对这两个值。

```yaml
groups:
  - name: dockerhub
    upstreams:
      # 本地部署推荐：官方源配置自己的账号，不启用透传
      - name: dockerhub-official
        url: https://registry-1.docker.io
        auth: config
        username: "${DOCKERHUB_USER}"
        password: "${DOCKERHUB_PASS}"

  - name: ghcr-public
    hosts: [ghcr.example.com]
    upstreams:
      # 公网部署：官方源可以启用透传，让用户用自己的账号拉取
      - name: ghcr-official
        url: https://ghcr.io
        auth: passthrough
```

### 安全提示

- 本地/内网单域名部署通常**不要**开 `passthrough`，只给需要的源配置 `username`/`password`。
- `passthrough` 支持配在任意源上，但三方镜像站也会收到用户的 `docker login` 凭证（Basic 原文或换取的 Bearer 令牌）。**除非完全信任该源，否则不要开启**；这不是程序能替你判断的风险。
- 如果多个分组里的源都启用了 `passthrough`，请按**多域名**部署：每个分组绑定独立域名并让 docker 登录对应域名。程序**不会**校验请求 Host，也不会阻止任意域名传入；凭证是否只到目标分组由部署者负责。单域名靠路径/自动探测区分分组时不要启用多组透传。
- 客户端接入：`docker login ghcr.example.com`（网关不校验凭证，账号密码在上游令牌服务才被真正使用），之后正常 pull；匿名拉取不受影响。

## 端口分离

```yaml
server:
  addr: ":8080"          # 只提供 /v2/*，对使用方开放
  adminAddr: ":8081"     # 控制台、/api/*、/metrics 只在这里
  adminToken: "换成随机串"
```

| 端口 | 提供的内容 |
| --- | --- |
| `addr` | `/v2/*`、`/healthz`、`/readyz` |
| `adminAddr` | 控制台 `/`、`/api/v1/*`、`/metrics`、健康检查 |

拆分后，拉取端口访问控制台与管理接口一律 404，不会泄露上游地址、凭据与缓存信息。`adminAddr` 留空则退回单端口模式。compose 示例已把管理端口绑到宿主机回环地址。

## 源级代理

每个上游可以单独指定代理，常见于「某个源需要翻墙、某个源必须直连」：

```yaml
groups:
  - name: dockerhub
    upstreams:
      - name: dockerhub
        url: https://registry-1.docker.io
        proxy: "socks5://127.0.0.1:1080"
        proxyUsername: "user"
        proxyPassword: "${PROXY_PASSWORD}"
      - name: dockerhub-daocloud
        url: https://docker.m.daocloud.io
        proxy: "direct"                    # 强制直连，忽略环境变量
```

| 写法 | 行为 |
| --- | --- |
| 不写 / `""` / `env` | 沿用 `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY` |
| `direct` | 强制直连（也可写 `none`、`off`） |
| `http://` / `https://` | 走指定的 HTTP 代理 |
| `socks5://` / `socks5h://` | 走指定的 SOCKS5 代理 |

凭据既可以拆成 `proxyUsername` / `proxyPassword`，也可以写在地址里（`http://user:pass@host:port`），两种只能选一种。密码含 `@`、`:`、`#` 时建议用独立字段。日志、`/api/v1/status` 和控制台中统一显示为 `***`。

健康探测与真实拉取共用同一个代理，不会出现「探测通过但拉不动」。

代理只作用于配置它的那个上游，改不了整个分组的走向：组内仍然按速度（或 `priority`）选源，挂了代理的官方源通常比境内加速源慢，只有在 `latest` 更新（`selection.preferFresh`）或其它源不可用时才会被选中。想让某个分组固定走代理，把组内其他上游删掉，或把 `selection.strategy` 改成 `priority` 并给它最小的 `priority`。

控制台的 Docker Hub 搜索会优先使用显式配了代理的上游，因为 `hub.docker.com` 在境内通常只有代理能访问；Quay、MCR 仍走默认直连。

## 内置加速源

`config.yaml` 已预置一批境内公共加速源，开箱即用。数据取自 [status.anye.xyz](https://status.anye.xyz/) 的实时状态，加上本机实测吞吐，只保留在线且无需登录的公共源。

| 分组 | 上游（初始顺序） | 实测吞吐 |
| --- | --- | --- |
| `dockerhub` | sparkcr → 1ms → daocloud → 1panel → 官方 | 3.4 MB/s |
| `ghcr` | 南京大学 → sparkcr → dockerproxy → 官方 | 19 MB/s |
| `quay` | 南京大学 → sparkcr → dockerproxy → daocloud → 官方 | 6.8 MB/s |
| `mcr` | sparkcr → dockerproxy → daocloud → 官方 | 22 MB/s |
| `k8s` | 南京大学 → sparkcr → daocloud → dockerproxy → 官方 | 4.1 MB/s |
| `gcr` | 南京大学 → sparkcr → daocloud → dockerproxy → 官方 | 1.6 MB/s |
| `elastic` | sparkcr → daocloud → 官方 | 9.0 MB/s |
| `nvcr` | 南京大学（`ngc.nju.edu.cn`）→ sparkcr → daocloud → 官方 | 30 MB/s |

- 表格只是出厂顺序，`speed` 策略会按实测吞吐动态重排，不用手改配置。
- 官方源一律放在 `priority: 100` 兜底：通常不可直连，但配上 `proxy` 后是内容绝对正确的退路。
- 这些都是第三方免费服务，可能限速或下线；健康检查会自动降级，也可以照格式换成自己的源或私有 registry。
