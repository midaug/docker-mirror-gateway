# 路由与客户端接入

## 路由

解析优先级从高到低：

| 顺序 | 方式 | 示例 | 适用场景 |
| --- | --- | --- | --- |
| 1 | 路径首段别名 | `mirror.example.com/ghcr.io/owner/app:1.0` | Docker CLI、CI、临时拉取 |
| 2 | 请求 Host | `ghcr.mirror.example.com/owner/app:1.0` | containerd / K8s / 多域名 |
| 3 | 路由缓存 | 上次判定的结果（默认 30 分钟） | 重复拉取 |
| 4 | 自动判定 | `mirror.example.com/owner/app:1.0` | 单域名且不带前缀 |
| 5 | 兜底分组 | `routing.defaultGroup` | 关闭自动判定时 |

### 路径前缀

仓库名首段命中某个分组的 `aliases` 时会被剥离，并路由到该组：

```bash
docker pull mirror.example.com/ghcr.io/stefanprodan/podinfo:latest
docker pull mirror.example.com/quay.io/prometheus/node-exporter:latest
docker pull mirror.example.com/nvcr.io/nvidia/cuda:12.4.1-base-ubuntu22.04
```

`ghcr.io/stefanprodan/podinfo` 本身就是合法的 repository 名（允许点号），Docker 不需要额外配置。短别名 `/ghcr/...`、`/quay/...`、`/k8s/...` 同样可用。

### 域名

给分组配 `hosts`，把域名指到本服务：

```yaml
groups:
  - name: ghcr
    hosts: ["ghcr.mirror.example.com"]
```

```bash
docker pull ghcr.mirror.example.com/stefanprodan/podinfo:latest
```

containerd 要求路径与上游一致，所以这种方式最适合 K8s：

```toml
# /etc/containerd/config.toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."ghcr.io"]
  endpoint = ["https://ghcr.mirror.example.com"]
```

### 自动判定

两个线索都没有时，并发询问各分组「有没有这个仓库」：带 tag 的请求直接查 manifest，blob / tags 请求用 `tags/list?n=1`。结果按 `routeTTL` 缓存，不存在的仓库走负缓存，后续请求零成本。

```bash
# 首次约 0.5~3 秒判定，之后走缓存
docker pull mirror.example.com/stefanprodan/podinfo:latest
```

同名仓库可能同时存在于多个源（如 `prometheus/node-exporter`），由 `routing.strategy` 决胜：`priority`（默认，按分组优先级）或 `fastest`（最快响应）。要精确指定就用路径前缀 `/quay.io/prometheus/node-exporter`。

| 请求 | 结果 |
| --- | --- |
| `mirror/ghcr.io/owner/app:1.0` | 剥离前缀 → `ghcr` 组，上游仓库 `owner/app` |
| `ghcr.mirror.local/owner/app:1.0` | Host 命中 → `ghcr` 组，仓库不变 |
| `mirror/library/nginx:latest` | 自动判定 → `dockerhub` 组，补全为 `library/nginx` |
| `/v2/` | 直接返回 `{}`，不触发路由 |

响应头 `X-Mirror-Gateway-Group` 与 `X-Mirror-Gateway-Upstream` 会标明实际使用的分组和源。

## 接入 Docker

### Docker Hub 加速

编辑 `/etc/docker/daemon.json`（macOS 为 Docker Desktop → Settings → Docker Engine）：

```json
{
  "registry-mirrors": ["http://127.0.0.1:8080"],
  "insecure-registries": ["127.0.0.1:8080"]
}
```

```bash
sudo systemctl restart docker
docker pull alpine:latest          # 自动走 dockerhub 组
```

> `registry-mirrors` 只对 Docker Hub 生效，其他仓库用路径前缀（Docker CLI）或域名（containerd）。

### 其他运行时

```bash
# containerd：按仓库分别配置
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:8080"]
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."ghcr.io"]
  endpoint = ["http://ghcr.mirror.example.com"]

# nerdctl
sudo nerdctl --registry-mirror=http://127.0.0.1:8080 pull alpine

# Podman（/etc/containers/registries.conf）
[[registry]]
location = "docker.io"
[[registry.mirror]]
location = "127.0.0.1:8080"
insecure = true
```

当成普通 registry 直接访问，便于排查：

```bash
curl -s http://127.0.0.1:8080/v2/
curl -s -H 'Accept: application/vnd.oci.image.index.v1+json' \
  http://127.0.0.1:8080/v2/ghcr.io/stefanprodan/podinfo/manifests/latest | head -c 200
```

## HTTPS

Docker Mirror Gateway 只监听 HTTP，TLS 交给反向代理。Caddy 示例（泛域名 + 自动证书）：

```caddyfile
*.mirror.example.com, mirror.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1
    }
}

# 管理后台单独走内网域名，并限制来源 IP
admin.mirror.example.com {
    @intranet remote_ip 10.0.0.0/8 192.168.0.0/16
    reverse_proxy @intranet 127.0.0.1:8081
}
```

然后把 `daemon.json` 里的地址换成 `https://mirror.example.com`，就不需要 `insecure-registries` 了。
