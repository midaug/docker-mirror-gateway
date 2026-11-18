# syntax=docker/dockerfile:1

# ---------- 构建阶段 ----------
FROM golang:1.23-alpine AS builder

WORKDIR /src

# 国内网络可加：--build-arg GOPROXY=https://goproxy.cn,direct
ARG GOPROXY=https://proxy.golang.org,direct
ENV CGO_ENABLED=0 GOOS=linux GOPROXY=${GOPROXY}

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/docker-mirror-gateway ./cmd/docker-mirror-gateway

# ---------- 运行阶段 ----------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 -h /var/lib/docker-mirror-gateway mirror \
    && mkdir -p /var/lib/docker-mirror-gateway/cache \
    && chown -R mirror:mirror /var/lib/docker-mirror-gateway

COPY --from=builder /out/docker-mirror-gateway /usr/local/bin/docker-mirror-gateway

USER mirror
WORKDIR /var/lib/docker-mirror-gateway
ENV MIRROR_GATEWAY_CACHE_DIR=/var/lib/docker-mirror-gateway/cache

# 8080 镜像拉取，8081 管理后台
EXPOSE 8080 8081
VOLUME ["/var/lib/docker-mirror-gateway"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/docker-mirror-gateway"]
