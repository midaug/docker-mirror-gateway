APP     := docker-mirror-gateway
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build run test vet fmt tidy docker up down logs clean

all: build

## build: 编译到 bin/docker-mirror-gateway
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(APP) ./cmd/docker-mirror-gateway

## run: 本地运行（读取 config.yaml）
run:
	go run ./cmd/docker-mirror-gateway -config config.yaml

## test: 运行全部测试
test:
	go test ./... -count=1

## vet: 静态检查
vet:
	go vet ./...

## fmt: 格式化代码
fmt:
	gofmt -l -w .

## tidy: 整理依赖
tidy:
	go mod tidy

## docker: 构建镜像
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(APP):$(VERSION) .

## up: 使用 compose 启动
up:
	docker compose up -d --build

## down: 停止 compose 服务
down:
	docker compose down

## logs: 查看容器日志
logs:
	docker compose logs -f --tail=100

## clean: 清理构建产物（不会删除 data/ 缓存）
clean:
	rm -rf bin
