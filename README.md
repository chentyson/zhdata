# zhdata

单文件 Go 实现的多路复用隧道（zhenhanvn / s-ui 的「一级核心」）。一条 uuid = 一组并行会话，
流在多会话间 fan-out，带按 seq 重组、快重传、按交付滑动窗口、GOAWAY 接管、zhv5 分桶抗整形。
纯 Go，无 CGO（sqlite 走 `modernc.org/sqlite`）。

协议 magic：`zhv7`（握手带每进程随机 16B `device_id`，服务端显式回认证 OK/reject）。
服务端同时接受 `zhv6` / `zhv5` / `zhv4` 旧客户端。

版本：`0.15.3`（`zhdata -version` → `zhdata 0.15.3`）。

## 构建

```bash
go build ./cmd/zhdata        # 可执行文件在 ./cmd/zhdata（v0.15.2 起仓库是 module）
./build.sh                   # 六平台交叉编译 + zip（资产名见接口文档 §4）
go vet ./... && go test ./... -count=1
```

Go module：`github.com/chentyson/zhdata`。根目录是库包 `package zhdata`，`./cmd/zhdata` 是 CLI。
CLI 的 flag / config / 日志行为与拆分前的单文件版完全一致。

## 快速开始

```bash
# 服务端（直连后端）
zhdata -mode server -listen 49888,49889 -users /opt/zhdata/users.txt \
       -backend direct -admin 127.0.0.1:6070 -admin-token <token>

# 服务端（字节级 relay：控制面在 relay，字节原样转给下一跳 zhdata）
zhdata -mode server -listen 49888 -users /opt/zhdata/users.txt \
       -backend zhdata -upstream 1.2.3.4:49888,1.2.3.4:49889

# 客户端（本地 mixed socks5 + http）
zhdata -mode client -listen 127.0.0.1:10808 \
       -server example.com:49888,example.com:49889 -uuid your-uuid -conns 16 -spread 8

curl -x socks5h://127.0.0.1:10808 http://example.com
curl -x http://127.0.0.1:10808   http://example.com
```

也支持 `-c config.json`（全部字段都有同名 CLI flag，优先级：显式 CLI flag > 配置文件 > 默认值）。
字段表与语义：见 [zhdata-核心接口文档.md](zhdata-核心接口文档.md) §3.1；
服务端 relay 语义见 §3.7。

## 管理 API（server 模式，`-admin 127.0.0.1:6070`）

| 端点 | 用途 |
|---|---|
| `GET /v1/version` | 核心版本 |
| `PUT /v1/users` | 全量替换 API 用户集（与文件/sqlite 源取并集，API 策略覆盖同名 uuid） |
| `GET /v1/stats` | `sessions_active` + 按 uuid 的 `devices` / `up` / `down` 累计字节 |
| `GET /v1/sessions` | 活会话明细（面板入站连接列表）：`{"ts":..,"total":N,"sessions":[{uuid,device,slot,remote,started_at,relay}]}`，按建立时间**倒序**；`?limit=n` 取最近 n 条（默认 50，钳到 10–100，不是分页），`total` 恒为全量；relay 进程列的是活管道（`relay=true`） |
| `POST /v1/users/kick` | 按 uuid 切断会话（relay 模式下同时切断该 uuid 的字节管道） |
| `POST /v1/reload` | 立即重读文件/sqlite 用户源 |
| `GET /v1/relay` | relay 状态：`{"enabled":..,"upstream":[..],"pipes":N}` |
| `PUT /v1/relay` | 热更新 relay：`{"upstream":["h:p",..],"enabled":bool}`（只影响新连接） |

`-admin-token` 设置后所有 `/v1/*` 需要 `Authorization: Bearer <token>`。
契约权威定义在 s-ui 仓库 `docs/zhdata-admin-api.md`。

## 作为 Go 库嵌入

```go
import "github.com/chentyson/zhdata"

cli, err := zhdata.NewClient(&zhdata.ClientConfig{
	Servers: []string{"example.com:49888", "example.com:49889"},
	UUID:    "your-uuid",
	Conns:   16, Spread: 8, DLPool: -1,
	LogLevel: "info",
	Logf:     func(format string, a ...any) { log.Printf(format, a...) },
})
defer cli.Close()

conn, err := cli.DialContext(ctx, "api.example.com", 443) // 内存流直连，复用完整客户端栈
// conn 实现 CloseWrite()：半关 app→tunnel（上游发 tClose/finSeq），下行继续可读
```

服务端/relay 也能内嵌：`zhdata.NewServer(&zhdata.ServerOptions{...})` + `(*Server).Close()`。
日志可整体接管：`zhdata.SetLogLevel(...)` / `zhdata.SetLogSink(func(level int, tag, msg string))`。
完整签名与语义：接口文档 §8。

## 发布

tag 纯 semver（`v0.15.3`）→ `.github/workflows/release.yml` 构建六平台并上传
`zhdata-{windows,linux,macos}-{64,arm64}.zip`。zhenhanvn 按 `bin/zhdata/zhdata(.exe)` +
`-c {0}` 启动，契约见接口文档 §2/§4。

## 目录

| 路径 | 说明 |
|---|---|
| `*.go`（根包 `zhdata`） | 协议、客户端栈（`client.go`）、服务端栈（`server.go`/`session.go`）、relay（`relay.go`）、admin API（`admin.go`）、嵌入 API（`api.go`/`client_api.go`）、用户源（`keysource.go`）、日志（`logx.go`） |
| `cmd/zhdata/` | CLI 入口与 flag/JSON 配置解析（`appconfig.go`） |
| `tools/` | 压测与抓包辅助小工具（`zbench`、`qspike`、`spd`、`fsrv`、`udpflood`） |
| `zhdata-核心接口文档.md` | 与 zhenhanvn / s-ui 的接口契约（含 relay、库嵌入、日志格式） |
| `zhdata-一级核心接入方案.md`、`开发方案.md` | 接入方案与设计背景 |
