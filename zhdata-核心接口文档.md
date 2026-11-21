# zhdata 核心接口文档（zhenhanvn 一级核心接入）

版本：zhdata **v0.15.1**（2026-11-20）· 适用 zhenhanvn（v2rayN-7.24.9 fork）
配套：《zhdata-一级核心接入方案.md》（总体方案）、《开发方案.md》§11（契约核对记录）、s-ui 侧契约 `s-ui/docs/zhdata-admin-api.md`（admin API 权威定义）

---

## 1. 总览

zhdata 是与 xray / sing-box / mihomo 同级的**外部一级核心**：单二进制、单进程、JSON 配置、stdout 日志。

```text
zhenhanvn/
├── zhenhanvn.exe
├── bin/
│   ├── xray/xray.exe
│   └── zhdata/zhdata.exe        ← 本核心
└── binConfigs/
    └── config.json              ← 本核心被启动时读取的配置（与 xray 同名，按当前节点类型生成）
```

```text
zhenhanvn 启动核心：  bin/zhdata/zhdata.exe -c config.json     （cwd = binConfigs）
本地代理：            127.0.0.1:<listen>  = SOCKS5 + HTTP（mixed）
远端：                s-ui zhdata inbound 或 zhdata server（协议 zhv7，兼容 zhv6/zhv5/zhv4 旧客户端）
```

## 2. 进程与命令行契约

| 契约 | 说明 |
|---|---|
| 可执行文件 | `bin/zhdata/zhdata(.exe)`（`CoreExes = ["zhdata"]`；非 Windows 无 .exe，`CoreManager.Init` 自动 chmod） |
| 启动参数 | `Arguments = "-c {0}"`；`{0}` 普通路径为**相对文件名** `config.json`（工作目录已是 `binConfigs`），非 Windows sudo 路径为绝对带引号路径——两种都支持 |
| 版本探测 | `zhdata -version` → exit 0，stdout 输出 `zhdata 0.15.3`（满足 `v?(?<version>\d+\.\d+\.\d+)`） |
| 前台常驻 | 不 fork、不守护；启动 100ms 内 `HasExited` 即失败 |
| 停止 | zhenhanvn 用 `Process.Kill()`（Windows=TerminateProcess），**不发信号**；核心无状态文件，强杀无损。reload = 杀进程后重启 |
| 单进程 | 核心不自管子进程（Windows Kill 不带子进程树） |
| CLI 兼容 | 全部配置字段都有同名 CLI flag（`-mode/-listen/-server/-uuid/-users/-backend/-upstream/-max-clients/-max-sessions/-decoy/-v/-device-id/-conns/-spread/-train/-migrate/-dlconns/-slotbase/-pprof/-admin/-admin-token/-loglevel`），显式 CLI > 配置文件 > 默认 |
| 构建路径 | v0.15.2 起仓库是 Go module `github.com/chentyson/zhdata`：可执行入口在 `./cmd/zhdata`（`go build ./cmd/zhdata`，`build.sh`/`build.ps1` 已改），产物名与 flag 语义不变；根包是可直接嵌入的库（§8） |

C# 注册示例（CoreInfoManager.InitCoreInfo）：

```csharp
new CoreInfo {
    CoreType = ECoreType.zhdata,
    CoreExes = ["zhdata"],
    Arguments = "-c {0}",                       // 有且只有一个 {0}，不能再含花括号
    Url = GetCoreUrl(ECoreType.zhdata),
    ReleaseApiUrl = GetCoreUrl(ECoreType.zhdata).Replace(Global.GithubUrl, Global.GithubApiUrl),
    DownloadUrlWin64 = GetCoreUrl(ECoreType.zhdata) + "/download/{0}/zhdata-windows-64.zip",
    // ... WinArm64/Linux64/LinuxArm64/OSX64/OSXArm64 同款命名
    Match = "zhdata",                            // 本 fork 无消费者，仅为完整
    VersionArg = "-version",
}
// Global.CoreUrls: { ECoreType.zhdata, "chentyson/zhdata" }   ← 必须登记，否则启动即 KeyNotFoundException
```

## 3. 配置接口（`-c config.json`）

### 3.1 字段表

| JSON 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `mode` | string | `client` | `client` \| `server` |
| `listen` | string | 必填 | client：本地代理口（如 `127.0.0.1:10808`）；server：逗号分隔多端口 |
| `server` | string \| string[] | 必填(client) | `["h1:p1","h2:p2"]` 或 `"h1:p1,h2:p2"`；slot i 拨 `servers[i%N]`（多端口分桶抗整形） |
| `uuid` | string | 必填(client) | 用户 uuid（大小写不敏感） |
| `users` | string | 必填(server) | 逗号分隔 txt/json/**s-ui sqlite** 路径（只读、20s mtime 热加载） |
| `backend` | string | `direct` | server 侧：`direct` 或 socks 地址（对接 sing-box socks inbound）；`zhdata` = 字节级 relay（§3.7，需 `upstream`） |
| `upstream` | string | 空 | server 侧（`backend=zhdata`）：上游 zhdata 服务端列表 `h1:p1,h2:p2`；每条下游连接按 round-robin 选一个上游（v0.15.2 起） |
| `max_clients` | int | 100 | server 侧兼容别名；等价于全局会话上限。若显式设置 `max_sessions`，以 `max_sessions` 为准 |
| `max_sessions` | int | 0 | server 侧全局并发 tunnel session 资源上限；`<=0` 表示不限制 |
| `device_id` | string | 空 | client：可选 16 字节 hex（32 个十六进制字符）。空 = 每次进程启动自动生成随机 device_id；进程存活期间所有 slot 共用 |
| `decoy` | string | 空 | server 侧：非协议 HTTP 流量 splice 目标 `host:port` |
| `loglevel` | string | `info` | `none` \| `error` \| `warning` \| `info` \| `debug`（§5） |
| `verbose` | bool | false | 兼容字段：true ≡ `loglevel: "debug"` |
| `conns` | int | 1 | 并行会话数 1–64（建议 16–32） |
| `spread` | int | 8 | 单大流 fan-out lane 上限（自动 cap 到 conns） |
| `dlconns` | int | -1 | 下载/上传池分离：-1=auto(conns/2)，0=共享 |
| `migrate` | int | 1 | 卡 lane 整体迁移开关 |
| `train` | int | 0 | 空闲训练帧秒数（0=关） |
| `slotbase` | int | 0 | 多实例槽位起始（slotbase+conns ≤ 64） |
| `pprof` | string | 空 | `127.0.0.1:6060` 开 pprof |
| `admin` | string | 空 | server 侧：admin 管理 API 监听地址（如 `127.0.0.1:6070`）；空 = 关闭。契约要求只绑 loopback |
| `admin_token` | string | 空 | server 侧：可选 Bearer token；设置后所有 admin `/v1/*` 必须带 `Authorization: Bearer <token>`（§3.6） |

- **未知字段容忍**：新版 zhenhanvn 多写的字段旧核心直接忽略（不会启动失败）。
- **优先级**：显式 CLI flag > 配置文件 > 默认值（纯 CLI 旧用法完全不变）。
- 客户端最小配置（zhenhanvn 应生成的形态）：

```json
{
  "mode": "client",
  "listen": "127.0.0.1:10808",
  "server": ["example.com:8443", "example.com:8444"],
  "uuid": "your-uuid",
  "conns": 16,
  "spread": 8,
  "dlconns": -1,
  "migrate": 1,
  "loglevel": "info"
}
```

### 3.2 ProfileItem → config 映射（CoreConfigZhDataService）

| ProfileItem | config 字段 | 说明 |
|---|---|---|
| `Address` + `Port` | `server[0]` | 主服务器；`ProtoExtra.zhdata.servers` 存在时**追加**其余条目 |
| `Password` | `uuid` | |
| `ProtoExtra`（JSON） | `conns/spread/dlconns/migrate/slotbase/train/loglevel/servers` | 高级参数；示例 `{"zhdata":{"servers":["h2:p2"],"conns":16}}` |
| 本地端口分配 | `listen` | 由 zhenhanvn 按现有端口池分配（避免冲突），核心绑定失败会 error 退出并打印原因 |
| `Remark` | —（不参与） | |

分享链接（Fmt 层）：`zhdata://<uuid>@<host1>:<port1>[,<host2>:<port2>][?conns=16&spread=8#remark]`

### 3.3 本地代理接口（mixed）

- 一个 `listen` 端口同时接受：
  - **SOCKS5**（首字节 `0x05`；CONNECT only，IPv4/IPv6/域名，远端解析）
  - **HTTP 代理**（首字节 A–Z：`CONNECT host:port`；绝对 URI `http://host[:port]/path` 自动改写 origin-form；origin-form 用 `Host:` 兜底；非法请求回 400）
- `127.0.0.1` 与 `::1` 自动成对监听（`localhost` 解析差异不影响）。
- 系统代理、可用性测试（SOCKS 探测）、下载全部指向该端口 → **`GetLocalPort(EInboundProtocol.socks)` 直接返回 `listen` 端口即可**。
- 不支持 SOCKS BIND / UDP associate（浏览器/HTTP 场景无影响；xray 对应能力同样缺席时可接受）。

### 3.4 用户策略与设备数限制

v0.15.0 起客户端 handshake 携带 `device_id`。服务端把“设备”定义为：

```text
一个正在活跃的 zhdata 客户端进程实例
```

服务端内部 key：

```text
sessionKey = uuid + device_id + slot
streamKey  = uuid + device_id + sid
deviceKey  = uuid + device_id
```

`device_id` 是每次进程启动随机生成的 16 字节 ID，只保存在内存中：

- 不写入节点配置；
- 不写入分享 URI；
- 同一进程内所有 slot 共用；
- 进程退出后丢弃；
- 用户复制 `zhdata://...` 节点到另一进程/另一台机器会得到新的 device_id；
- 同一台机器开两个 zhenhanvn 进程会被算成两个设备实例。

服务端读取用户源时会读取可选的设备数策略：

```text
max_devices = 0    不限制设备数
max_devices > 0    同一 uuid 同时允许的活跃 device_id 数量
```

支持来源：

| 来源 | 写法 |
|---|---|
| txt | 每行 `uuid` 或 `uuid <max_devices>`，例如 `4ae0... 2` |
| JSON users | `{"users":[{"uuid":"...","max_devices":2}]}` |
| s-ui SQLite | 自动查找 `uuid` 表，同时读取可选列：`max_devices`、`max_clients`、`devices`、`device_limit` |

当同一 uuid 已存在的 device_id 重连时不增加设备数；当新的 device_id 超过限制时，服务端返回认证 reject reason `3`，客户端日志显示：

```text
zhdata device limit reached: this UUID has too many active devices
```

### 3.5 服务端启动参数说明（C498 当前测试节点）

同一个 zhdata 二进制既是客户端核心，也是服务端核心。客户端与服务端不是两套代码，差异只在运行模式和启动参数：

```text
客户端：-mode client -listen <本地代理口> -server <zhdata服务端地址列表> -uuid <UUID>
服务端：-mode server -listen <zhdata公开端口列表> -users <用户 key 源> -backend <后端出站方式>
```

zhenhanvn 目前启动客户端核心时使用 JSON 配置：

```text
bin/zhdata/zhdata.exe -c config.json
```

C498 测试节点目前服务端仍使用 systemd 命令行参数，不是 `-c config.json`。两者进入的是同一核心代码路径；JSON 配置只是 CLI 参数的另一种输入方式，优先级为：**显式 CLI 参数 > JSON 配置 > 默认值**。

C498 当前 systemd 主服务文件：

```ini
[Unit]
Description=zhdata-tunnel server (port 49888)
After=network.target

[Service]
WorkingDirectory=/opt/zhdata
ExecStart=/opt/zhdata/zhdata-tunnel-linux-amd64 -mode server -listen :49888,:49889,:49890,:49891,:49892,:49893,:49894,:49895,:49896,:49897,:49898,:49899,:49900,:49901,:49902,:49903 -users /etc/s-ui/private/s-ui.db,/opt/zhdata/users.txt -backend direct -v -max-clients 100
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

C498 当前 drop-in `/etc/systemd/system/zhdata.service.d/ports64.conf` 会把监听端口覆盖为 64 个端口：

```ini
[Service]
ExecStart=
ExecStart=/opt/zhdata/zhdata-tunnel-linux-amd64 -mode server -listen :49888,:49889,:49890,:49891,:49892,:49893,:49894,:49895,:49896,:49897,:49898,:49899,:49900,:49901,:49902,:49903,:49904,:49905,:49906,:49907,:49908,:49909,:49910,:49911,:49912,:49913,:49914,:49915,:49916,:49917,:49918,:49919,:49920,:49921,:49922,:49923,:49924,:49925,:49926,:49927,:49928,:49929,:49930,:49931,:49932,:49933,:49934,:49935,:49936,:49937,:49938,:49939,:49940,:49941,:49942,:49943,:49944,:49945,:49946,:49947,:49948,:49949,:49950,:49951 -users /etc/s-ui/private/s-ui.db,/opt/zhdata/users.txt -backend direct -v -max-clients 100
```

当前 C498 实际生效参数含义：

| 参数 | 当前值 | 含义 |
|---|---|---|
| `-mode server` | `server` | 以 zhdata 服务端模式运行，接受 zhenhanvn/zhdata 客户端握手 |
| `-listen` | `:49888`–`:49951` | 服务端监听 64 个 TCP 端口；每个端口对应一条独立公网 TCP 桶，用于抗 ISP 按流整形 |
| `-users` | `/etc/s-ui/private/s-ui.db,/opt/zhdata/users.txt` | 逗号分隔的用户 key 源；支持 s-ui SQLite 和 txt/json；只读、20 秒 mtime 热加载 |
| `-backend direct` | `direct` | 服务端收到隧道 stream 的目标地址后直接出站；也可填 socks 地址，例如 `127.0.0.1:12345` |
| `-max-clients 100` | 100 | v0.15.0 起作为兼容别名使用；等价于服务端全局并发 zhdata tunnel session 上限。不是客户数，也不是终端数。150 台终端 × 32 slot = 4800 条并发 session，因此当前 100 会拒绝其余连接，需设到至少 4800，并留重连/幽灵连接余量。 |
| `-max-sessions <N>` | 推荐新增 | 服务端全局并发 tunnel session 资源上限；`<=0` 表示不限制。若同时存在 `-max-clients`，显式 `-max-sessions` 优先。 |
| `-v` | 开启 | 旧 verbose 开关。当前代码中等价于 `loglevel=debug`；如果不显式设置 `-loglevel`，服务会输出 debug 级日志 |

注意：

- 当前 C498 服务仍使用 `-v`，所以日志级别是 **debug**，不是默认的 info。
- 如果希望服务端保持“默认简单 + 明确故障提示”，建议改成 `-loglevel info` 或 `-loglevel warning`，去掉 `-v`。
- 当前 `users` 里 `/etc/s-ui/private/s-ui.db` 不存在时，服务端会 warning 但仍继续从 `/opt/zhdata/users.txt` 加载 key；当前 C498 实际加载 1 个 key。
- zhenhanvn 客户端默认基础 URI 是 `host:49888`，内部隐藏参数会自动展开为 `49888..49919`，共 32 个端口；C498 服务端监听到 `49951`，多余端口暂不影响。

### 3.6 admin 管理 API（v0.15.1 起，仅 server 模式）

权威契约：s-ui 仓库 `docs/zhdata-admin-api.md` §1。本节是核心侧实现说明，端点/语义与契约一致。

启用：`-admin 127.0.0.1:6070`（或 config `"admin"`）+ 可选 `-admin-token <token>`（或 `"admin_token"`）。地址为空 = 关闭。server 模式且地址非空即起 `http.Server`；**绑定失败视为启动失败并退出**（systemd 能立刻感知管理面没起来）。契约要求只绑 loopback，由部署方保证。

鉴权：设置 token 后**所有** `/v1/*` 必须带 `Authorization: Bearer <token>`（常数时间比较），缺失/错误回 401；未设置 token 则不鉴权。

| 端点 | 语义 | 成功返回 |
|---|---|---|
| `GET /v1/version` | 核心版本（与 `-version` 同串，仍满足 `v?(?<version>\d+\.\d+\.\d+)`） | `{"version":"0.15.3"}` |
| `PUT /v1/users` | **全量替换 API 用户集**：与文件/sqlite 用户源取并集；同名 uuid 的策略 **API 覆盖文件**（`max_devices` 0=不限，可解除文件限制）；空数组清空 API 集（不影响文件源）；内存中原子换表 | `{"ok":true,"keys":N}` |
| `GET /v1/stats` | 自进程启动累计计数：`sessions_active`=当前活跃会话；`users[]` 按 uuid 给出 `devices`（当前活跃 device_id 数，与 `max_devices` 同一张表）、`up`/`down` 隧道字节 | `{"ts":..,"sessions_active":..,"users":[{"uuid":..,"devices":..,"up":..,"down":..}]}` |
| `GET /v1/sessions[?limit=n]` | v0.15.3 起：面板「入站连接列表」数据源。一条隧道会话一行，**按 `started_at` 倒序（新的在前）**；`device`=握手里的 16B device_id（32 位小写 hex；zhv4/5/6 旧客户端没有 device_id → 空串），`slot`=握手 slot，`remote`=隧道对端 `ip:port`，`started_at`=会话建立时刻（**unix 秒**），`relay`=该会话是否走 `-backend zhdata` 管道（relay 进程里全是 true，直连进程全是 false）。数据源是 `a.conns`（键 `uuid:device:slot`）+ 活管道集合。`?limit=n` 只取最近 n 条：**默认 50，越界钳到 10/100，非法值按默认**；`limit` 只是上限，**不是分页游标**（同一 query 每次返回同一批头部，面板轮询不会抖动）。`total` **始终等于全量活动会话数**，不受 limit 影响。**GET only、无副作用**；per-session 字节数 P0 不提供（用 `GET /v1/stats` 的按 uuid 累计） | `{"ts":<unix>,"total":<活动会话总数>,"sessions":[{"uuid":"..","device":"<32hex>","slot":<int>,"remote":"ip:port","started_at":<unix>,"relay":<bool>}]}` |
| `POST /v1/users/kick` | 按 uuid 切断该用户全部会话（先发 `tGoaway` 再关 socket，客户端按既有退避重连）；uuid 大小写不敏感；空 uuid = 400（P0 只按 uuid 踢）；relay 进程里同时切断该 uuid 的活管道（§3.7） | `{"kicked":N}` |
| `POST /v1/reload` | 契约 §1.5（原 P1）已实现：立即重读文件/sqlite 用户源，绕过 mtime/size 签名检查；API 用户集不动 | `{"ok":true,"keys":N}` |
| `GET /v1/relay` | v0.15.2 起：读 relay 状态（§3.7）。`pipes` = 当前活着的字节管道数（一条下游会话 = 一条上游 TCP） | `{"enabled":bool,"upstream":["h:p",..],"pipes":N}` |
| `PUT /v1/relay` | v0.15.2 起：热更新 relay。`upstream`（可选）全量替换上游列表（校验 `host:port`；空列表 + `enabled=true` → 400）；`enabled`（可选）开关 relay；两者都不给 → 400；**只影响新连接**，已有管道不动（关掉 relay 后旧管道继续跑到自然结束） | `{"ok":true,"upstream":N,"enabled":bool}` |

实现要点（admin 是叠加的**第三用户源**）：

- 现有 `users`（txt/json/s-ui sqlite，20s mtime 热加载）**行为不变**；`PUT /v1/users` 之后握手与策略读取看到的是「文件 ∪ API」的合并视图，`snapshot()`/`policy()` 原子切换。
- `reloadIfChanged` 只盯文件签名，**不会**把 API 用户集重载清掉。
- 替换 users 不强制断已有会话：被移除 uuid 的**新**连接握手被拒（reject），既有会话等 kick 或自然结束（契约 §1.2）。
- `up` = 已写入后端的字节（pumpUp + 拨号前补写），`down` = 后端读入并送往客户端的字节（downlink pump），与 s-ui「按周期采样求增量」语义配套。zhv4-6 旧客户端同样按 uuid 计入（其握手也认证 uuid）；理论上无 uuid 的会话计入保留 key `-`，不丢字节。
- `keys` 报告**本次 PUT 写入的 API 用户集大小**；合并后的总数见 `[users]` 日志行与 `GET /v1/stats`。
- 错误返回统一 `{"error":"..."}`（400/401/404/405），不影响契约里成功响应的形状。
- 日志 tag `admin`：info = 启动监听 / PUT / kick / reload；warning = 绑定或 serve 异常。kick 动作本身在 `[server] [warning] kick uuid=... cut N session(s)`（默认 info 级可见）。
- `GET /v1/sessions` 与 `GET /v1/stats` 是**只读快照**，不加锁副作用、不产生日志行（面板轮询）；`sessions_active` 与 `sessions` 行数同源（`a.conns` + relay 管道），所以面板两边不会看到互相矛盾的数。kick 之后两侧同时归零（relay 管道也在同一刀里）。

客户端模式传 `-admin` 只打一条 warning 并忽略，不改变 client 行为。

### 3.7 字节级 relay（`-backend zhdata`，v0.15.2 起，仅 server 模式）

用途：让一台 zhdata 服务端只做**入口 + 控制面**，把隧道原样转给下一跳 zhdata 服务端（多级中转 / 单入口多出口），relay 本机不需要落地后端。

启用：`-backend zhdata -upstream h1:p1,h2:p2`（或 config `"backend":"zhdata"` + `"upstream":"h1:p1,h2:p2"`）。启动日志 `[relay] [info] byte-level relay to [..] (round-robin per connection)`；`backend=zhdata` 而 upstream 为空/无合法地址 → **启动失败退出**（systemd 可感知）。

语义（不可降级）：

- **控制面留在 relay**：48B 握手（16B salt + 32B 密文块）由 relay 按用户 key 暴力解密，得到 uuid / slot / device_id / 版本，认证、`max_sessions`、`max_devices`、每 IP 握手失败限频、decoy splice 全部在 relay 上执行，与直连服务端一字不差（认证失败回 32B 明文 reject：reason 0=通用、1=认证失败、2=会话数超限、3=设备数超限）。
- **数据面字节直通**：握手之后 relay 把「48B 头 + 后续全部密文字节」原样搬运，**从不解析/改写/注入任何帧**。协议帧格式对 relay 完全透明，因此 zhv5 分桶、多 lane spread、重传等特性跨 relay 天然可用（无需 relay 认识它们）。relay 也不代答 tAck/tGoaway。
- **设备引用计数**：relay 复用同一张 `(uuid, device_id)` 表（与直连服务端同一实现）；超过 `max_devices` 时明文 reject reason=3，日志后缀 `[relay]`。
- **上游选择**：每条**下游连接**按 round-robin 取一个 upstream（同一下游会话生命周期内固定一条上游 TCP）。上游拨号失败（超时/拒绝）→ 直接关下游（P0 不做上游重试，客户端按既有退避重连）。
- **统计口径**：两个方向的原始字节都计入 `addTraffic(uuid, ...)`，所以 relay 与最终服务端两侧 `GET /v1/stats` 都会统计到这段流量（口径是**密文字节**，含 48B 握手，不等于 payload）。
- **kick**：`POST /v1/users/kick` 除切会话外还会切断该 uuid 的 relay 管道，日志 `[server] [warning] kick uuid=.. cut N session(s), M relay pipe(s)`。因为 relay 不能往管道里注入 `tGoaway`，kick 的 relay 语义就是**断管道**：客户端 1–2s 退避后重连（重新握手），所以面板要**先移除用户/凭据再 kick**，否则会立刻重连回来。
- **关掉 relay**（`PUT /v1/relay {"enabled":false}`）：只影响**新连接**——新连接在本机落地，后端按 `direct` 直连目标（`backend=zhdata` 不是 socks 地址，不会被当成 socks 拨号）；已建立的管道不动，跑到自然结束。
- **容量口径（P0 已知代价）**：一条下游会话 = 一条上游 TCP，relay 上不另建会话池，所以上游的连接数按下游 `conns` 放大；relay 侧带宽/流量限速未实现（P1）。

示例：

```bash
# 入口机（relay）
zhdata -mode server -listen 49888 -users /opt/zhdata/users.txt \
       -backend zhdata -upstream 1.2.3.4:49888,1.2.3.4:49889 \
       -admin 127.0.0.1:6070 -admin-token <token>
# 热更新上游 / 查看状态
curl -X PUT -H "Authorization: Bearer <token>" -H 'Content-Type: application/json' \
     -d '{"upstream":["1.2.3.4:49888"],"enabled":true}' http://127.0.0.1:6070/v1/relay
curl -H "Authorization: Bearer <token>" http://127.0.0.1:6070/v1/relay
```

## 4. 更新与发布契约

| 项 | 要求（已满足） |
|---|---|
| 仓库 | `chentyson/zhdata`（GitHub Releases） |
| tag | 纯 semver `v0.15.3` 形式（`zhdata-v0.12.4` 这类前缀 tag 会解析成 0.0.0） |
| 资产名（精确） | `zhdata-windows-64.zip`、`zhdata-windows-arm64.zip`、`zhdata-linux-64.zip`、`zhdata-linux-arm64.zip`、`zhdata-macos-64.zip`、`zhdata-macos-arm64.zip`（amd64→64） |
| zip 布局 | 单顶层目录 `zhdata/` + 二进制；v2rayN 解压按**文件名扁平化**进 `bin/zhdata/`，两种布局都兼容；文件名不得含 `geo`（会被丢弃） |
| CI | 仓库 `.github/workflows/release.yml`：push `v*.*.*` tag → 六平台构建 → 六 zip 上传（正式 release，非 draft，`/latest` 重定向依赖正式 release） |
| 必改 C# 点 | `ParseDownloadUrl` 加 `case ECoreType.zhdata`（default 会抛 ArgumentException，只用单 `{0}`）；`IsCheckUpdateSupported => true`；`GetCheckUpdateCoreTypes()` 加入；`Global.CoreUrls` 登记 |

## 5. 日志接口（zhenhanvn 日志窗）

**有接口。** v0.13.0 起核心日志为**单行、带级别标记**的结构化文本；v0.14.0 起加入明确握手/后端故障提示和客户端解析 IP 日志，全部走 **stdout**（stderr 基本为空），`displayLog=true`（zhdata 为正式协议节点时自动为 true）时被 `ProcessService.BeginOutputReadLine` 逐行送入消息视图，实时可见。

### 5.1 行格式

```text
2026-11-19 09:54:12 [client] [warning] slot 3 taken over (goaway), reconnect in 2s
└─── 时间戳(log.LstdFlags) ───┘└─ tag ─┘└── level ──┘└── 消息 ──────────────────────
```

- **tag**（子系统）：`main` `client` `server` `session` `users` `pprof` `admin` `relay` `dbg`
- **level**：`error` `warning` `info` `debug`

zhenhanvn 侧解析/过滤正则（可直接用于着色/过滤）：

```regex
^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[(main|client|server|session|users|pprof|admin|relay|dbg)\] \[(error|warning|info|debug)\] (.*)$
```

### 5.2 级别语义（默认 `info`，生产安静）

| 级别 | 事件 |
|---|---|
| `error` | 致命错误（配置错误、启动失败、退出前）；pumpUp 写失败；会话写队列死锁 |
| `warning` | 拨号失败/退避重试、握手/UUID 认证失败、设备数超限、session 总数超限、GOAWAY 接管、握手限频、流重置、后端 DNS/连接失败、后端无响应、队列满、用户源重载异常、kick 切断会话（`[server] kick uuid=... cut N`）、admin 绑定/serve 异常 |
| `info` | 核心启动监听（client/server）、客户端本次进程 device_id、zhdata 服务端目标解析出的 IP 地址、会话建立、users 加载/热加载/`PUT /v1/users` 替换、admin API 启动监听、admin `POST /v1/reload` |
| `debug` | 每流生命周期（backend connected/done、session ended、recycle 换新、重传、bucket 细节）、原 `-v` 全部内部日志 |

`loglevel` 门控在**核心侧**完成：默认 `info` 时 debug 行不产生（零开销）；`debug` ≡ 旧 `-v`。环境变量 `ZHD_DEBUG=1` 追加协议帧级 trace（比 debug 更细，调试专用）。

**查询类端点不产生日志行**（`GET /v1/stats`、`GET /v1/sessions`、`GET /v1/relay`、`GET /v1/version`）：面板会按秒级轮询，日志只在状态真正变化时出现（会话建立/结束在 `info`/`debug`，kick/PUT/reload 在 `info`/`warning`），面板要「谁在线」请直接读 `GET /v1/sessions`（§3.6）。

### 5.3 样例（真实输出）

```text
2026-11-19 09:03:47 [server] [info] listening on [127.0.0.1:18555], users=1, backend=direct, max-sessions=10, max-clients=10
2026-11-19 09:03:47 [admin] [info] admin API listening on http://127.0.0.1:6070/v1/ (auth=true)
2026-11-19 09:03:49 [client] [info] mixed socks5+http listening on 127.0.0.1:10808 -> C498.example:49888 device=4c81ad5d31d34f8a4f6c19877d2461c4
2026-11-19 09:03:49 [client] [info] server target resolved target=C498.example:49888 ips=1.2.3.4,5.6.7.8
2026-11-19 09:03:49 [server] [info] session established uuid=smoke-uu device=4c81ad5d31d34f8a4f6c19877d2461c4 slot=0 from 127.0.0.1
2026-11-19 09:03:52 [server] [warning] device limit reached, rejecting uuid=smoke-uu device=7d2001b6c4d5a6b78e9f001122334455 slot=0 from 1.2.3.4 (devices=2 limit=2)
2026-11-19 09:03:52 [client] [warning] slot 0 dial/auth failed: zhdata device limit reached: this UUID has too many active devices (check server address, DNS, port, firewall, UUID; retry in 4s)
2026-11-19 09:03:55 [main] [error] client mode requires -listen, -server and -uuid (or config listen/server/uuid)
2026-11-19 09:04:01 [client] [debug] bucket done sid=7 cl=20971520 dur=2.113s
```

### 5.4 zhenhanvn 侧建议

- 消息视图已逐行实时显示（无需改动即可用）；若要按级别过滤/着色，用 §5.1 正则提取 level 即可。
- `stderr` 仍会被 `SaveLogError` 落盘（核心偶尔在极早期（logger 初始化前）可能有一条 stderr，不影响级别体系）。
- 日志窗"错误"高亮规则：匹配 `[error]`；"警告"匹配 `[warning]`。

## 6. 功能边界（v0.15.3）

| 能力 | 状态 |
|---|---|
| 入站连接列表 | **v0.15.3 起**：`GET /v1/sessions[?limit=n]` 一条隧道会话一行（uuid/device/slot/remote `ip:port`/started_at/relay），按建立时间**倒序**（新的在前），`limit` 默认 50（钳到 10–100，非分页），`total` 恒为全量活动会话数；只读无副作用；relay 进程列出它的活管道（`relay=true`）。per-session 字节数 = P0 未做（面板要按会话流量需在核心加 per-session 计数，P1） |
| 多级中转 / relay | **v0.15.2 起**：`-backend zhdata -upstream h:p[,h:p]`，控制面（认证/设备数/会话数/decoy/统计）在 relay，数据面字节直通不解析帧；`GET/PUT /v1/relay` 热更新上游与开关（§3.7）。relay 侧带宽限速 = P1 未做 |
| 作为 Go 库嵌入 | **v0.15.2 起**：仓库是 module `github.com/chentyson/zhdata`，s-ui 可直接 `import "github.com/chentyson/zhdata"`，用 `NewClient` + `DialContext` 把隧道当 `net.Conn` 用（§8）；CLI 构建路径改为 `./cmd/zhdata` |
| 速度/流量统计 | **admin API（v0.15.1 起）**：`GET /v1/stats` 暴露 `sessions_active` 与按 uuid 的 `devices`/`up`/`down` 累计字节（§3.6），s-ui 面板按周期采样求增量入库。zhenhanvn 速度栏对 zhdata 仍恒 0（GUI 未接 admin API，与 hysteria2/brook 等一致，非错误）。clash_api `ws://:StatePort2/traffic` + `IsRunningCore` 判定方案仍未实现（backlog） |
| 用户/凭据管理 | **admin API（v0.15.1 起）**：`PUT /v1/users` 推送 API 用户集（与文件/sqlite 源并集、API 策略覆盖）、`POST /v1/users/kick` 按 uuid 切会话、`POST /v1/reload` 强制重载（§3.6）。文件 `users` 热加载仍是独立可用路径，面板不可用时也能工作 |
| 批量测速"测试全部" | 现只批 xray/sing_box，zhdata 节点被跳过（C# 侧加批量需 `LoadCoreConfigSpeedtest` 支持 zhdata 配置） |
| TUN | zhdata 无 tun 入站；TUN 模式需走 sing-box 前置 → zhdata socks 的链式方案（框架侧设计） |
| 组节点（PolicyGroup/ProxyChain） | 白名单只含 Xray/sing_box（框架侧） |
| 协议兼容 | magic `zhv7`：握手携带每次进程随机 `device_id`；服务端显式回认证 OK/reject（reject 带 reason）。服务端同时接受 `zhv6`/`zhv5`/`zhv4` 旧客户端。新客户端需配 v0.15.0 服务端。 |

## 7. zhenhanvn 开发者验收清单

1. 添加协议 zhdata 节点（uuid/address/port），保存。
2. 启动：消息窗出现 `[client] [info] mixed socks5+http listening on 127.0.0.1:<port> -> <server> device=<16-byte hex>`。
3. 核心缺失提示下载 → 解压出 `bin/zhdata/zhdata.exe` → 能启动。
4. 检查更新页出现 zhdata 并可升级（tag `v*.*.*`）。
5. `curl -x socks5h://127.0.0.1:<port> http://example.com` 与 `-x http://127.0.0.1:<port>` 均 200。
6. 系统代理（socks 与 http 两种模式）切到 zhdata 节点可上网。
7. 切换节点/停止：旧 zhdata 进程消失（Kill），无残留。
8. `-loglevel warning`（或 config）时 debug/info 行消失；`ZHD_DEBUG=1` 出现帧 trace。
9. 断线：出现 `[warning] slot N dial ... retry`，自动恢复。
10. 导入/导出 `zhdata://<uuid>@<host>:<port>` 链接。

## 8. 作为 Go 库嵌入（v0.15.2 起）

v0.15.2 把仓库拆成 module `github.com/chentyson/zhdata`：根目录是 `package zhdata`（可嵌入库 + 全部单测），`./cmd/zhdata` 是 CLI 入口（`main.go` + flag/JSON 解析）。**CLI 行为与旧单文件版完全一致**（flag、config、日志、systemd 路径都不变，只是构建命令变成 `go build ./cmd/zhdata`）。纯 Go，无 CGO（sqlite 走 `modernc.org/sqlite`）。

s-ui 侧引用（本仓库为同机兄弟目录，开发期用 replace）：

```
require github.com/chentyson/zhdata v0.0.0
replace github.com/chentyson/zhdata => ../v2rayn/zhdata-src
```

### 8.1 客户端嵌入 API（面板「把 zhdata 当 net.Conn 用」）

```go
type ClientConfig struct {
	Servers  []string // 必填：zhdata 服务端 host:port（多端口 = 多 slot）
	UUID     string   // 必填
	Conns    int      // <=0 → 16
	Spread   int      // <=0 → 8
	DLPool   int      // 下载/上传池分离：-1=auto(conns/2)，0=共享
	SlotBase int      // 多实例槽位起始（slotbase+conns ≤ 64）
	Train    int      // 空闲训练帧秒数（0=关）
	DeviceID []byte   // 空 → 随机 16B；非空必须 16B
	LogLevel string   // none|error|warning|info|debug（空=info）
	Logf     func(string, ...any) // 可选：日志转发，行前缀 "zhdata[tag/level]"
}

func NewClient(cfg *ClientConfig) (*Client, error)
func (c *Client) DialContext(ctx context.Context, host string, port int) (net.Conn, error)
func (c *Client) Close()
func (c *Client) DeviceID() []byte
```

`DialContext` 走**内存流直连**：不经过本地 socks/http 口，直接把一条流挂进完整客户端栈（多会话、lane spread、窗口/重传、zhv5 分桶、GOAWAY takeover、空闲回收全部照用）。语义要点：

- 返回的 `net.Conn` 实现 **`CloseWrite()`**：只半关 app→tunnel 方向，产生 `tClose(finSeq)`，目标看到 FIN 但仍可继续回数据（HTTP 半关闭、`io.Copy` + CloseWrite 模式可用）；反方向（目标结束）由服务端 `tClose` 让 app 的 `Read` 返回 `io.EOF`。
- `ctx` 取消/超时：立即回收该流并返回 `ctx.Err()`；目标连不上：服务端 `tReset`，错误串含 `could not connect`；`Close()` 之后 `DialContext` 返回 `ErrClientClosed`（幂等，可重复调用）。
- 域名由**服务端解析**（客户端不做 DNS），IP 目标直填。

```go
cli, err := zhdata.NewClient(&zhdata.ClientConfig{
	Servers: []string{"example.com:49888", "example.com:49889"},
	UUID:    "your-uuid", Conns: 16, Spread: 8, DLPool: -1,
	LogLevel: "info", Logf: func(format string, a ...any) { log.Printf(format, a...) },
})
defer cli.Close()
conn, err := cli.DialContext(ctx, "api.example.com", 443) // 走隧道
```

### 8.2 服务端 / relay 嵌入 API

```go
type ServerOptions struct {
	Listen, Users, Backend, Upstream string // Backend "zhdata" = 字节级 relay（§3.7）
	MaxClients, MaxSessions int
	Decoy, AdminAddr, AdminToken string
	Verbose bool
}
func NewServer(o *ServerOptions) (*Server, error) // 起监听 + admin API + relay；失败即返回 error
func (s *Server) Close()                          // 停监听 / admin / 后台循环
```

`ClientOptions`/`NewClientApp`（`Listen: ""` = 不建本地代理口）与 `ClientApp.DeviceIDHex()` 也已导出，便于面板直接跑一个 client 实例而不暴露本地口。

### 8.3 日志接入

```go
zhdata.SetLogLevel(zhdata.LevelInfo)         // 或 ParseLogLevel("info")
zhdata.SetLogSink(func(level int, tag, msg string) { /* 落库/日志窗 */ })
```

`SetLogSink` 是**进程级**开关：设置后所有 tag 的日志不再写 stderr，而是走回调（`tag` 见 §5.1，含 `relay`）；传 `nil` 恢复 stderr。`LogLevelName()`/`LogErr/LogWarn/LogInfo/LogDbg` 也导出，便于面板统一格式化。
