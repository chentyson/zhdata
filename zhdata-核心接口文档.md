# zhdata 核心接口文档（zhenhanvn 一级核心接入）

版本：zhdata **v0.15.0**（2026-11-20）· 适用 zhenhanvn（v2rayN-7.24.9 fork）
配套：《zhdata-一级核心接入方案.md》（总体方案）、《开发方案.md》§11（契约核对记录）

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
| 版本探测 | `zhdata -version` → exit 0，stdout 输出 `zhdata 0.15.0`（满足 `v?(?<version>\d+\.\d+\.\d+)`） |
| 前台常驻 | 不 fork、不守护；启动 100ms 内 `HasExited` 即失败 |
| 停止 | zhenhanvn 用 `Process.Kill()`（Windows=TerminateProcess），**不发信号**；核心无状态文件，强杀无损。reload = 杀进程后重启 |
| 单进程 | 核心不自管子进程（Windows Kill 不带子进程树） |
| CLI 兼容 | 全部配置字段都有同名 CLI flag（`-mode/-listen/-server/-uuid/-users/-backend/-max-clients/-max-sessions/-decoy/-v/-device-id/-conns/-spread/-train/-migrate/-dlconns/-slotbase/-pprof/-loglevel`），显式 CLI > 配置文件 > 默认 |

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
| `backend` | string | `direct` | server 侧：`direct` 或 socks 地址（对接 sing-box socks inbound） |
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

## 4. 更新与发布契约

| 项 | 要求（已满足） |
|---|---|
| 仓库 | `chentyson/zhdata`（GitHub Releases） |
| tag | 纯 semver `v0.15.0` 形式（`zhdata-v0.12.4` 这类前缀 tag 会解析成 0.0.0） |
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

- **tag**（子系统）：`main` `client` `server` `session` `users` `pprof` `dbg`
- **level**：`error` `warning` `info` `debug`

zhenhanvn 侧解析/过滤正则（可直接用于着色/过滤）：

```regex
^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[(main|client|server|session|users|pprof|dbg)\] \[(error|warning|info|debug)\] (.*)$
```

### 5.2 级别语义（默认 `info`，生产安静）

| 级别 | 事件 |
|---|---|
| `error` | 致命错误（配置错误、启动失败、退出前）；pumpUp 写失败；会话写队列死锁 |
| `warning` | 拨号失败/退避重试、握手/UUID 认证失败、设备数超限、session 总数超限、GOAWAY 接管、握手限频、流重置、后端 DNS/连接失败、后端无响应、队列满、用户源重载异常 |
| `info` | 核心启动监听（client/server）、客户端本次进程 device_id、zhdata 服务端目标解析出的 IP 地址、会话建立、users 加载/热加载 |
| `debug` | 每流生命周期（backend connected/done、session ended、recycle 换新、重传、bucket 细节）、原 `-v` 全部内部日志 |

`loglevel` 门控在**核心侧**完成：默认 `info` 时 debug 行不产生（零开销）；`debug` ≡ 旧 `-v`。环境变量 `ZHD_DEBUG=1` 追加协议帧级 trace（比 debug 更细，调试专用）。

### 5.3 样例（真实输出）

```text
2026-11-19 09:03:47 [server] [info] listening on [127.0.0.1:18555], users=1, backend=direct, max-sessions=10, max-clients=10
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

## 6. 功能边界（v0.15.0）

| 能力 | 状态 |
|---|---|
| 速度/流量统计 | **无 API**：zhenhanvn 速度栏对 zhdata 恒 0（与 hysteria2/brook 等一致，非错误）。可选方案：核心暴露 clash_api `ws://:StatePort2/traffic` + `IsRunningCore` 加 zhdata 判定（未实现，列入 backlog） |
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
