#!/usr/bin/env bash
# ===== zhdata-tunnel 服务端安装脚本（CentOS/Stream9/Rocky/Alma，需 root） =====
# 用法: 把本目录整个 dist/ 目录(含 zhdata-tunnel-linux-amd64 和 server/users.txt)上传到服务器,
#   scp -r dist root@C498.uwintech.com:/root/zhdata-dist
# 然后在服务器上执行:
#   bash /root/zhdata-dist/server/install-server.sh
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/../zhdata-tunnel-linux-amd64"

mkdir -p /opt/zhdata
cp -f "$BIN" /opt/zhdata/zhdata-tunnel-linux-amd64
cp -f "$HERE/users.txt" /opt/zhdata/users.txt
chmod +x /opt/zhdata/zhdata-tunnel-linux-amd64

cat >/etc/systemd/system/zhdata.service <<'EOF'
[Unit]
Description=zhdata-tunnel server (ports 49888-49895)
After=network.target
[Service]
WorkingDirectory=/opt/zhdata
ExecStart=/opt/zhdata/zhdata-tunnel-linux-amd64 -mode server -listen :49888,:49889,:49890,:49891,:49892,:49893,:49894,:49895 -users /etc/s-ui/private/s-ui.db,/opt/zhdata/users.txt -backend direct -max-clients 100
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
EOF
# 用户 key 来源(逗号分隔,自动识别 txt/sqlite,并集去重,20s 内热加载):
#   /etc/s-ui/private/s-ui.db    s-ui sqlite(自适应查找带 uuid 列的表;未装 s-ui 则自动忽略)
#   /opt/zhdata/users.txt        手工兜底名单
# 如需接入 s-ui/sing-box 分流: 把上面 ExecStart 的 -backend direct 改为 -backend 127.0.0.1:12345
# (并在 s-ui 面板添加 127.0.0.1:12345 无密码 socks 入站)

systemctl daemon-reload
systemctl enable zhdata >/dev/null 2>&1
systemctl restart zhdata

# 防火墙(有 firewalld 才执行)
if command -v firewall-cmd >/dev/null 2>&1; then
  for p in 49888 49889 49890 49891 49892 49893 49894 49895; do firewall-cmd --permanent --add-port=$p/tcp; done && firewall-cmd --reload
fi
# BBR(内核>=4.9 才有; CentOS7 自带 3.10 无效, 需装 elrepo kernel-ml 或换系统)
sysctl -w net.ipv4.tcp_congestion_control=bbr 2>/dev/null || true

systemctl status zhdata --no-pager | head -6
