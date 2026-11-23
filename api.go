package zhdata

// Public API surface for embedding zhdata (s-ui panel outbound, the CLI,
// tests). The command in ./cmd/zhdata is a thin wrapper over these symbols:
// flags/JSON config resolution stays in the command, protocol behaviour
// lives here.

import (
	"encoding/hex"
	"sync/atomic"
)

// Version is the core version. `zhdata -version` prints "zhdata <Version>"
// and GET /v1/version reports the same string; both stay parseable by
// zhenhanvn's `v?(?<version>\d+\.\d+\.\d+)`.
//
// v0.15.5 is log/behaviour only (protocol unchanged, magic zhv7): a session we
// closed ourselves now reports WHY (idle recycle / stopping / local write err /
// ctrl queue jam) instead of Go's bare "use of closed network connection", so
// the idle-pool refresh no longer prints "session ended ... (retry in 1s)" every
// 5s while nothing is running; genuine session deaths are logged at warning;
// the idle recycle prefers streamless slots (v0.11.9 stream-loss vs v0.12.4
// stale-bucket balance).
const Version = "0.15.5"

// Reserved in-tunnel account query defaults (v0.15.4; -query-host/-query-url,
// see zhdata-核心接口文档.md §3.8). The CLI uses these as flag defaults; the
// query handler lives in query.go.
const (
	DefaultQueryHost = "query.zhdata"
	DefaultQueryURL  = "http://127.0.0.1:62444/62444/query?key=%s"
)

// Log level tokens for SetLogLevel — same gate as the CLI -loglevel flag.
const (
	LevelNone  = lvlNone
	LevelError = lvlError
	LevelWarn  = lvlWarn
	LevelInfo  = lvlInfo
	LevelDebug = lvlDebug
)

// SetLogLevel sets the package-wide log gate (LevelNone silences everything).
// Embedders that want their own logging call SetLogSink instead of touching
// std log output.
func SetLogLevel(level int) { logLevel = level }

// LogLevel reports the current log gate.
func LogLevel() int { return logLevel }

// LogLevelName is the human name of the current gate ("none" when silent).
func LogLevelName() string {
	if logLevel == lvlNone || logLevel < 0 || logLevel >= len(lvlName) {
		return "none"
	}
	return lvlName[logLevel]
}

// ParseLogLevel accepts none|error|warning|warn|info|debug (case-insensitive).
func ParseLogLevel(s string) (int, bool) { return parseLogLevel(s) }

// SetLogSink routes every package log line to fn (level token, tag, formatted
// message) instead of std log. Pass nil to restore stdout logging. The gate in
// SetLogLevel still applies. Process-wide: one embedder, one sink.
func SetLogSink(fn func(level int, tag, msg string)) {
	if fn == nil {
		logSink.Store(nil)
		return
	}
	logSink.Store(&fn)
}

// LogErr/LogWarn/LogInfo/LogDbg write one line at the given level (used by the
// CLI for its own startup messages so they share tag/level formatting).
func LogErr(tag, format string, v ...any)  { logAt(lvlError, tag, format, v...) }
func LogWarn(tag, format string, v ...any) { logAt(lvlWarn, tag, format, v...) }
func LogInfo(tag, format string, v ...any) { logAt(lvlInfo, tag, format, v...) }
func LogDbg(tag, format string, v ...any)  { logAt(lvlDebug, tag, format, v...) }

var logSink atomic.Pointer[func(level int, tag, msg string)]

// ---------------- server ----------------

// ServerOptions configures a zhdata server (tunnel endpoint).
type ServerOptions struct {
	Listen      string // comma-separated public ports (required)
	Users       string // comma-separated txt/json/sqlite key sources (required)
	Backend     string // "direct" (default) | socks addr | "zhdata" = byte-level relay upstream
	Upstream    string // with Backend "zhdata": comma-separated upstream zhdata host:port (round-robin per connection)
	MaxClients  int    // compat alias for the session cap
	MaxSessions int    // concurrent tunnel sessions; <=0 = unlimited
	Decoy       string // host:port to splice non-protocol traffic
	AdminAddr   string // admin API listen address (empty = off)
	AdminToken  string // optional Bearer token for admin /v1/*
	QueryHost   string // reserved account-query hostname (DefaultQueryHost; empty = off)
	QueryURL    string // account source URL for the query handler, %s = uuid (empty = off)
	Verbose     bool
}

// Server is a running zhdata server. Type alias of the internal serverApp so
// its methods (Addr/Close/AdminAddr/...) are reachable.
type Server = serverApp

// NewServer binds the tunnel listeners (and the admin API when requested) and
// starts serving. Call Close to stop.
func NewServer(o *ServerOptions) (*Server, error) {
	return newServerApp(serverConfig{
		listen:      o.Listen,
		usersPath:   o.Users,
		backend:     o.Backend,
		upstream:    o.Upstream,
		maxClients:  o.MaxClients,
		maxSessions: o.MaxSessions,
		decoy:       o.Decoy,
		adminAddr:   o.AdminAddr,
		adminToken:  o.AdminToken,
		queryHost:   o.QueryHost,
		queryURL:    o.QueryURL,
		verbose:     o.Verbose,
	})
}

// ---------------- socks/mixed client stack ----------------

// ClientOptions configures the tunnel client (the client half of the CLI).
// Listen "" means no local proxy listener: streams are then created through
// Client.DialContext instead of SOCKS connections.
type ClientOptions struct {
	Listen   string // local mixed socks5+http listen addr; "" = no listener
	Server   string // comma-separated zhdata server host:port (multi-port slots)
	UUID     string
	DeviceID string // 16-byte hex; empty = random per process
	Conns    int
	Spread   int
	Train    int
	Migrate  int
	DLConns  int
	SlotBase int
	Verbose  bool
}

// ClientApp is the multi-session tunnel client (socks side). Alias of the
// internal clientApp so Addr/Close stay reachable.
type ClientApp = clientApp

func NewClientApp(o *ClientOptions) (*ClientApp, error) {
	return newClientApp(clientConfig{
		listen:   o.Listen,
		server:   o.Server,
		uuid:     o.UUID,
		deviceID: o.DeviceID,
		conns:    o.Conns,
		spread:   o.Spread,
		train:    o.Train,
		migrate:  o.Migrate,
		dlconns:  o.DLConns,
		slotbase: o.SlotBase,
		verbose:  o.Verbose,
	})
}

// DeviceIDHex reports the per-process device id (hex) the client authenticates
// with — useful for panel logs and device-limit debugging.
func (a *clientApp) DeviceIDHex() string { return hex.EncodeToString(a.deviceID[:]) }
