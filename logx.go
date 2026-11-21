package zhdata

import (
	"fmt"
	"log"
	"strings"
)

// Log level interface for zhenhanvn's log window.
//
// Line format (what the GUI receives on stdout):
//
//	2026-01-02 03:04:05 [client] [warning] slot 3 taken over (goaway), ...
//	└── log.LstdFlags timestamp ─┘└─ tag ─┘└─ level ─┘└── message ───────────
//
// zhenhanvn can filter/color lines with:
//
//	^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[(client|server|session|users|pprof|admin)\] \[(error|warning|info|debug)\]
//
// Level gate: default "info" (production quiet). "debug" == legacy -v: per-stream
// lifecycle (backend connected/done, session ended, recycle redial) plus a.logf
// internals. ZHD_DEBUG=1 additionally turns on protocol-frame tracing.
const (
	lvlNone  = -1
	lvlError = 0
	lvlWarn  = 1
	lvlInfo  = 2
	lvlDebug = 3
)

var logLevel = lvlInfo

// parseLogLevel accepts none|error|warning|warn|info|debug (case-insensitive).
// "none" is the zhenhanvn "logging disabled" level: the core stays silent.
func parseLogLevel(s string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "none":
		return lvlNone, true
	case "error":
		return lvlError, true
	case "warning", "warn":
		return lvlWarn, true
	case "info":
		return lvlInfo, true
	case "debug":
		return lvlDebug, true
	}
	return lvlInfo, false
}

var lvlName = []string{"error", "warning", "info", "debug"}

func logAt(lvl int, tag, format string, v ...any) {
	if logLevel == lvlNone || lvl > logLevel {
		return
	}
	if fn := logSink.Load(); fn != nil {
		(*fn)(lvl, tag, fmt.Sprintf(format, v...))
		return
	}
	log.Printf("[%s] [%s] %s", tag, lvlName[lvl], fmt.Sprintf(format, v...))
}

func logErr(tag, format string, v ...any)  { logAt(lvlError, tag, format, v...) }
func logWarn(tag, format string, v ...any) { logAt(lvlWarn, tag, format, v...) }
func logInfo(tag, format string, v ...any) { logAt(lvlInfo, tag, format, v...) }
func logDbg(tag, format string, v ...any)  { logAt(lvlDebug, tag, format, v...) }
