package zhdata

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

// TestLogLevelGating verifies the logAt level gate: at lvlWarn the info/debug
// channels emit nothing while error/warning pass through, and lines carry the
// "[tag] [level] " prefix zhenhanvn's log window filters on. Captures the log
// output (stdout in production) into a buffer and restores it after.
func TestLogLevelGating(t *testing.T) {
	oldLevel := logLevel
	oldOut, oldFlags := log.Writer(), log.Flags()
	defer func() {
		logLevel = oldLevel
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	}()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0) // keep assertions on the message itself

	logLevel = lvlWarn
	buf.Reset()
	logErr("client", "dial failed: %v", os.ErrDeadlineExceeded)
	logWarn("client", "slot %d taken over (goaway)", 3)
	logInfo("client", "session established slot=%d", 0)
	logDbg("client", "open ack slot=%d sid=%d", 0, 7)
	out := buf.String()
	if !strings.Contains(out, "[client] [error] dial failed: "+os.ErrDeadlineExceeded.Error()+"\n") {
		t.Errorf("missing error line at warn level: %q", out)
	}
	if !strings.Contains(out, "[client] [warning] slot 3 taken over (goaway)") {
		t.Errorf("missing warning line at warn level: %q", out)
	}
	if strings.Contains(out, "[info]") || strings.Contains(out, "[debug]") {
		t.Errorf("info/debug must be silent at warn level: %q", out)
	}

	logLevel = lvlError
	buf.Reset()
	logWarn("server", "accept: %v", os.ErrClosed)
	logErr("server", "boom")
	out = buf.String()
	if strings.Contains(out, "[warning]") {
		t.Errorf("warning must be silent at error level: %q", out)
	}
	if !strings.Contains(out, "[server] [error] boom") {
		t.Errorf("error line missing at error level: %q", out)
	}

	logLevel = lvlDebug
	buf.Reset()
	logInfo("users", "loaded %d keys", 2)
	logDbg("dbg", "socksAccept:enter")
	out = buf.String()
	if !strings.Contains(out, "[users] [info] loaded 2 keys") {
		t.Errorf("info line missing at debug level: %q", out)
	}
	if !strings.Contains(out, "[dbg] [debug] socksAccept:enter") {
		t.Errorf("debug line missing at debug level: %q", out)
	}
}

// TestParseLogLevel lives in appconfig_test.go (lead's -loglevel flag test);
// only the gating behavior is covered here.
