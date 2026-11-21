package zhdata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------- embedded outbound client API (s-ui integration surface) ----------------

// byte path over the exported API: DialContext -> write -> CloseWrite (tClose
// finSeq) -> the server CloseWrites the target -> response arrives, then EOF.
func TestClientAPIDialAndCloseWrite(t *testing.T) {
	const uuid = "libapi-dial-0001"
	const body = "lib-pong-body"
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer target.Close()
	host, port := splitHostPort(t, target.URL)

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	c, err := NewClient(&ClientConfig{Servers: []string{srv.Addr()}, UUID: uuid, Conns: 2, Spread: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if id := c.DeviceID(); len(id) != deviceIDLen || allZero(id) {
		t.Fatalf("empty DeviceID must become random 16B, got %x", id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := c.DialContext(ctx, host, int(port))
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()

	cw, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("DialContext conn must implement CloseWrite() error")
	}
	if _, err = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err = cw.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	// CloseWrite must not kill the downlink: the response still has to arrive.
	if err = conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response after CloseWrite: %v", err)
	}
	if !strings.Contains(string(got), body) {
		t.Fatalf("want %q in response, got %q", body, got)
	}
	if n := srv.estab.Load(); n < 1 {
		t.Fatalf("tunnel session for the embedded stream never came up (estab=%d)", n)
	}
}

// targets may be domain names (the zhdata server resolves them); a refused
// target must surface as a dial error (server tReset), not a hang.
func TestClientAPIBadTargetFails(t *testing.T) {
	const uuid = "libapi-badtgt-02"
	live, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := live.Addr().String()
	live.Close() // now this port refuses connections

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	c, err := NewClient(&ClientConfig{Servers: []string{srv.Addr()}, UUID: uuid, Conns: 1, Spread: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	host, port := splitHostPort(t, "http://"+deadAddr)
	conn, err := c.DialContext(ctx, host, int(port))
	if err == nil {
		conn.Close()
		t.Fatal("dialing a refused target must fail")
	}
	if !strings.Contains(err.Error(), "could not connect") {
		t.Fatalf("want 'could not connect', got %v", err)
	}
}

// ctx governs the wait for the tunnel: no session -> ctx wins, no hang.
func TestClientAPIContextAndClosed(t *testing.T) {
	const uuid = "libapi-ctx-0003"
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close() // no zhdata server ever answers

	c, err := NewClient(&ClientConfig{Servers: []string{deadAddr}, UUID: uuid, Conns: 1, Spread: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err = c.DialContext(ctx, "example.com", 80); err == nil {
		t.Fatal("dial without any live session must fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("ctx timeout was not honoured (waited %v)", d)
	}

	c.Close()
	if _, err = c.DialContext(context.Background(), "example.com", 80); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("DialContext after Close: want ErrClientClosed, got %v", err)
	}
}

// validation + documented defaults (Conns<=0 -> 16, Spread<=0 -> 8).
func TestClientAPIConfigDefaultsAndErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  *ClientConfig
		want string
	}{
		{"nil config", nil, "ClientConfig required"},
		{"no uuid", &ClientConfig{Servers: []string{"127.0.0.1:1"}}, "UUID required"},
		{"no servers", &ClientConfig{UUID: "u"}, "Servers required"},
		{"short device id", &ClientConfig{Servers: []string{"127.0.0.1:1"}, UUID: "u", DeviceID: []byte{1, 2}}, "16 bytes"},
		{"bad loglevel", &ClientConfig{Servers: []string{"127.0.0.1:1"}, UUID: "u", LogLevel: "chatty"}, "LogLevel"},
	}
	for _, tc := range cases {
		c, err := NewClient(tc.cfg)
		if err == nil {
			if c != nil {
				c.Close()
			}
			t.Fatalf("%s: want error, got nil", tc.name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: want error containing %q, got %v", tc.name, tc.want, err)
		}
	}

	const uuid = "libapi-def-0004"
	srv, err := newServerApp(serverConfig{listen: "127.0.0.1:0", usersPath: usersFileWith(t, uuid), backend: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	c, err := NewClient(&ClientConfig{Servers: []string{srv.Addr(), " "}, UUID: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.app.cfg.conns != 16 {
		t.Fatalf("Conns<=0 must default to 16, got %d", c.app.cfg.conns)
	}
	if c.app.cfg.spread != 8 {
		t.Fatalf("Spread<=0 must default to 8, got %d", c.app.cfg.spread)
	}
	if c.app.listener != nil {
		t.Fatal("embedded client must not bind a local proxy listener")
	}
	if c.app.Addr() != "" {
		t.Fatalf("Addr() in listener-less mode must be empty, got %q", c.app.Addr())
	}
	var fixed [deviceIDLen]byte
	copy(fixed[:], "fixed-device-id-01")
	c2, err := NewClient(&ClientConfig{Servers: []string{srv.Addr()}, UUID: uuid, DeviceID: fixed[:]})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if got := c2.DeviceID(); !equalBytes(got, fixed[:]) {
		t.Fatalf("explicit DeviceID ignored: %x", got)
	}
}

// Logf routes core log lines into the embedder's logger; LogLevel gates them.
func TestClientAPILogSink(t *testing.T) {
	oldLevel := LogLevel()
	t.Cleanup(func() {
		SetLogSink(nil)
		SetLogLevel(oldLevel)
	})

	var mu sync.Mutex
	var lines []string
	const uuid = "libapi-log-0005"
	c, err := NewClient(&ClientConfig{
		Servers:  []string{"127.0.0.1:1"}, // never connects: only startup lines
		UUID:     uuid,
		Conns:    1,
		Spread:   1,
		LogLevel: "debug",
		Logf: func(format string, v ...any) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, fmt.Sprintf(format, v...))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(lines) > 0
	}, "Logf received no core log lines")
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "zhdata[") {
		t.Fatalf("Logf lines missing the zhdata tag prefix: %q", joined)
	}
	if LogLevel() != LevelDebug {
		t.Fatalf("LogLevel \"debug\" must set the gate, got %d", LogLevel())
	}
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
