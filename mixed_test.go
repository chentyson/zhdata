package zhdata

// End-to-end tests for the Mixed local listener (SOCKS5 + HTTP proxy on one
// port), mirroring the harness of TestEndToEnd in zhdata_test.go: server app
// + client app + httptest backend, dialed through the client's local port.

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mixedBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + i%26)
	}
	return b
}

// mixedApps starts server + client + httptest backend (any GET returns
// body) and waits for one tunnel session. Returns the client app and the
// backend's "127.0.0.1:port" address.
func mixedApps(t *testing.T, uuid string, body []byte) (*clientApp, string) {
	t.Helper()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body)
	}))
	t.Cleanup(target.Close)

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	cli, err := newClientApp(clientConfig{
		listen: "127.0.0.1:0",
		server: srv.Addr(),
		uuid:   uuid,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)

	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")
	return cli, strings.TrimPrefix(target.URL, "http://")
}

// TestMixedConnect: HTTP CONNECT through the mixed port. The 200 reply must
// be sent locally and everything after it relayed raw: the test speaks plain
// HTTP to the httptest backend over the CONNECTed tunnel.
func TestMixedConnect(t *testing.T) {
	body := mixedBody(64 << 10)
	cli, target := mixedApps(t, "mixed-connect-user", body)

	c, err := net.Dial("tcp", cli.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(15 * time.Second))

	fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("CONNECT status: %v", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("CONNECT rejected: %q", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("CONNECT reply headers: %v", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if _, err = io.WriteString(c, "GET / HTTP/1.0\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("CONNECT body mismatch: got %d bytes, want %d", len(got), len(body))
	}
}

// TestMixedGetAbsoluteURI: plain GET with an absolute-URI request line
// (http://127.0.0.1:<port>/path). The listener must rewrite the request line
// to origin-form and replay the rewritten head (plus any already-buffered
// bytes) into the tunnel; the response body must arrive whole.
func TestMixedGetAbsoluteURI(t *testing.T) {
	body := mixedBody(300 << 10) // spans many 16KB tunnel frames
	cli, target := mixedApps(t, "mixed-get-user", body)

	c, err := net.Dial("tcp", cli.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(20 * time.Second))

	fmt.Fprintf(c, "GET http://%s/path HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body mismatch: got %d bytes, want %d", len(got), len(body))
	}
}

// TestMixedOriginFormHostHeader: origin-form request (no absolute URI): the
// target must fall back to the Host: header and the request must be relayed
// unchanged.
func TestMixedOriginFormHostHeader(t *testing.T) {
	body := mixedBody(16 << 10)
	cli, target := mixedApps(t, "mixed-origin-form-user", body)

	c, err := net.Dial("tcp", cli.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(15 * time.Second))

	fmt.Fprintf(c, "GET /path HTTP/1.1\r\nHost: %s\r\n\r\n", target)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: got %d bytes, want %d", len(got), len(body))
	}
}

// TestMixedBadFirstByte: a first byte that is neither 0x05 (SOCKS5) nor an
// ASCII method letter must close the connection (same as the old behavior),
// quickly — not after the handshake timeout.
func TestMixedBadFirstByte(t *testing.T) {
	cli, _ := mixedApps(t, "mixed-bad-first-user", nil)

	c, err := net.Dial("tcp", cli.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	t0 := time.Now()
	if _, err = c.Write([]byte{0xff, 0xfe, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	n, err := c.Read(make([]byte, 1))
	if err == nil {
		t.Fatalf("expected immediate close, got %d bytes", n)
	}
	if d := time.Since(t0); d > 4*time.Second {
		t.Fatalf("close took %v (expected well under the handshake timeout)", d.Round(time.Millisecond))
	}
}

// TestMixedSocks5Regression: SOCKS5 (0x05) traffic on the same mixed port
// must keep working end-to-end (the peeked 0x05 byte must not be lost).
func TestMixedSocks5Regression(t *testing.T) {
	body := mixedBody(32 << 10)
	cli, target := mixedApps(t, "mixed-socks-user", body)
	host, port := splitHostPort(t, "http://"+target)

	c, err := socksDial(cli.Addr(), atypDomain, []byte(host), port)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err = c.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "200 OK") {
		t.Fatalf("missing 200 status: %q", firstN(string(got), 200))
	}
	if !bytes.Contains(got, body) {
		t.Fatalf("SOCKS5 regression: body missing/truncated (%d bytes read, want >= %d)", len(got), len(body))
	}
}
