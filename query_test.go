package zhdata

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------------- in-tunnel account query (query.zhdata, v0.15.4) ----------------

// queryThroughTunnel does what zhenhanvn does: a plain HTTP GET for
// http://query.zhdata/62444/query?key=<uuid> through the client's socks port.
// The key= value is decorative: the server answers for the session's own uuid.
func queryThroughTunnel(t *testing.T, socksAddr, uuid string) string {
	t.Helper()
	// A cold client has no session yet: on a loaded machine the first tOpen
	// can be torn down before the handshake completes and the read comes back
	// empty, so retry until a framed answer arrives.
	deadline := time.Now().Add(15 * time.Second)
	for {
		raw, err := queryOnce(socksAddr, uuid)
		if raw != "" || time.Now().After(deadline) {
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func queryOnce(socksAddr, uuid string) (string, error) {
	c, err := socksDial(socksAddr, atypDomain, []byte("query.zhdata"), 80)
	if err != nil {
		return "", err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	req := "GET /62444/query?key=" + uuid + " HTTP/1.1\r\nHost: query.zhdata\r\nConnection: close\r\n\r\n"
	if _, err = c.Write([]byte(req)); err != nil {
		return "", err
	}
	b, err := io.ReadAll(c)
	if len(b) == 0 {
		return "", nil // torn down early; the caller retries
	}
	return string(b), err
}

// queryTestResp mirrors the JSON contract the GUI parses (lowercase,
// case-sensitive names; extra/missing fields tolerated).
type queryTestResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Name             string `json:"name"`
		Expiry           int64  `json:"expiry"`
		ExpiryStr        string `json:"expiry_str"`
		UnlimitedTime    bool   `json:"unlimited_time"`
		Volume           int64  `json:"volume"`
		VolumeStr        string `json:"volume_str"`
		UnlimitedTraffic bool   `json:"unlimited_traffic"`
		Used             int64  `json:"used"`
		UsedStr          string `json:"used_str"`
		Up               int64  `json:"up"`
		Down             int64  `json:"down"`
		Devs             int    `json:"devs"`
		Email            string `json:"email"`
		Enable           *bool  `json:"enable"`
	} `json:"data"`
}

// parseQueryResponse checks the HTTP/1.1 framing the GUI's HTTP client
// requires, then decodes the body.
func parseQueryResponse(t *testing.T, raw string) queryTestResp {
	t.Helper()
	head, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("no HTTP header terminator in %q", raw)
	}
	if !strings.HasPrefix(head, "HTTP/1.1 200 OK\r\n") {
		t.Fatalf("not an HTTP/1.1 200 response: %q", raw)
	}
	for _, h := range []string{"Content-Type: application/json", "Connection: close"} {
		if !strings.Contains(head, h) {
			t.Fatalf("missing %q header: %q", h, head)
		}
	}
	clLine := ""
	for _, l := range strings.Split(head, "\r\n")[1:] {
		if strings.HasPrefix(strings.ToLower(l), "content-length:") {
			clLine = l
		}
	}
	if clLine == "" {
		t.Fatalf("missing Content-Length: %q", head)
	}
	cl, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(clLine, ":", 2)[1]))
	if err != nil || cl != len(body) {
		t.Fatalf("Content-Length %q vs body %d bytes: %q", clLine, len(body), body)
	}
	var q queryTestResp
	if err := json.Unmarshal([]byte(body), &q); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, body)
	}
	return q
}

// (a) account fields pushed via the admin API are answered for the session uuid
func TestQueryAccountFromAdminPushedFields(t *testing.T) {
	const uuid = "query-acct-uu-01"
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
		queryHost: queryHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	admin := adminTestServer(t, srv, "")
	if code, out := adminJSON(t, http.MethodPut, admin.URL+"/v1/users", "",
		`{"users":[{"uuid":"`+uuid+`","max_devices":0,"expiry":1780000000,"quota":107374182400,"used":12345678}]}`); code != http.StatusOK {
		t.Fatalf("PUT /v1/users: %d %v", code, out)
	}

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")

	q := parseQueryResponse(t, queryThroughTunnel(t, cli.Addr(), uuid))
	if q.Code != 200 {
		t.Fatalf("want code 200, got %d (%s)", q.Code, q.Msg)
	}
	if q.Data.Expiry != 1780000000 || q.Data.Volume != 107374182400 || q.Data.Used != 12345678 {
		t.Fatalf("pushed account fields not answered: %+v", q.Data)
	}
	if q.Data.UnlimitedTime || q.Data.UnlimitedTraffic {
		t.Fatalf("limited account flagged unlimited: %+v", q.Data)
	}
	if want := time.Unix(1780000000, 0).Format("2006-01-02 15:04:05"); q.Data.ExpiryStr != want {
		t.Fatalf("expiry_str: want %q, got %q", want, q.Data.ExpiryStr)
	}
	if !strings.Contains(q.Data.VolumeStr, "GB") || q.Data.UsedStr == "" {
		t.Fatalf("missing human quota strings: %+v", q.Data)
	}
	if q.Data.Devs != 1 {
		t.Fatalf("devs: %+v", q.Data)
	}

	// the query stream must be traffic-accounted like a real backend stream
	waitFor(t, func() bool {
		row, ok := statsRow(statsOf(t, admin.URL, ""), uuid)
		return ok && row.Up >= 64 && row.Down >= 256
	}, "query stream bytes counted in /v1/stats")
}

// (b) session uuid without account data and without -query-url -> code 404
func TestQueryNoAccountData404(t *testing.T) {
	const uuid = "query-404-uu-02"
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
		queryHost: queryHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")

	q := parseQueryResponse(t, queryThroughTunnel(t, cli.Addr(), uuid))
	if q.Code != http.StatusNotFound || q.Msg != "no account info for this uuid" {
		t.Fatalf("want 404 account answer, got %d %q", q.Code, q.Msg)
	}
}

// (c) -query-url: the upstream body is passed through verbatim (whatever code
// its own JSON carries); an unreachable source answers code 502.
func TestQueryUpstreamURLPassThrough(t *testing.T) {
	const uuid = "query-url-uu-03"
	const upBody = `{"code":404,"msg":"not found","data":{"name":"panel-user"}}`
	var gotKey string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("key")
		w.Write([]byte(upBody))
	}))
	defer up.Close()

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
		queryHost: queryHost,
		queryURL:  up.URL + "/62444/query?key=%s",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")

	raw := queryThroughTunnel(t, cli.Addr(), uuid)
	_, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok || body != upBody {
		t.Fatalf("upstream body not passed through verbatim: %q", raw)
	}
	if gotKey != uuid {
		t.Fatalf("account source must be fetched with the session uuid: key=%q", gotKey)
	}

	// unreachable source: refuse-fast loopback port -> code 502
	srv2, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
		queryHost: queryHost,
		queryURL:  "http://127.0.0.1:1/62444/query?key=%s",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	cli2, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv2.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()
	waitFor(t, func() bool { return srv2.estab.Load() >= 1 }, "second session established")

	q := parseQueryResponse(t, queryThroughTunnel(t, cli2.Addr(), uuid))
	if q.Code != http.StatusBadGateway || q.Msg != "account source unreachable" {
		t.Fatalf("want 502 unreachable answer, got %d %q", q.Code, q.Msg)
	}
}

// (d) a byte-level relay hop must pass the query stream through untouched so
// the FINAL server's query handler answers it.
func TestQueryThroughRelay(t *testing.T) {
	const uuid = "query-relay-uu-04"
	e := newRelayEnv(t, uuid, serverConfig{queryHost: queryHost}, serverConfig{})
	admin := adminTestServer(t, e.final, "")
	if code, out := adminJSON(t, http.MethodPut, admin.URL+"/v1/users", "",
		`{"users":[{"uuid":"`+uuid+`","expiry":1780000000,"quota":107374182400,"used":42}]}`); code != http.StatusOK {
		t.Fatalf("PUT /v1/users on final: %d %v", code, out)
	}

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return e.final.estab.Load() >= 1 }, "relay->final session established")

	q := parseQueryResponse(t, queryThroughTunnel(t, cli.Addr(), uuid))
	if q.Code != 200 || q.Data.Expiry != 1780000000 || q.Data.Used != 42 {
		t.Fatalf("relay query: want the final server's answer, got %d %+v", q.Code, q.Data)
	}
}

// (e) no regression: an ordinary domain through the same tunnel still dials
// normally; query.zhdata is answered locally (a real dial would have to
// resolve that name and fail).
func TestQueryHostDoesNotBreakNormalDial(t *testing.T) {
	const uuid = "query-ok-uu-05"
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong-after-query"))
	}))
	defer target.Close()
	host, port := splitHostPort(t, target.URL)

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
		queryHost: queryHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")

	body, err := httpThroughTunnel(t, cli.Addr(), host, port)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "pong-after-query") {
		t.Fatalf("ordinary domain through the same tunnel: %q", body)
	}

	q := parseQueryResponse(t, queryThroughTunnel(t, cli.Addr(), uuid))
	if q.Code != http.StatusNotFound {
		t.Fatalf("query.zhdata must be answered locally (no dial, no DNS): got %d %q", q.Code, q.Msg)
	}
}
