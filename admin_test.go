package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// ---- admin API test helpers ----

// adminHTTP performs one admin API call; token="" sends no Authorization header.
func adminHTTP(t *testing.T, method, url, token, body string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

func adminJSON(t *testing.T, method, url, token, body string) (int, map[string]any) {
	t.Helper()
	code, raw := adminHTTP(t, method, url, token, body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s %s: %v (body %q)", method, url, err, raw)
	}
	return code, out
}

// adminTestServer serves the admin handler over httptest (no real -admin bind).
func adminTestServer(t *testing.T, app *serverApp, token string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer((&adminAPI{app: app, token: token}).handler())
	t.Cleanup(s.Close)
	return s
}

type statsUserRow struct {
	UUID    string `json:"uuid"`
	Devices int    `json:"devices"`
	Up      int64  `json:"up"`
	Down    int64  `json:"down"`
}

type statsPayload struct {
	TS             int64          `json:"ts"`
	SessionsActive int            `json:"sessions_active"`
	Users          []statsUserRow `json:"users"`
}

func statsOf(t *testing.T, base, token string) statsPayload {
	t.Helper()
	code, raw := adminHTTP(t, http.MethodGet, base+"/v1/stats", token, "")
	if code != http.StatusOK {
		t.Fatalf("stats: %d %s", code, raw)
	}
	var s statsPayload
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("stats decode: %v (body %q)", err, raw)
	}
	return s
}

func statsRow(s statsPayload, uuid string) (statsUserRow, bool) {
	for _, u := range s.Users {
		if u.UUID == uuid {
			return u, true
		}
	}
	return statsUserRow{}, false
}

// ---- tests ----

func TestAdminVersionAndBearerAuth(t *testing.T) {
	const uuid = "admin-uu-001"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, uuid),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "s3cr3t").URL

	// contract: with -admin-token set, EVERY /v1/* route requires the bearer token
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/version", ""},
		{http.MethodPut, "/v1/users", `{"users":[]}`},
		{http.MethodGet, "/v1/stats", ""},
		{http.MethodPost, "/v1/users/kick", `{"uuid":"x"}`},
		{http.MethodPost, "/v1/reload", ""},
	} {
		if code, _ := adminJSON(t, tc.method, base+tc.path, "", tc.body); code != http.StatusUnauthorized {
			t.Fatalf("%s %s without token: want 401, got %d", tc.method, tc.path, code)
		}
		if code, _ := adminJSON(t, tc.method, base+tc.path, "wrong-token", tc.body); code != http.StatusUnauthorized {
			t.Fatalf("%s %s with wrong token: want 401, got %d", tc.method, tc.path, code)
		}
	}

	code, v := adminJSON(t, http.MethodGet, base+"/v1/version", "s3cr3t", "")
	if code != http.StatusOK || v["version"] != version {
		t.Fatalf("GET /v1/version: %d %v", code, v)
	}
	// zhenhanvn parses the version with `v?(?<version>\d+\.\d+\.\d+)` out of
	// the -version line; the admin API reports the same string.
	m := regexp.MustCompile(`v?(\d+\.\d+\.\d+)`).FindStringSubmatch("zhdata " + version)
	if m == nil || m[1] != version {
		t.Fatalf("version %q not parseable by zhenhanvn regex", version)
	}

	// no token configured -> no auth required
	srv2, err := newServerApp(serverConfig{listen: "127.0.0.1:0", usersPath: usersFileWith(t, uuid), backend: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	base2 := adminTestServer(t, srv2, "").URL
	if code, _ := adminHTTP(t, http.MethodGet, base2+"/v1/version", "", ""); code != http.StatusOK {
		t.Fatalf("admin without -admin-token must be open: got %d", code)
	}
}

// server mode + non-empty -admin must bring up the http.Server for real.
func TestAdminServerModeStartsListener(t *testing.T) {
	const uuid = "admin-uu-002"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, uuid),
		backend:     "direct",
		maxSessions: 10,
		adminAddr:   "127.0.0.1:0",
		adminToken:  "tok",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr := srv.AdminAddr()
	if addr == "" {
		t.Fatal("admin listener not started in server mode")
	}
	if code, raw := adminHTTP(t, http.MethodGet, "http://"+addr+"/v1/version", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("unauth over real admin bind: want 401, got %d %s", code, raw)
	}
	code, raw := adminHTTP(t, http.MethodGet, "http://"+addr+"/v1/version", "tok", "")
	if code != http.StatusOK || !strings.Contains(string(raw), `"version":"`+version+`"`) {
		t.Fatalf("version over real admin bind: %d %s", code, raw)
	}
}

func TestAdminPutUsersUnionAndPolicyOverride(t *testing.T) {
	txtp := filepath.Join(t.TempDir(), "users.txt")
	if err := os.WriteFile(txtp, []byte("file-a 2\nfile-b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   txtp,
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL
	ks := srv.ks

	if k := ks.snapshot(); len(k) != 2 || !contains(k, "file-a") || !contains(k, "file-b") {
		t.Fatalf("file baseline keys: %v", k)
	}

	// PUT replaces the API set: effective keys = union (uuids lowercased)
	code, resp := adminJSON(t, http.MethodPut, base+"/v1/users", "",
		`{"users":[{"uuid":"File-A","max_devices":9},{"uuid":"api-1","max_devices":1}]}`)
	if code != http.StatusOK || resp["ok"] != true || resp["keys"] != float64(2) {
		t.Fatalf("PUT /v1/users: %d %v", code, resp)
	}
	k := ks.snapshot()
	if len(k) != 3 || !contains(k, "file-a") || !contains(k, "file-b") || !contains(k, "api-1") {
		t.Fatalf("union wrong: %v", k)
	}
	// policy merge: API overrides the file entry for the same uuid
	if p := ks.policy("file-a"); p.MaxDevices != 9 {
		t.Fatalf("API policy must override file limit for file-a: %+v", p)
	}
	if p := ks.policy("api-1"); p.MaxDevices != 1 {
		t.Fatalf("api-1 policy: %+v", p)
	}
	if p := ks.policy("file-b"); p.MaxDevices != 0 {
		t.Fatalf("file-b must stay unlimited: %+v", p)
	}

	// replacement drops previous API users; max_devices 0 = unlimited, and it
	// still overrides the file limit (removes it)
	code, resp = adminJSON(t, http.MethodPut, base+"/v1/users", "", `{"users":[{"uuid":"file-a","max_devices":0}]}`)
	if code != http.StatusOK || resp["keys"] != float64(1) {
		t.Fatalf("second PUT: %d %v", code, resp)
	}
	if p := ks.policy("file-a"); p.MaxDevices != 0 {
		t.Fatalf("API max_devices=0 must lift the file limit 2: %+v", p)
	}
	k = ks.snapshot()
	if contains(k, "api-1") || !contains(k, "file-a") || !contains(k, "file-b") {
		t.Fatalf("replacement semantics wrong: %v", k)
	}

	// empty array clears the API set without touching the file source
	code, resp = adminJSON(t, http.MethodPut, base+"/v1/users", "", `{"users":[]}`)
	if code != http.StatusOK || resp["keys"] != float64(0) {
		t.Fatalf("clear API set: %d %v", code, resp)
	}
	if k = ks.snapshot(); len(k) != 2 || !contains(k, "file-a") || !contains(k, "file-b") {
		t.Fatalf("file keys must survive an empty API set: %v", k)
	}

	// a file hot-reload must not wipe the API user set
	if code, _ = adminHTTP(t, http.MethodPut, base+"/v1/users", "", `{"users":[{"uuid":"api-keep"}]}`); code != http.StatusOK {
		t.Fatalf("seed API user: %d", code)
	}
	if err := os.WriteFile(txtp, []byte("file-a 2\nfile-b\nfile-c\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !ks.reloadIfChanged() {
		t.Fatal("expected file hot reload after users.txt change")
	}
	k = ks.snapshot()
	if !contains(k, "api-keep") || !contains(k, "file-c") || len(k) != 4 {
		t.Fatalf("API set lost across file reload: %v", k)
	}
}

// The API user set must be effective in the auth path, including its
// max_devices policy (union + override are not just bookkeeping).
func TestAdminAPIOnlyUserConnectsAndIsDeviceLimited(t *testing.T) {
	const fileUUID = "admin-file-uu"
	const apiUUID = "admin-api-uu"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, fileUUID),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL
	if code, _ := adminHTTP(t, http.MethodPut, base+"/v1/users", "",
		`{"users":[{"uuid":"`+apiUUID+`","max_devices":1}]}`); code != http.StatusOK {
		t.Fatalf("PUT /v1/users: %d", code)
	}

	cli1, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     apiUUID,
		deviceID: "0000000000000000000000000000c001",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli1.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "API-only user session established")

	cli2, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     apiUUID,
		deviceID: "0000000000000000000000000000c002",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()

	// give the second device time to dial and be rejected by max_devices=1
	time.Sleep(1500 * time.Millisecond)
	srv.mu.Lock()
	active := srv.active
	devices := len(srv.devices[apiUUID])
	srv.mu.Unlock()
	if active != 1 || devices != 1 {
		t.Fatalf("API max_devices=1 not enforced: active=%d devices=%d", active, devices)
	}
}

func TestAdminStatsCounts(t *testing.T) {
	const uuid = "admin-stats-uu"
	body := strings.Repeat("x", 2048)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("stats-" + body))
	}))
	defer target.Close()
	host, port := splitHostPort(t, target.URL)

	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, uuid),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	// empty stats shape must match the contract (users: [], not null)
	s0 := statsOf(t, base, "")
	if s0.SessionsActive != 0 || s0.Users == nil || len(s0.Users) != 0 || s0.TS <= 0 {
		t.Fatalf("empty stats shape: %+v", s0)
	}

	cli, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		deviceID: "0000000000000000000000000000d001",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "stats test session")

	s1 := statsOf(t, base, "")
	if s1.SessionsActive != 1 {
		t.Fatalf("sessions_active: want 1, got %d", s1.SessionsActive)
	}
	r1, ok := statsRow(s1, uuid)
	if !ok || r1.Devices != 1 {
		t.Fatalf("expected one active device for uuid: %+v", s1.Users)
	}

	got, err := httpThroughTunnel(t, cli.Addr(), host, port)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "stats-"+body) {
		t.Fatalf("bad tunnel body %q", got)
	}

	// up = bytes forwarded to the backend (the 18-byte "GET / HTTP/1.0\r\n\r\n"
	// request), down = backend response bytes toward the client; counters are
	// cumulative since start, so a poll-until is enough.
	var s2 statsPayload
	waitFor(t, func() bool {
		s2 = statsOf(t, base, "")
		row, ok := statsRow(s2, uuid)
		return ok && row.Up >= 18 && row.Down >= 2048
	}, "stats up/down counted")
	row, _ := statsRow(s2, uuid)
	if row.Devices != 1 {
		t.Fatalf("devices during live session: %+v", row)
	}
	if s2.SessionsActive != 1 {
		t.Fatalf("sessions_active after traffic: %d", s2.SessionsActive)
	}
}

func TestAdminKickCutsAllSessions(t *testing.T) {
	const uuid = "admin-kick-uu"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, uuid),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	clients := make([]*clientApp, 0, 2)
	for _, dev := range []string{"0000000000000000000000000000e001", "0000000000000000000000000000e002"} {
		cli, err := newClientApp(clientConfig{
			listen:   "127.0.0.1:0",
			server:   srv.Addr(),
			uuid:     uuid,
			deviceID: dev,
		})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, cli)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	waitFor(t, func() bool { return srv.estab.Load() >= 2 }, "two devices established")

	// uuid matching is case-insensitive like the key store
	code, resp := adminJSON(t, http.MethodPost, base+"/v1/users/kick", "", `{"uuid":"`+strings.ToUpper(uuid)+`"}`)
	if code != http.StatusOK || resp["kicked"] != float64(2) {
		t.Fatalf("POST /v1/users/kick: %d %v", code, resp)
	}
	waitFor(t, func() bool { return statsOf(t, base, "").SessionsActive == 0 }, "kick cut every session of the uuid")

	// a uuid with no sessions kicks 0; empty uuid is a client error (P0: per-uuid only)
	code, resp = adminJSON(t, http.MethodPost, base+"/v1/users/kick", "", `{"uuid":"nobody-here"}`)
	if code != http.StatusOK || resp["kicked"] != float64(0) {
		t.Fatalf("kick unknown uuid: %d %v", code, resp)
	}
	if code, _ = adminJSON(t, http.MethodPost, base+"/v1/users/kick", "", `{"uuid":""}`); code != http.StatusBadRequest {
		t.Fatalf("empty uuid: want 400, got %d", code)
	}
}

func TestAdminMethodAndPathErrors(t *testing.T) {
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, "admin-err-uu"),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	if code, _ := adminJSON(t, http.MethodGet, base+"/v1/users", "", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/users: want 405, got %d", code)
	}
	if code, _ := adminJSON(t, http.MethodPost, base+"/v1/version", "", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/version: want 405, got %d", code)
	}
	if code, _ := adminJSON(t, http.MethodPut, base+"/v1/users", "", `{"users":[{"uuid":`); code != http.StatusBadRequest {
		t.Fatalf("malformed json: want 400, got %d", code)
	}
	if code, _ := adminHTTP(t, http.MethodGet, base+"/v1/nope", "", ""); code != http.StatusNotFound {
		t.Fatalf("unknown path: want 404, got %d", code)
	}
	// PUT with no "users" key behaves as the empty set (full-replace semantics)
	if code, resp := adminJSON(t, http.MethodPut, base+"/v1/users", "", `{}`); code != http.StatusOK || resp["keys"] != float64(0) {
		t.Fatalf(`PUT {} : %d %v`, code, resp)
	}
}
