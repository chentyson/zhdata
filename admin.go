package zhdata

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Admin API (docs/zhdata-admin-api.md §1) — the management surface an s-ui
// panel pushes credentials and reads counters through. Server mode only, and
// only when -admin <addr> is non-empty (empty = off). The contract requires
// the operator to bind loopback (e.g. 127.0.0.1:6070); we bind exactly the
// address given. -admin-token, when set, guards EVERY /v1/* route with
// `Authorization: Bearer <token>`.
//
// Endpoints (all application/json):
//
//	GET  /v1/version      -> {"version":"0.15.2"}
//	PUT  /v1/users        -> replace the API user set; {"ok":true,"keys":N}
//	GET  /v1/stats        -> {"ts":..,"sessions_active":..,"users":[..]}
//	POST /v1/users/kick   -> {"kicked":N}
//	POST /v1/reload       -> force file-source reload now (§1.5 "optional P1")
//	GET  /v1/relay        -> {"enabled":..,"upstream":[..],"pipes":N}
//	PUT  /v1/relay        -> {"upstream":[..],"enabled":bool} -> {"ok":true,..}
//
// PUT /v1/users replaces only the *API user set*: the effective user set stays
// the union with the file/sqlite -users sources (existing users.txt setups are
// untouched), and an API max_devices overrides the file policy of the same
// uuid (0 = unlimited). Empty list = clear the API set. Sessions of removed
// users are NOT force-cut here: new connections are rejected at auth, existing
// ones keep running until kick/close (contract §1.2).

const adminBodyLimit = 8 << 20 // PUT /v1/users body cap

type adminAPI struct {
	app   *serverApp
	token string
}

// startAdmin binds the admin listener and serves the API until serverApp.Close.
// A bind failure aborts startup: an operator running with -admin must know the
// management plane is not up rather than discover it from a missing log line.
func (a *serverApp) startAdmin(addr, token string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logErr("admin", "listen %s: %v", addr, err)
		return err
	}
	api := &adminAPI{app: a, token: strings.TrimSpace(token)}
	a.adminLn = ln
	a.adminSrv = &http.Server{
		Handler:           api.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          log.New(adminLogWriter{}, "", 0), // route net/http noise into the [admin] tag
	}
	go func() {
		if err := a.adminSrv.Serve(ln); err != nil && err != http.ErrServerClosed && !a.stopped.Load() {
			logWarn("admin", "serve: %v", err)
		}
	}()
	logInfo("admin", "admin API listening on http://%s/v1/ (auth=%v)", ln.Addr().String(), api.token != "")
	return nil
}

// AdminAddr reports the resolved admin listener address ("" when admin is off).
// With -admin 127.0.0.1:0 this is how callers (and tests) learn the real port.
func (a *serverApp) AdminAddr() string {
	if a.adminLn == nil {
		return ""
	}
	return a.adminLn.Addr().String()
}

// adminLogWriter funnels net/http's internal error log into the logx system.
type adminLogWriter struct{}

func (adminLogWriter) Write(p []byte) (int, error) {
	logWarn("admin", "%s", strings.TrimSpace(string(p)))
	return len(p), nil
}

func (api *adminAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/version", api.requireAuth(api.handleVersion))
	mux.HandleFunc("/v1/users", api.requireAuth(api.handleUsersPut))
	mux.HandleFunc("/v1/users/kick", api.requireAuth(api.handleKick))
	mux.HandleFunc("/v1/stats", api.requireAuth(api.handleStats))
	mux.HandleFunc("/v1/reload", api.requireAuth(api.handleReload))
	mux.HandleFunc("/v1/relay", api.requireAuth(api.handleRelay))
	return mux
}

// requireAuth enforces the optional Bearer token (constant-time compare).
func (api *adminAPI) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if api.token == "" {
			next(w, r)
			return
		}
		const prefix = "Bearer "
		got := r.Header.Get("Authorization")
		if !strings.HasPrefix(got, prefix) ||
			subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got[len(prefix):])), []byte(api.token)) != 1 {
			writeAdminJSON(w, http.StatusUnauthorized, map[string]any{"error": "missing or invalid bearer token"})
			return
		}
		next(w, r)
	}
}

// GET /v1/version — the version string must stay parseable by
// zhenhanvn's `v?(?<version>\d+\.\d+\.\d+)` (same contract as -version).
func (api *adminAPI) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"version": Version})
}

type adminUserJSON struct {
	UUID       string `json:"uuid"`
	MaxDevices int    `json:"max_devices"`
}

type adminUsersReq struct {
	Users []adminUserJSON `json:"users"`
}

// PUT /v1/users — full replacement of the API user set (contract §1.2).
// "keys" reports the size of the replaced API set (what this request loaded);
// the effective set (union with file sources) is visible via GET /v1/stats
// and the [users] log lines.
func (api *adminAPI) handleUsersPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		methodNotAllowed(w, http.MethodPut)
		return
	}
	var req adminUsersReq
	dec := json.NewDecoder(io.LimitReader(r.Body, adminBodyLimit))
	if err := dec.Decode(&req); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json: " + err.Error()})
		return
	}
	users := make([]apiUser, 0, len(req.Users))
	for _, u := range req.Users {
		k := strings.ToLower(strings.TrimSpace(u.UUID))
		if k == "" || k == "null" {
			continue
		}
		users = append(users, apiUser{UUID: k, MaxDevices: u.MaxDevices})
	}
	apiN, total := api.app.ks.setAPIUsers(users)
	logInfo("admin", "PUT /v1/users: api=%d total=%d", apiN, total)
	writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true, "keys": apiN})
}

// POST /v1/users/kick — cut every session of one uuid (contract §1.4).
// P0 requires per-uuid only: an empty uuid is rejected rather than guessed at.
func (api *adminAPI) handleKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var req struct {
		UUID string `json:"uuid"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, adminBodyLimit))
	if err := dec.Decode(&req); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json: " + err.Error()})
		return
	}
	uuid := strings.ToLower(strings.TrimSpace(req.UUID))
	if uuid == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]any{"error": "uuid required (P0 kicks are per-uuid)"})
		return
	}
	n := api.app.kickUUID(uuid)
	logInfo("admin", "POST /v1/users/kick uuid=%s kicked=%d", short(uuid), n)
	writeAdminJSON(w, http.StatusOK, map[string]any{"kicked": n})
}

type adminUserStats struct {
	UUID    string `json:"uuid"`
	Devices int    `json:"devices"`
	Up      int64  `json:"up"`
	Down    int64  `json:"down"`
}

// GET /v1/stats — counters cumulative since process start (contract §1.3).
// devices reuses the per-uuid device table that max_devices enforces, so the
// panel sees exactly the number the limit compares against. uuids seen only
// in traffic (e.g. removed from the user set but already idle) still appear
// with their counters so the panel's sampling delta never loses bytes.
func (api *adminAPI) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	app := api.app
	rows := map[string]*adminUserStats{}
	app.traffMu.Lock()
	for uuid, c := range app.traffic {
		rows[uuid] = &adminUserStats{UUID: uuid, Up: c.up.Load(), Down: c.down.Load()}
	}
	app.traffMu.Unlock()
	app.mu.Lock()
	active := app.active
	for uuid, dm := range app.devices {
		row, ok := rows[uuid]
		if !ok {
			row = &adminUserStats{UUID: uuid}
			rows[uuid] = row
		}
		row.Devices = len(dm)
	}
	app.mu.Unlock()
	users := make([]adminUserStats, 0, len(rows))
	for _, row := range rows {
		users = append(users, *row)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].UUID < users[j].UUID })
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"ts":              time.Now().Unix(),
		"sessions_active": active,
		"users":           users,
	})
}

// POST /v1/reload — force an immediate re-read of the file/sqlite user
// sources (contract §1.5, optional P1 — implemented). The API user set is
// untouched; "keys" reports the merged total afterwards.
func (api *adminAPI) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	total, errs := api.app.ks.reloadNow()
	resp := map[string]any{"ok": true, "keys": total}
	if len(errs) > 0 {
		msgs := make([]string, 0, len(errs))
		for _, e := range errs {
			msgs = append(msgs, e.Error())
		}
		resp["warnings"] = msgs
	}
	logInfo("admin", "POST /v1/reload: keys=%d warnings=%d", total, len(errs))
	writeAdminJSON(w, http.StatusOK, resp)
}

// GET /v1/relay — the byte-level relay backend state (v0.15.2):
//
//	{"enabled":bool,"upstream":["h:p",...],"pipes":<live relay pipes>}
//
// PUT /v1/relay — hot update, either field alone:
//
//	{"upstream":["h:p",...],"enabled":true} -> {"ok":true,"upstream":N,"enabled":true}
//
// enabled=false falls back to the direct backend for NEW connections; pipes
// already established keep flowing to their upstream until they end (same
// "new connections only" rule as -backend).
func (api *adminAPI) handleRelay(w http.ResponseWriter, r *http.Request) {
	app := api.app
	switch r.Method {
	case http.MethodGet:
		enabled, upstream, pipes := app.relay.snapshot()
		if upstream == nil {
			upstream = []string{}
		}
		writeAdminJSON(w, http.StatusOK, map[string]any{
			"enabled":  enabled,
			"upstream": upstream,
			"pipes":    pipes,
		})
	case http.MethodPut:
		var req struct {
			Upstream *[]string `json:"upstream"`
			Enabled  *bool     `json:"enabled"`
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, adminBodyLimit))
		if err := dec.Decode(&req); err != nil {
			writeAdminJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json: " + err.Error()})
			return
		}
		on, n, err := app.relay.apply(req.Upstream, req.Enabled)
		if err != nil {
			writeAdminJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		logInfo("admin", "PUT /v1/relay: enabled=%v upstream=%d", on, n)
		writeAdminJSON(w, http.StatusOK, map[string]any{"ok": true, "upstream": n, "enabled": on})
	default:
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method must be GET or PUT"})
	}
}

func methodNotAllowed(w http.ResponseWriter, want string) {
	writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method must be " + want})
}

func writeAdminJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "encode: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}
