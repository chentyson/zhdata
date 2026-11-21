package zhdata

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---------------- GET /v1/sessions (panel inbound connection list) ----------------

type sessionsPayload struct {
	TS       int64         `json:"ts"`
	Total    int64         `json:"total"`
	Sessions []sessionInfo `json:"sessions"`
}

func sessionsOf(t *testing.T, base, token string) sessionsPayload {
	t.Helper()
	code, raw := adminHTTP(t, http.MethodGet, base+"/v1/sessions", token, "")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/sessions: %d %q", code, raw)
	}
	var p sessionsPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("decode sessions: %v (body %q)", err, raw)
	}
	return p
}

var deviceHexRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

// A live session is one row per tunnel session: uuid, the handshake's device_id
// and slot, the peer address, the moment it was established, and relay=false in
// a process that terminates the tunnel itself.
func TestSessionsListLiveSessions(t *testing.T) {
	const uuid = "sess-list-0001"
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	// Empty set is an empty JSON array, not null, and total reads 0 (the panel
	// renders both directly).
	if code, raw := adminHTTP(t, http.MethodGet, base+"/v1/sessions", "", ""); code != http.StatusOK ||
		!strings.Contains(string(raw), `"sessions":[]`) || !strings.Contains(string(raw), `"total":0`) {
		t.Fatalf("empty sessions: code=%d body=%q", code, raw)
	}

	started := time.Now()
	// Six raw zhv7 handshake sessions: two devices x three slots. Held open, so
	// the server-side session set is exactly these six (raw conns never
	// reconnect behind our back, which keeps the kick assertion deterministic).
	hold := make([]net.Conn, 0, 6)
	wantDevice := map[string]bool{}
	for d := byte(1); d <= 2; d++ {
		dev := probeDevice(d)
		wantDevice[deviceHex(dev)] = true
		for slot := byte(0); slot < 3; slot++ {
			resp, c := relayProbe(t, srv.Addr(), uuid, dev, slot)
			if isAuthReject(resp) {
				t.Fatalf("probe device=%d slot=%d rejected: %x", d, slot, resp)
			}
			hold = append(hold, c)
		}
	}
	defer func() {
		for _, c := range hold {
			c.Close()
		}
	}()

	// Plus a real client session set (its own device, two slots).
	clientDev := "636c69656e742d6465762d3030303031"
	cl, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		conns:    2,
		deviceID: clientDev,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	want := 8
	waitFor(t, func() bool { return len(sessionsOf(t, base, "").Sessions) == want }, "eight live session rows")

	p := sessionsOf(t, base, "")
	if p.TS <= 0 {
		t.Fatalf("ts: %v", p.TS)
	}
	if p.Total != int64(want) {
		t.Fatalf("total: want %d, got %d", want, p.Total)
	}
	if len(p.Sessions) != want {
		t.Fatalf("sessions: want %d, got %d (%+v)", want, len(p.Sessions), p.Sessions)
	}
	seen := map[string]bool{}
	perDevice := map[string][]int{}
	for i, row := range p.Sessions {
		if row.UUID != uuid {
			t.Fatalf("row %d uuid: %q", i, row.UUID)
		}
		if !deviceHexRE.MatchString(row.Device) {
			t.Fatalf("row %d device must be 32 lowercase hex: %q", i, row.Device)
		}
		if row.Slot < 0 || row.Slot >= maxSlots {
			t.Fatalf("row %d slot out of range: %d", i, row.Slot)
		}
		if !strings.HasPrefix(row.Remote, "127.0.0.1:") {
			t.Fatalf("row %d remote: %q", i, row.Remote)
		}
		if row.Relay {
			t.Fatalf("row %d: a direct-mode session must report relay=false", i)
		}
		if row.StartedAt < started.Unix()-2 || row.StartedAt > time.Now().Unix()+2 {
			t.Fatalf("row %d started_at %d outside [%d, now]", i, row.StartedAt, started.Unix())
		}
		if row.StartedAt > p.TS {
			t.Fatalf("row %d started_at %d after ts %d", i, row.StartedAt, p.TS)
		}
		if i > 0 && p.Sessions[i-1].StartedAt < row.StartedAt {
			t.Fatalf("sessions must be ordered by started_at descending (newest first): %d then %d",
				p.Sessions[i-1].StartedAt, row.StartedAt)
		}
		key := row.UUID + "|" + row.Device + "|" + fmt.Sprint(row.Slot)
		if seen[key] {
			t.Fatalf("duplicate session row %s (a.conns is keyed uuid:device:slot)", key)
		}
		seen[key] = true
		perDevice[row.Device] = append(perDevice[row.Device], row.Slot)
	}

	// Both probe devices show up with exactly the slots the handshakes asked
	// for, and the real client with its configured device_id.
	for dev := range wantDevice {
		slots := perDevice[dev]
		sort.Ints(slots)
		if len(slots) != 3 || slots[0] != 0 || slots[1] != 1 || slots[2] != 2 {
			t.Fatalf("device %s slots: %v (want [0 1 2])", dev, slots)
		}
	}
	if slots := perDevice[clientDev]; len(slots) != 2 {
		t.Fatalf("client device rows: %v (want 2 slots)", slots)
	}

	// sessions_active and the row set are the same truth.
	if st := statsOf(t, base, ""); st.SessionsActive != want {
		t.Fatalf("sessions_active=%d but %d session rows", st.SessionsActive, want)
	}

	// GET only.
	if code, _ := adminHTTP(t, http.MethodPost, base+"/v1/sessions", "", `{}`); code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/sessions: want 405, got %d", code)
	}

	// Closing the client leaves its two rows; kick then cuts the six probe
	// sessions and the list goes empty.
	cl.Close()
	waitFor(t, func() bool { return len(sessionsOf(t, base, "").Sessions) == 6 }, "client rows gone after Close")

	code, resp := adminJSON(t, http.MethodPost, base+"/v1/users/kick", "", `{"uuid":"`+uuid+`"}`)
	if code != http.StatusOK || resp["kicked"] == nil || resp["kicked"].(float64) < 6 {
		t.Fatalf("kick: %d %v", code, resp)
	}
	waitFor(t, func() bool {
		p := sessionsOf(t, base, "")
		return p.Total == 0 && len(p.Sessions) == 0
	}, "kick emptied the session list (total and rows both 0)")
}

// A -backend zhdata process terminates no tunnel of its own: its list is the
// live byte pipes, every row relay=true. The upstream process lists the same
// tunnels as ordinary sessions (relay=false).
func TestSessionsListRelayPipes(t *testing.T) {
	const uuid = "sess-relay-0001"
	env := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	relayBase := adminTestServer(t, env.relay, "").URL
	finalBase := adminTestServer(t, env.final, "").URL

	hold := make([]net.Conn, 0, 2)
	defer func() {
		for _, c := range hold {
			c.Close()
		}
	}()
	dev := probeDevice(9)
	for slot := byte(0); slot < 2; slot++ {
		resp, c := relayProbe(t, env.relay.Addr(), uuid, dev, slot)
		if isAuthReject(resp) {
			t.Fatalf("relay probe slot=%d rejected: %x", slot, resp)
		}
		hold = append(hold, c)
	}

	waitFor(t, func() bool {
		p := sessionsOf(t, relayBase, "")
		return len(p.Sessions) == 2
	}, "relay pipes listed")

	p := sessionsOf(t, relayBase, "")
	if len(p.Sessions) != 2 {
		t.Fatalf("relay sessions: %+v", p.Sessions)
	}
	slots := []int{}
	for _, row := range p.Sessions {
		if !row.Relay {
			t.Fatalf("relay process row must be relay=true: %+v", row)
		}
		if row.UUID != uuid || row.Device != deviceHex(dev) {
			t.Fatalf("relay row identity: %+v", row)
		}
		if !strings.HasPrefix(row.Remote, "127.0.0.1:") {
			t.Fatalf("relay row remote: %q", row.Remote)
		}
		slots = append(slots, row.Slot)
	}
	sort.Ints(slots)
	if slots[0] != 0 || slots[1] != 1 {
		t.Fatalf("relay slots: %v", slots)
	}

	// The pipe count and the session rows are the same set.
	if _, _, pipes := env.relay.relay.snapshot(); pipes != len(p.Sessions) {
		t.Fatalf("pipes=%d but %d session rows", pipes, len(p.Sessions))
	}

	// Same tunnels, seen from the process that terminates them.
	fp := sessionsOf(t, finalBase, "")
	if len(fp.Sessions) != 2 {
		t.Fatalf("final-side sessions: %+v", fp.Sessions)
	}
	for _, row := range fp.Sessions {
		if row.Relay {
			t.Fatalf("direct-mode process rows must be relay=false: %+v", row)
		}
	}
}

// The optional admin token guards the new route like every other /v1/* route.
func TestSessionsListAuth(t *testing.T) {
	const uuid = "sess-auth-0001"
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "tok").URL

	if code, _ := adminHTTP(t, http.MethodGet, base+"/v1/sessions", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: want 401, got %d", code)
	}
	if code, _ := adminHTTP(t, http.MethodGet, base+"/v1/sessions", "tok", ""); code != http.StatusOK {
		t.Fatalf("with token: want 200, got %d", code)
	}
}

// ?limit takes the newest n rows (rows come back started_at descending), is
// clamped into 10..100, falls back to the default 50 when unparseable, and is
// only an upper bound - never a paging cursor. "total" always counts every live
// session, independent of the window.
func TestSessionsLimitClampAndTotal(t *testing.T) {
	const uuid = "sess-limit-001"
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	query := func(q string) sessionsPayload {
		t.Helper()
		code, raw := adminHTTP(t, http.MethodGet, base+"/v1/sessions"+q, "", "")
		if code != http.StatusOK {
			t.Fatalf("GET /v1/sessions%s: %d %q", q, code, raw)
		}
		var p sessionsPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("decode %s: %v (%q)", q, err, raw)
		}
		return p
	}

	hold := make([]net.Conn, 0, 14)
	defer func() {
		for _, c := range hold {
			c.Close()
		}
	}()

	// Twelve earlier sessions, then - a second later - two more, so "newest
	// first" is provable rather than just "some order".
	for _, d := range []byte{21, 22, 23} {
		dev := probeDevice(d)
		for slot := byte(0); slot < 4; slot++ {
			resp, c := relayProbe(t, srv.Addr(), uuid, dev, slot)
			if isAuthReject(resp) {
				t.Fatalf("probe device=%d slot=%d rejected: %x", d, slot, resp)
			}
			hold = append(hold, c)
		}
	}
	waitFor(t, func() bool { return query("").Total == 12 }, "twelve sessions up")
	time.Sleep(1200 * time.Millisecond)

	newest := deviceHex(probeDevice(24))
	for slot := byte(0); slot < 2; slot++ {
		resp, c := relayProbe(t, srv.Addr(), uuid, probeDevice(24), slot)
		if isAuthReject(resp) {
			t.Fatalf("late probe slot=%d rejected: %x", slot, resp)
		}
		hold = append(hold, c)
	}
	const total = 14
	waitFor(t, func() bool { return query("").Total == total }, "fourteen sessions up")

	full := query("")
	if full.Total != total || len(full.Sessions) != total {
		t.Fatalf("default window: total=%d rows=%d (want %d/%d)", full.Total, len(full.Sessions), total, total)
	}
	for i := 1; i < len(full.Sessions); i++ {
		if full.Sessions[i-1].StartedAt < full.Sessions[i].StartedAt {
			t.Fatalf("rows must be started_at descending: %d then %d",
				full.Sessions[i-1].StartedAt, full.Sessions[i].StartedAt)
		}
	}
	// The two sessions opened a second later lead the list.
	for i := 0; i < 2; i++ {
		if full.Sessions[i].Device != newest {
			t.Fatalf("row %d must be the newest device: %+v", i, full.Sessions[i])
		}
	}

	cases := []struct {
		q    string
		rows int
	}{
		{"?limit=11", 11},   // inside the range
		{"?limit=99", 14},   // inside the range, above total
		{"?limit=10", 10},   // lower boundary
		{"?limit=3", 10},    // below the range -> floor
		{"?limit=1", 10},    // below the range -> floor
		{"?limit=0", 10},    // below the range -> floor
		{"?limit=-5", 10},   // below the range -> floor
		{"?limit=100", 14},  // upper boundary (ceiling, above total)
		{"?limit=1000", 14}, // above the range -> ceiling, still >= total
		{"?limit=abc", 14},  // unparseable -> default 50
		{"?limit=", 14},     // empty -> default
		{"?limit=50", 14},   // default, above total
	}
	for _, tc := range cases {
		p := query(tc.q)
		if p.Total != total {
			t.Fatalf("%s: total=%d, want %d (total ignores limit)", tc.q, p.Total, total)
		}
		if len(p.Sessions) != tc.rows {
			t.Fatalf("%s: rows=%d, want %d", tc.q, len(p.Sessions), tc.rows)
		}
		if len(p.Sessions) > 0 && p.Sessions[0].Device != newest {
			t.Fatalf("%s: window must start at the newest session: %+v", tc.q, p.Sessions[0])
		}
		if len(p.Sessions) > 0 {
			for i := 1; i < len(p.Sessions); i++ {
				if p.Sessions[i-1].StartedAt < p.Sessions[i].StartedAt {
					t.Fatalf("%s: window not newest-first: %+v", tc.q, p.Sessions)
				}
			}
		}
	}

	// limit is not a cursor: the same query returns the same head of the list.
	a, b := query("?limit=11"), query("?limit=11")
	for i := range a.Sessions {
		if a.Sessions[i].Device != b.Sessions[i].Device || a.Sessions[i].Slot != b.Sessions[i].Slot {
			t.Fatalf("?limit=11 not deterministic: %v vs %v", a.Sessions, b.Sessions)
		}
	}
	narrow := query("?limit=11")
	if len(narrow.Sessions) != 11 || len(full.Sessions) < 11 {
		t.Fatalf("?limit=11 window: %d rows vs full %d", len(narrow.Sessions), len(full.Sessions))
	}
	for i := range narrow.Sessions {
		if narrow.Sessions[i].Device != full.Sessions[i].Device || narrow.Sessions[i].Slot != full.Sessions[i].Slot {
			t.Fatalf("?limit=11 must be the head of the full list: %v vs %v", narrow.Sessions, full.Sessions[:11])
		}
	}
}
