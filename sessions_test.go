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

	// Empty set is an empty JSON array, not null (the panel renders it directly).
	if code, raw := adminHTTP(t, http.MethodGet, base+"/v1/sessions", "", ""); code != http.StatusOK ||
		!strings.Contains(string(raw), `"sessions":[]`) {
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
		if i > 0 && p.Sessions[i-1].StartedAt > row.StartedAt {
			t.Fatalf("sessions must be ordered by started_at ascending: %d then %d",
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
	waitFor(t, func() bool { return len(sessionsOf(t, base, "").Sessions) == 0 }, "kick emptied the session list")
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
