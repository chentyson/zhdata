package zhdata

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------------- relay (byte-level pass-through backend) e2e ----------------

// relayProbe performs a raw zhv7 handshake against addr and returns the 32B
// auth response plus the live conn (caller closes). A successful probe is a
// full tunnel session: on the relay it becomes a live pipe, so holding the
// conn open holds the pipe (used by the limits/kick tests).
func relayProbe(t *testing.T, addr, uuid string, device [16]byte, slot byte) ([]byte, net.Conn) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("probe dial %s: %v", addr, err)
	}
	c.SetDeadline(time.Now().Add(8 * time.Second))
	salt := make([]byte, 16)
	if _, err = rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	tx := newCTR(uuid, salt)
	hp := make([]byte, 32)
	copy(hp, magic)
	hp[4] = slot
	copy(hp[handDevOff:handDevOff+deviceIDLen], device[:])
	copy(hp[handNonceOff:], randBytes(handNonceLen))
	tx.XORKeyStream(hp, hp)
	if _, err = c.Write(append(salt, hp...)); err != nil {
		t.Fatalf("probe handshake write: %v", err)
	}
	resp := make([]byte, authRespLen)
	if _, err = io.ReadFull(c, resp); err != nil {
		t.Fatalf("probe auth read: %v", err)
	}
	// A probe either gets a plaintext reject or an auth-OK encrypted under THIS
	// salt/uuid keystream. Over a relay the OK bytes come back from the
	// upstream, so verifying them here proves the pipe is byte-exact.
	if !isAuthReject(resp) && !verifyAuthOK(resp, newCTR(uuid, salt)) {
		t.Fatalf("probe got neither a valid reject nor a valid auth-OK from %s (keystream desync?): %x", addr, resp)
	}
	return resp, c
}

func probeDevice(tag byte) [16]byte {
	var d [16]byte
	copy(d[:], "relay-probe-device-")
	d[15] = tag
	return d
}

type relayEnv struct {
	final  *serverApp // terminates the tunnel, dials the real target
	relay  *serverApp // -backend zhdata: identifies the user, pipes the bytes
	target string     // httptest body
	host   string
	port   uint16
}

func newRelayEnv(t *testing.T, uuid string, finalCfg, relayCfg serverConfig) *relayEnv {
	t.Helper()
	body := "relay-pong-body"
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	t.Cleanup(target.Close)
	host, port := splitHostPort(t, target.URL)

	finalCfg.listen = "127.0.0.1:0"
	finalCfg.usersPath = usersFileWith(t, uuid)
	if finalCfg.backend == "" {
		finalCfg.backend = "direct"
	}
	final, err := newServerApp(finalCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(final.Close)

	relayCfg.listen = "127.0.0.1:0"
	if relayCfg.usersPath == "" {
		relayCfg.usersPath = usersFileWith(t, uuid)
	}
	relayCfg.backend = backendZhdata
	relayCfg.upstream = final.Addr()
	relay, err := newServerApp(relayCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(relay.Close)

	return &relayEnv{final: final, relay: relay, target: body, host: host, port: port}
}

func relayPipes(t *testing.T, e *relayEnv) (int, bool, []string) {
	t.Helper()
	enabled, upstream, pipes := e.relay.relay.snapshot()
	return pipes, enabled, upstream
}

// 1) the byte path works end to end (HTTP through socks -> relay -> final -> target)
func TestRelayEndToEnd(t *testing.T) {
	const uuid = "relay-e2e-0001"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	waitFor(t, func() bool { return e.relay.estab.Load() >= 1 }, "relay session established")
	waitFor(t, func() bool { return e.final.estab.Load() >= 1 }, "upstream session established through relay")
	pipes, enabled, upstream := relayPipes(t, e)
	if !enabled || pipes != 1 || len(upstream) != 1 || upstream[0] != e.final.Addr() {
		t.Fatalf("relay state: pipes=%d enabled=%v upstream=%v", pipes, enabled, upstream)
	}

	got, err := httpThroughTunnel(t, cli.Addr(), e.host, e.port)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains([]byte(got), []byte(e.target)) {
		t.Fatalf("relay byte path: want %q in response, got %q", e.target, got)
	}

	// The probe path proves the relay is truly transparent: the upstream's
	// auth-OK arrives encrypted under the *downstream* salt/uuid keystream, so
	// only a byte-exact relay can pass it (relayProbe verifies the magic).
	resp, pc := relayProbe(t, e.relay.Addr(), uuid, probeDevice(9), 3)
	defer pc.Close()
	if isAuthReject(resp) {
		t.Fatalf("relay probe rejected: reason=%d", authRejectReason(resp))
	}
}

// 2) traffic counters: both the relay (ciphertext bytes) and the final server
// (payload bytes) account for the same uuid.
func TestRelayStatsCountBothSides(t *testing.T) {
	const uuid = "relay-stats-0002"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	relayAdmin := adminTestServer(t, e.relay, "")
	finalAdmin := adminTestServer(t, e.final, "")

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return e.final.estab.Load() >= 1 }, "upstream session")

	got, err := httpThroughTunnel(t, cli.Addr(), e.host, e.port)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		rf, ok := statsRow(statsOf(t, finalAdmin.URL, ""), uuid)
		return ok && rf.Up >= 18 && rf.Down >= int64(len(got))
	}, "final-side stats up/down counted")
	waitFor(t, func() bool {
		rr, ok := statsRow(statsOf(t, relayAdmin.URL, ""), uuid)
		return ok && rr.Up >= 48 && rr.Down > 0
	}, "relay-side stats up/down counted (ciphertext bytes)")

	rf, _ := statsRow(statsOf(t, finalAdmin.URL, ""), uuid)
	rr, _ := statsRow(statsOf(t, relayAdmin.URL, ""), uuid)
	// relay counts raw ciphertext (handshake + frames), final counts payload:
	// both must see the uuid, with devices tracked at both hops.
	if rf.Devices != 1 || rr.Devices != 1 {
		t.Fatalf("devices: final=%d relay=%d", rf.Devices, rr.Devices)
	}
	if s := statsOf(t, relayAdmin.URL, ""); s.SessionsActive < 1 {
		t.Fatalf("relay sessions_active=%d, relay pipes must count", s.SessionsActive)
	}
}

// 3) device limit is enforced at the relay (reason 3) without touching upstream.
func TestRelayDeviceLimitReject(t *testing.T) {
	const uuid = "relay-dev-0003"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{
		usersPath: usersFileWithDeviceLimit(t, uuid, 1),
	})

	// first device: accepted (auth-OK comes from the upstream through the pipe)
	resp, c1 := relayProbe(t, e.relay.Addr(), uuid, probeDevice(1), 0)
	defer c1.Close()
	if isAuthReject(resp) {
		t.Fatalf("first probe rejected: reason=%d", authRejectReason(resp))
	}
	waitFor(t, func() bool { return e.final.estab.Load() >= 1 }, "upstream session for device 1")

	// second device (different device_id, different slot): the relay refuses it
	// with reason 3 before dialing anything upstream.
	resp2, c2 := relayProbe(t, e.relay.Addr(), uuid, probeDevice(2), 1)
	defer c2.Close()
	if !isAuthReject(resp2) || authRejectReason(resp2) != authReasonDevice {
		t.Fatalf("second probe: want reject reason %d, got reject=%v reason=%d",
			authReasonDevice, isAuthReject(resp2), authRejectReason(resp2))
	}
	if pipes, _, _ := relayPipes(t, e); pipes != 1 {
		t.Fatalf("rejected probe must not open an upstream pipe: pipes=%d", pipes)
	}
	if n := e.final.estab.Load(); n != 1 {
		t.Fatalf("rejected probe must not reach the upstream: final estab=%d", n)
	}
}

// 3b) max-sessions is enforced at the relay too (reason 2).
func TestRelayMaxSessionsReject(t *testing.T) {
	const uuid = "relay-sess-0004"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{maxSessions: 1, maxClients: 0})

	resp, c1 := relayProbe(t, e.relay.Addr(), uuid, probeDevice(1), 0)
	defer c1.Close()
	if isAuthReject(resp) {
		t.Fatalf("first probe rejected: reason=%d", authRejectReason(resp))
	}
	waitFor(t, func() bool { _, _, n := e.relay.relay.snapshot(); return n >= 1 }, "pipe open")

	resp2, c2 := relayProbe(t, e.relay.Addr(), uuid, probeDevice(2), 1)
	defer c2.Close()
	if !isAuthReject(resp2) || authRejectReason(resp2) != authReasonMaxSess {
		t.Fatalf("second probe: want reject reason %d, got reject=%v reason=%d",
			authReasonMaxSess, isAuthReject(resp2), authRejectReason(resp2))
	}
}

// 3c) unknown uuid at the relay: rejected exactly like the direct path.
func TestRelayUnknownUUIDReject(t *testing.T) {
	const uuid = "relay-uuid-0005"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	resp, c := relayProbe(t, e.relay.Addr(), "not-in-user-set", probeDevice(1), 0)
	defer c.Close()
	if !isAuthReject(resp) || authRejectReason(resp) != authReasonBadAuth {
		t.Fatalf("unknown uuid: want reject reason %d, got reject=%v reason=%d",
			authReasonBadAuth, isAuthReject(resp), authRejectReason(resp))
	}
	if pipes, _, _ := relayPipes(t, e); pipes != 0 {
		t.Fatalf("unknown uuid must not open a pipe: pipes=%d", pipes)
	}
}

// 4) admin hot switch: PUT /v1/relay toggles the backend and replaces the
// upstream list; only NEW connections change behaviour.
func TestRelayAdminHotSwitch(t *testing.T) {
	const uuid = "relay-switch-0006"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	admin := adminTestServer(t, e.relay, "")

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return e.final.estab.Load() >= 1 }, "pipe established")
	if p, _, _ := relayPipes(t, e); p != 1 {
		t.Fatalf("pipes=%d want 1", p)
	}

	// bad requests
	if code, _ := adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{"upstream":[]}`); code != http.StatusBadRequest {
		t.Fatalf("empty upstream while enabled: want 400, got %d", code)
	}
	if code, _ := adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{"upstream":["no-port"]}`); code != http.StatusBadRequest {
		t.Fatalf("invalid upstream address: want 400, got %d", code)
	}
	if code, _ := adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{}`); code != http.StatusBadRequest {
		t.Fatalf("empty body fields: want 400, got %d", code)
	}

	// disable: new connections terminate at the relay with the direct backend
	code, out := adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{"enabled":false}`)
	if code != http.StatusOK || out["enabled"] != false || out["ok"] != true {
		t.Fatalf("PUT disable: %d %v", code, out)
	}
	if got := out["upstream"]; got != float64(1) {
		t.Fatalf("PUT disable must keep the upstream list: %v", got)
	}

	direct, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid, deviceID: deviceHex(probeDevice(7))})
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	finalEstabBefore := e.final.estab.Load()
	waitFor(t, func() bool { return e.relay.estab.Load() >= 2 }, "direct session terminated at the relay")
	body, err := httpThroughTunnel(t, direct.Addr(), e.host, e.port)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains([]byte(body), []byte(e.target)) {
		t.Fatalf("direct fallback body: want %q, got %q", e.target, body)
	}
	if n := e.final.estab.Load(); n != finalEstabBefore {
		t.Fatalf("new session after disable must not reach upstream: final estab %d -> %d", finalEstabBefore, n)
	}
	if p, enabled, _ := relayPipes(t, e); p != 1 || enabled {
		t.Fatalf("after disable: pipes=%d enabled=%v (existing pipe must survive, enabled must read false)", p, enabled)
	}

	// The s-ui panel always sends both fields (enabled = len(upstream) > 0), so
	// "clear the list and switch off" is the real disable body: it must be 200,
	// not the empty-list-while-enabled 400 above.
	code, out = adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{"upstream":[],"enabled":false}`)
	if code != http.StatusOK || out["enabled"] != false || out["upstream"] != float64(0) || out["ok"] != true {
		t.Fatalf(`PUT {"upstream":[],"enabled":false}: %d %v`, code, out)
	}
	if p, enabled, ups := relayPipes(t, e); p != 1 || enabled || len(ups) != 0 {
		t.Fatalf("after clear+disable: pipes=%d enabled=%v upstream=%v", p, enabled, ups)
	}

	// re-enable: the third connection is relayed again
	code, out = adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "", `{"upstream":["`+e.final.Addr()+`"],"enabled":true}`)
	if code != http.StatusOK || out["enabled"] != true || out["upstream"] != float64(1) {
		t.Fatalf("PUT enable: %d %v", code, out)
	}
	back, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid, deviceID: deviceHex(probeDevice(8))})
	if err != nil {
		t.Fatal(err)
	}
	defer back.Close()
	waitFor(t, func() bool { return e.final.estab.Load() >= finalEstabBefore+1 }, "re-enabled relay reaches upstream")
	waitFor(t, func() bool { _, _, n := e.relay.relay.snapshot(); return n >= 2 }, "new pipe opened")
}

// 5) admin kick tears the relay pipes down; the client reconnects on its normal
// backoff (documented: relay kick = cut pipe, no tGoaway).
func TestRelayKickClosesPipes(t *testing.T) {
	const uuid = "relay-kick-0007"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	admin := adminTestServer(t, e.relay, "")

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: e.relay.Addr(), uuid: uuid})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { _, _, n := e.relay.relay.snapshot(); return n >= 1 }, "pipe established")

	code, out := adminJSON(t, http.MethodPost, admin.URL+"/v1/users/kick", "", `{"uuid":"`+uuid+`"}`)
	if code != http.StatusOK {
		t.Fatalf("kick: %d %v", code, out)
	}
	if out["kicked"].(float64) < 1 {
		t.Fatalf("kick must report the cut pipe: %v", out)
	}
	waitFor(t, func() bool {
		_, _, n := e.relay.relay.snapshot()
		return n == 0
	}, "kick closed the relay pipe")
	waitFor(t, func() bool {
		return statsOf(t, admin.URL, "").SessionsActive == 0
	}, "relay sessions_active back to 0")

	// The user is still in the pushed set, so the client reconnects (1-2s
	// backoff) and a new pipe appears.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, n := e.relay.relay.snapshot(); n >= 1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("client did not re-establish a relay pipe after kick")
}

// round-robin: consecutive downstream connections spread over the upstream list.
func TestRelayRoundRobinUpstreams(t *testing.T) {
	const uuid = "relay-rr-0008"
	e := newRelayEnv(t, uuid, serverConfig{}, serverConfig{})
	second, err := newServerApp(serverConfig{listen: "127.0.0.1:0", usersPath: usersFileWith(t, uuid), backend: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	admin := adminTestServer(t, e.relay, "")
	up := e.final.Addr() + "," + second.Addr()
	if code, out := adminJSON(t, http.MethodPut, admin.URL+"/v1/relay", "",
		`{"upstream":["`+e.final.Addr()+`","`+second.Addr()+`"]}`); code != http.StatusOK {
		t.Fatalf("PUT two upstreams: %d %v (upstream %s)", code, out, up)
	}

	for i := 0; i < 2; i++ {
		cli, err := newClientApp(clientConfig{
			listen:   "127.0.0.1:0",
			server:   e.relay.Addr(),
			uuid:     uuid,
			deviceID: deviceHex(probeDevice(byte(20 + i))),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
	}
	waitFor(t, func() bool {
		return e.final.estab.Load() >= 1 && second.estab.Load() >= 1
	}, "both upstreams received one connection")
}
