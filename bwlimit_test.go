package zhdata

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Bandwidth caps (v0.15.6): the bucket math, the policy plumbing (users file /
// sqlite / admin API), and one end-to-end check that a capped user is actually
// held near the cap in both directions.

// ---- bucket ----

func TestBWBucketChargesAndWaits(t *testing.T) {
	// 200 kbit/s = 25000 B/s; the burst floor (32KB) is above one second of
	// this cap on purpose, so even a small cap gets a frame of headroom.
	b := newBWBucket(200)
	if b.bps != 25000 {
		t.Fatalf("bps = %v, want 25000 (kbit/s -> bytes/s)", b.bps)
	}
	if got := b.reserve(1000); got != 0 {
		t.Fatalf("first charge under the burst waited %v", got)
	}
	// Drain the burst, then a full second of the cap must cost about a second.
	b.reserve(int64(b.burst))
	wait := b.reserve(25000)
	if wait < 900*time.Millisecond || wait > 1100*time.Millisecond {
		t.Fatalf("25000 bytes at 200 kbit/s waited %v, want ~1s", wait)
	}
	// Concurrent streams of one uuid add up instead of each getting the cap:
	// four 8KB reservations from four "streams" land on one shared debt.
	b2 := newBWBucket(800) // 100000 B/s, burst 100000
	var total time.Duration
	for i := 0; i < 4; i++ {
		total += b2.reserve(100 << 10)
	}
	if total < 150*time.Millisecond {
		t.Fatalf("4x100KB at 100000 B/s with a 100KB burst should debt ~150ms, got %v", total)
	}
}

func TestBWPairReuseAndLift(t *testing.T) {
	r := newBWRegistry()
	if r.pairFor("u1", 0, 0) != nil {
		t.Fatal("no caps must give no buckets")
	}
	p1 := r.pairFor("u1", 2000, 2000)
	if p1 == nil || p1.up == nil || p1.down == nil {
		t.Fatalf("caps missing: %+v", p1)
	}
	if p2 := r.pairFor("u1", 2000, 2000); p2 != p1 {
		t.Fatal("same caps must reuse the live buckets (a rebuilt bucket would hand out a fresh burst)")
	}
	p3 := r.pairFor("u1", 1000, 2000)
	if p3 == p1 {
		t.Fatal("changed caps must reshape the buckets")
	}
	if r.pairFor("u1", 0, 0) != nil {
		t.Fatal("a lifted cap must stop limiting on the next frame")
	}
	r.drop("u1")
	if p4 := r.pairFor("u1", 2000, 2000); p4 == p3 {
		t.Fatal("drop must discard the old buckets")
	}
}

func TestWaitBWParksAbandonsAndBounds(t *testing.T) {
	r := newBWRegistry()
	// 100 kbit/s = 12500 B/s: 50KB costs ~4s of debt.
	p := r.pairFor("slow", 0, 100)
	// burn the burst first so the wait is real
	p.down.reserve(int64(p.down.burst) + 50000)

	dead := false
	start := time.Now()
	if !waitBW(p, false, 5000, 0, func() bool { return dead }) {
		t.Fatal("live stream abandoned the park")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("parked %v for a bucket already in debt", elapsed)
	}

	dead = true
	start = time.Now()
	if waitBW(p, false, 5000, 0, func() bool { return dead }) {
		t.Fatal("dead stream should not keep parking")
	}
	if elapsed := time.Since(start); elapsed > bwWaitSlice*4 {
		t.Fatalf("dead stream waited %v", elapsed)
	}

	// A bounded park (the pre-dial flush holds the stream lock) returns fast but
	// still charges: the next, unbounded wait inherits the debt.
	p2 := r.pairFor("predial", 100, 0)
	p2.up.reserve(int64(p2.up.burst) + 50000)
	start = time.Now()
	if !waitBW(p2, true, 5000, bwParkBound, nil) {
		t.Fatal("bounded park should report ok")
	}
	if elapsed := time.Since(start); elapsed > bwParkBound {
		t.Fatalf("bounded park ran %v, bound is %v", elapsed, bwParkBound)
	}
	start = time.Now()
	waitBW(p2, true, 1000, 0, nil)
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("debt from the bounded park was not carried over (%v)", elapsed)
	}
}

// ---- policy plumbing ----

func TestBWCapsFromUserSources(t *testing.T) {
	dir := t.TempDir()

	jsonPath := filepath.Join(dir, "users.json")
	body := `{"users":[
	  {"uuid":"J-BOTH","up_kbps":1000,"down_kbps":2000},
	  {"uuid":"J-SPEED","speed_limit":500},
	  {"uuid":"J-PLAIN"}
	]}`
	if err := os.WriteFile(jsonPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	_, policies, _, err := loadUsersWithPolicies(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if p := policies["j-both"]; p.UpKbps != 1000 || p.DownKbps != 2000 {
		t.Fatalf("json up/down: %+v", p)
	}
	if p := policies["j-speed"]; p.UpKbps != 500 || p.DownKbps != 500 {
		t.Fatalf("json speed_limit must cap both directions: %+v", p)
	}
	if _, ok := policies["j-plain"]; ok {
		t.Fatal("a plain key must carry no policy")
	}

	txtPath := filepath.Join(dir, "users.txt")
	if err = os.WriteFile(txtPath, []byte("# uuid [devices] [kbit/s]\nT-CAP 2 400\nT-PLAIN\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, policies, _, err = loadUsersWithPolicies(txtPath)
	if err != nil {
		t.Fatal(err)
	}
	if p := policies["t-cap"]; p.MaxDevices != 2 || p.UpKbps != 400 || p.DownKbps != 400 {
		t.Fatalf("txt third field must be the cap: %+v", p)
	}
	if _, ok := policies["t-plain"]; ok {
		t.Fatal("txt plain key must carry no policy")
	}

	dbPath := filepath.Join(dir, "s-ui.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE client (id integer primary key, uuid text, up_kbps integer, speed_limit_kbps integer, max_devices integer);`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO client (uuid,up_kbps,max_devices) VALUES ('db-split',300,2);`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO client (uuid,speed_limit_kbps) VALUES ('db-speed',1500);`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	_, policies, _, err = readUsersFromDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if p := policies["db-split"]; p.UpKbps != 300 || p.DownKbps != 0 || p.MaxDevices != 2 {
		t.Fatalf("sqlite up_kbps must not invent a down cap: %+v", p)
	}
	if p := policies["db-speed"]; p.UpKbps != 1500 || p.DownKbps != 1500 {
		t.Fatalf("sqlite speed_limit_kbps must cap both: %+v", p)
	}
}

func TestAdminPushCarriesCapsAndOverrides(t *testing.T) {
	ks, err := newKeyStore(usersFileWith(t, "api-a", "file-only"))
	if err != nil {
		t.Fatal(err)
	}
	ks.setAPIUsers([]apiUser{{UUID: "API-A", UpKbps: 2000, DownKbps: 2000}})
	if p := ks.policy("api-a"); p.UpKbps != 2000 || p.DownKbps != 2000 {
		t.Fatalf("admin push did not carry the caps: %+v", p)
	}
	capped := ks.capped()
	if len(capped) != 1 || capped["api-a"].DownKbps != 2000 {
		t.Fatalf("capped() view: %+v", capped)
	}
	// Same wholesale rule as max_devices: an entry without caps lifts them.
	ks.setAPIUsers([]apiUser{{UUID: "api-a"}})
	if p := ks.policy("api-a"); p.UpKbps != 0 || p.DownKbps != 0 {
		t.Fatalf("caps must lift when the pushed entry carries none: %+v", p)
	}
	if len(ks.capped()) != 0 {
		t.Fatal("capped() must empty with the caps lifted")
	}
}

func TestAdminUsersPutAndStatsExposeCaps(t *testing.T) {
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, "stats-uuid"),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	base := adminTestServer(t, srv, "").URL

	code, resp := adminJSON(t, http.MethodPut, base+"/v1/users", "",
		`{"users":[{"uuid":"stats-uuid","max_devices":0,"speed_limit_kbps":2000}]}`)
	if code != http.StatusOK {
		t.Fatalf("PUT /v1/users: %d %v", code, resp)
	}
	if p := srv.ks.policy("stats-uuid"); p.UpKbps != 2000 || p.DownKbps != 2000 {
		t.Fatalf("speed_limit_kbps must cap both directions: %+v", p)
	}

	_, stats := adminJSON(t, http.MethodGet, base+"/v1/stats", "", "")
	rows, _ := stats["users"].([]any)
	var found bool
	for _, v := range rows {
		row, _ := v.(map[string]any)
		if row["uuid"] != "stats-uuid" {
			continue
		}
		found = true
		if row["up_kbps"] != float64(2000) || row["down_kbps"] != float64(2000) {
			t.Fatalf("GET /v1/stats must report the cap in force: %v", row)
		}
	}
	if !found {
		t.Fatalf("no stats row for the capped user: %v", stats["users"])
	}
}

// ---- end to end: the cap actually holds ----

func TestCappedUserIsShapedBothWays(t *testing.T) {
	const uuid = "b0000000-0000-4000-8000-000000000001"
	const size = 120000 // bytes each way
	const kbps = 400    // 50000 B/s -> ~2.4s per direction
	payload := bytes.Repeat([]byte("z"), size)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n, _ := io.Copy(io.Discard, r.Body)
			fmt.Fprintf(w, "got %d", n)
			return
		}
		w.Write(payload)
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
	cli, err := newClientApp(clientConfig{
		listen: "127.0.0.1:0",
		server: srv.Addr(),
		uuid:   uuid,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "session established")

	// Uncapped first: the same transfer must be quick, otherwise the assertion
	// below would pass on a slow machine for the wrong reason.
	got := downloadThroughTunnel(t, cli.Addr(), host, port, size)
	if len(got) != size {
		t.Fatalf("uncapped download delivered %d of %d bytes", len(got), size)
	}

	// Now push the cap the way the panel does, mid-session: no restart.
	srv.ks.setAPIUsers([]apiUser{{UUID: uuid, UpKbps: kbps, DownKbps: kbps}})

	start := time.Now()
	got = downloadThroughTunnel(t, cli.Addr(), host, port, size)
	down := time.Since(start)
	if len(got) != size {
		t.Fatalf("capped download delivered %d of %d bytes", len(got), size)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("capped download corrupted the body")
	}
	// 2.4s expected at the cap; allow a generous floor and a strict ceiling so
	// a missing limiter cannot slip through.
	if down < time.Second {
		t.Fatalf("download took %v for %d bytes at %d kbit/s: not shaped", down, size, kbps)
	}
	if down > 15*time.Second {
		t.Fatalf("download took %v: over-shaped or stalled", down)
	}

	start = time.Now()
	body := uploadThroughTunnel(t, cli.Addr(), host, port, payload)
	up := time.Since(start)
	if !strings.Contains(body, fmt.Sprintf("got %d", size)) {
		t.Fatalf("capped upload not delivered intact: %q", body)
	}
	if up < time.Second {
		t.Fatalf("upload took %v for %d bytes at %d kbit/s: not shaped", up, size, kbps)
	}
	if up > 15*time.Second {
		t.Fatalf("upload took %v: over-shaped or stalled", up)
	}
}

// downloadThroughTunnel fetches path "/" through the client's socks listener and
// returns the response body, with a deadline wide enough for a capped transfer.
func downloadThroughTunnel(t *testing.T, socksAddr, host string, port uint16, want int) []byte {
	t.Helper()
	c, err := socksDial(socksAddr, atypDomain, []byte(host), port)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(40 * time.Second))
	if _, err = c.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	i := bytes.Index(raw, []byte("\r\n\r\n"))
	if i < 0 {
		t.Fatalf("no header separator in %q", raw[:min(len(raw), 200)])
	}
	return raw[i+4:]
}

func uploadThroughTunnel(t *testing.T, socksAddr, host string, port uint16, payload []byte) string {
	t.Helper()
	c, err := socksDial(socksAddr, atypDomain, []byte(host), port)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(40 * time.Second))
	head := fmt.Sprintf("POST / HTTP/1.0\r\nContent-Length: %d\r\n\r\n", len(payload))
	if _, err = c.Write([]byte(head)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Write(payload); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
