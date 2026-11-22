package zhdata

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func makeSuiDB(t *testing.T, uuids ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s-ui.db")
	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE client (id integer primary key, uuid text, name text);
		CREATE TABLE settings (key text, value text);`); err != nil {
		t.Fatal(err)
	}
	for _, u := range uuids {
		if _, err = db.Exec(`INSERT INTO client (uuid,name) VALUES (?,?)`, u, "user"); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestReadUsersFromDB(t *testing.T) {
	p := makeSuiDB(t, "UPPER-Aaa", "b-bb")
	u, _, _, err := readUsersFromDB(p)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(u)
	if len(u) != 2 || u[0] != "b-bb" || u[1] != "upper-aaa" {
		t.Fatalf("unexpected users: %v", u)
	}
}

// optional account columns (v0.15.4): expiry + quota columns are picked up
// when present and skipped silently when absent.
func TestReadUsersFromDBAccountColumns(t *testing.T) {
	p := filepath.Join(t.TempDir(), "acct.db")
	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE client (id integer primary key, uuid text, expired_at integer, quota integer, max_devices integer);`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO client (uuid,expired_at,quota,max_devices) VALUES ('acct-uu',1780000000,107374182400,2), ('acct-bare',NULL,NULL,NULL)`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	us, policies, accounts, err := readUsersFromDB(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 2 {
		t.Fatalf("users: %v", us)
	}
	if pol := policies["acct-uu"]; pol.MaxDevices != 2 {
		t.Fatalf("policy column lost: %+v", pol)
	}
	acc, ok := accounts["acct-uu"]
	if !ok || acc.Expiry != 1780000000 || acc.Quota != 107374182400 {
		t.Fatalf("account columns not read: %+v ok=%v", acc, ok)
	}
	if _, ok := accounts["acct-bare"]; ok {
		t.Fatal("NULL/absent account fields must not create a record")
	}
}

// a panel can expose its users as a VIEW over a JSON config column (s-ui keeps
// the zhdata uuid inside clients.config, not in a column); views and the used
// column are picked up like any table (v0.15.4).
func TestReadUsersFromDBViewOverJSONConfig(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s-ui.db")
	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE clients (id integer primary key, name text, config blob, expiry integer, volume integer, up integer, down integer);`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO clients (name,config,expiry,volume,up,down) VALUES ('C49801','{"zhdata":{"uuid":"SUI-UU-1"}}',1780000000,644245094400,8284166083,268733272548);`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE VIEW zhdata_users AS SELECT json_extract(config,'$.zhdata.uuid') uuid, expiry, volume, (up+down) used FROM clients WHERE json_extract(config,'$.zhdata.uuid') IS NOT NULL;`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	us, _, accounts, err := readUsersFromDB(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 1 || us[0] != "sui-uu-1" {
		t.Fatalf("view uuid not read (or not lowercased): %v", us)
	}
	acc, ok := accounts["sui-uu-1"]
	if !ok || acc.Expiry != 1780000000 || acc.Quota != 644245094400 || acc.Used != 277017438631 {
		t.Fatalf("view account fields not read: %+v ok=%v", acc, ok)
	}
}

func TestReadUsersFromDBNoUuidTable(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.db")
	db, _ := sql.Open("sqlite", p)
	db.Exec(`CREATE TABLE foo (a text)`)
	db.Close()
	if _, _, _, err := readUsersFromDB(p); err == nil {
		t.Fatal("expected error when no uuid column exists")
	}
}

// the JSON object users format can carry the optional account fields
// (v0.15.4); txt lines and plain uuid arrays cannot, and stay as-is.
func TestLoadUsersAccountFields(t *testing.T) {
	p := filepath.Join(t.TempDir(), "u.json")
	os.WriteFile(p, []byte(`{"users":[{"uuid":"J-1","expiry":1780000000,"quota":107374182400,"used":7},{"uuid":"j-2"}]}`), 0o644)
	us, _, accs, err := loadUsersWithPolicies(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 2 {
		t.Fatalf("users: %v", us)
	}
	if acc := accs["j-1"]; acc.Expiry != 1780000000 || acc.Quota != 107374182400 || acc.Used != 7 {
		t.Fatalf("json account fields not wired: %+v", acc)
	}
	if _, ok := accs["j-2"]; ok {
		t.Fatal("entry without account fields must not create a record")
	}
}

func TestKeyStoreUnionDedupHotReloadFailSafe(t *testing.T) {
	dbp := makeSuiDB(t, "uuid-in-db")
	txtp := filepath.Join(t.TempDir(), "u.txt")
	os.WriteFile(txtp, []byte("uuid-in-db\nextra-key\n"), 0o644)

	ks, err := newKeyStore(dbp + "," + txtp)
	if err != nil {
		t.Fatal(err)
	}
	k := ks.snapshot()
	sort.Strings(k)
	if len(k) != 2 || k[0] != "extra-key" || k[1] != "uuid-in-db" {
		t.Fatalf("union/dedup wrong: %v", k)
	}

	// add a user in the DB (s-ui adding a client), no process restart
	db, err := sql.Open("sqlite", dbp)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`INSERT INTO client (uuid,name) VALUES ('new-uuid','x')`)
	db.Close()
	if !ks.reloadIfChanged() {
		t.Fatal("expected reload after db change")
	}
	k = ks.snapshot()
	if !contains(k, "new-uuid") || len(k) != 3 {
		t.Fatalf("hot reload missed new uuid: %v", k)
	}

	// db file vanishes -> fall back to txt only
	os.Remove(dbp)
	if !ks.reloadIfChanged() {
		t.Fatal("expected reload after db removed")
	}
	k = ks.snapshot()
	if len(k) != 2 || contains(k, "new-uuid") {
		t.Fatalf("expected txt-only keys, got: %v", k)
	}

	// all sources gone -> fail-safe keeps previous keys
	os.Remove(txtp)
	ks.reloadIfChanged()
	k = ks.snapshot()
	if len(k) != 2 {
		t.Fatalf("fail-safe broken, keys now: %v", k)
	}
}

func TestEndToEndDBUsers(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("db-users-ok"))
	}))
	defer target.Close()
	host, port := splitHostPort(t, target.URL)

	dbp := makeSuiDB(t, "sui-user-001")
	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: dbp,
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, err := newClientApp(clientConfig{listen: "127.0.0.1:0", server: srv.Addr(), uuid: "sui-user-001"})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "db-user session")

	body, err := httpThroughTunnel(t, cli.Addr(), host, port)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "db-users-ok") {
		t.Fatalf("bad body %q", body)
	}
}

func contains(s []string, x string) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}
