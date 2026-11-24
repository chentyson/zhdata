package zhdata

// The one-command node setup step (zhdata -init-view): a panel database whose
// clients keep the zhdata uuid inside a JSON column must become readable to
// -users without the host having a JSON1-capable sqlite3 of its own.

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func panelTestDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s-ui.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE clients (id integer PRIMARY KEY AUTOINCREMENT, name text, "group" text, config text, expiry integer, volume integer, up integer, down integer);`,
		`INSERT INTO clients (name, "group", config, expiry, volume, up, down) VALUES ('alice', '2M', '{"vless":{"uuid":"aaaa"},"zhdata":{"uuid":"11111111-1111-1111-1111-111111111111","name":"alice"}}', 0, 0, 100, 250);`,
		// No zhdata credentials: must stay out of the key set.
		`INSERT INTO clients (name, "group", config, expiry, volume, up, down) VALUES ('bob', '', '{"vless":{"uuid":"bbbb"}}', 0, 0, 1, 1);`,
		// NULL counters must not drop the row.
		`INSERT INTO clients (name, "group", config, expiry, volume, up, down) VALUES ('carol', '', '{"zhdata":{"uuid":"22222222-2222-2222-2222-222222222222"}}', NULL, NULL, NULL, NULL);`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	return path
}

func TestInitUserViewMakesPanelDBReadable(t *testing.T) {
	path := panelTestDB(t)

	n, err := InitUserView(path)
	if err != nil {
		t.Fatalf("InitUserView: %v", err)
	}
	if n != 2 {
		t.Errorf("view reports %d users, want 2 (zhdata credentials only)", n)
	}

	// The whole point: -users on the same path now finds the uuids and the
	// account columns the query handler serves.
	keys, _, accounts, err := readUsersFromDB(path)
	if err != nil {
		t.Fatalf("readUsersFromDB after init: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("keys = %v, want the two zhdata uuids", keys)
	}
	acc, ok := accounts["11111111-1111-1111-1111-111111111111"]
	if !ok {
		t.Fatalf("no account for the seeded user, accounts=%v", accounts)
	}
	if acc.Used != 350 {
		t.Errorf("used = %d, want 350 (up+down, NULL-safe)", acc.Used)
	}
}

func TestInitUserViewIsIdempotent(t *testing.T) {
	path := panelTestDB(t)
	if _, err := InitUserView(path); err != nil {
		t.Fatalf("first InitUserView: %v", err)
	}
	n, err := InitUserView(path)
	if err != nil {
		t.Fatalf("second InitUserView: %v", err)
	}
	if n != 2 {
		t.Errorf("re-run reports %d users, want 2", n)
	}
}

func TestInitUserViewRejectsWrongFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := InitUserView(filepath.Join(dir, "missing.db")); err == nil {
		t.Error("missing database: want an error")
	}
	notDB := filepath.Join(dir, "users.txt")
	if err := os.WriteFile(notDB, []byte("11111111-1111-1111-1111-111111111111\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := InitUserView(notDB); err == nil {
		t.Error("plain text file: want an error, not a view")
	}
}
