package zhdata

// The user view for a panel sqlite database (zhdata-admin-api.md §2, 核心接口文档 §3.6).
//
// -users accepts an s-ui database directly, but readUsersFromDB only understands
// tables and views that already expose a "uuid" column. The panel stores its
// clients in a "clients" table whose uuid lives inside a JSON config column, so
// the deployment step is one view: zhdata_users.
//
// Some hosts (CentOS 7 is the common one) ship a sqlite3 old enough to lack
// JSON1, so their own shell cannot create that view. The core carries the
// statement instead: "zhdata -init-view <db>" runs it with the same sqlite
// engine the core reads with, which keeps node setup to one command and one
// binary. The view is deliberately the only thing this writes.

import (
	"database/sql"
	"fmt"
	"os"
)

// userViewName is the view -init-view creates and what readUsersFromDB then
// finds by its uuid column.
const userViewName = "zhdata_users"

// userViewSQL maps the panel's clients table onto the columns the core reads.
// IFNULL keeps a NULL counter from making a whole row unreadable, and the
// WHERE clause keeps non-panel rows (no zhdata credentials) out of the key set.
const userViewSQL = `CREATE VIEW IF NOT EXISTS ` + userViewName + ` AS ` +
	`SELECT json_extract(config, '$.zhdata.uuid') AS uuid, ` +
	`expiry, volume, (IFNULL(up, 0) + IFNULL(down, 0)) AS used ` +
	`FROM clients ` +
	`WHERE json_extract(config, '$.zhdata.uuid') IS NOT NULL`

// InitUserView creates the user view in the sqlite database at path and reports
// how many users it exposes. It is idempotent: an existing view is left as it
// is and merely re-counted.
func InitUserView(path string) (int, error) {
	if path == "" {
		return 0, fmt.Errorf("empty database path")
	}
	if _, err := os.Stat(path); err != nil {
		return 0, fmt.Errorf("database %s: %w", path, err)
	}
	if !isSqliteFile(path) {
		return 0, fmt.Errorf("%s is not a sqlite database", path)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer db.Close()

	// The panel writes this database concurrently, so a brief lock is normal.
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		return 0, fmt.Errorf("%s busy: %w", path, err)
	}
	if _, err := db.Exec(userViewSQL); err != nil {
		return 0, fmt.Errorf("create view %s in %s: %w", userViewName, path, err)
	}

	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + userViewName).Scan(&n); err != nil {
		return 0, fmt.Errorf("view %s in %s is not readable: %w (does the panel have clients with zhdata credentials?)", userViewName, path, err)
	}
	return n, nil
}
