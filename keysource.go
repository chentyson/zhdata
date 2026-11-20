package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// keyStore manages user keys from multiple sources (plain text and s-ui
// sqlite databases). -users accepts a comma separated list of paths; each
// path is auto-detected (sqlite magic) and results are unioned, lowercased,
// deduplicated. Files are re-read when mtime/size changes (hot reload).
type keyStore struct {
	paths []string

	mu       sync.RWMutex
	keys     []string
	policies map[string]userPolicy // uuid -> per-user policy (0 max devices = unlimited)
	sigs     map[string]string     // path -> "mtimeNano:size"
	loaded   bool
}

const keyReloadInterval = 20 * time.Second

func splitPaths(spec string) []string {
	var out []string
	for _, p := range strings.Split(spec, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newKeyStore(spec string) (*keyStore, error) {
	paths := splitPaths(spec)
	if len(paths) == 0 {
		return nil, fmt.Errorf("empty -users spec")
	}
	ks := &keyStore{paths: paths, policies: map[string]userPolicy{}, sigs: map[string]string{}}
	keys, policies, per, errs := ks.collect()
	for _, err := range errs {
		logWarn("users", "%v", err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no user keys from %v", paths)
	}
	ks.keys = keys
	ks.policies = policies
	ks.reSigs()
	ks.loaded = true
	logInfo("users", "loaded %d keys (%d device-limited) %v", len(keys), countLimited(policies), per)
	return ks, nil
}

func (ks *keyStore) snapshot() []string {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return append([]string(nil), ks.keys...)
}

func (ks *keyStore) policy(uuid string) userPolicy {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.policies[uuid]
}

func countLimited(policies map[string]userPolicy) int {
	n := 0
	for _, p := range policies {
		if p.MaxDevices > 0 {
			n++
		}
	}
	return n
}

func (ks *keyStore) reSigs() {
	sigs := map[string]string{}
	for _, p := range ks.paths {
		if st, err := os.Stat(p); err == nil {
			sigs[p] = fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
		}
	}
	ks.sigs = sigs
}

// reloadIfChanged re-reads sources if any file changed (or appeared/vanished).
// On total failure the previous key set is kept (fail-safe).
func (ks *keyStore) reloadIfChanged() bool {
	sigs := map[string]string{}
	changed := false
	for _, p := range ks.paths {
		if st, err := os.Stat(p); err == nil {
			s := fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
			sigs[p] = s
			if ks.sigs[p] != s {
				changed = true
			}
		} else if _, existed := ks.sigs[p]; existed {
			changed = true // file vanished
		}
	}
	if !changed {
		return false
	}
	keys, policies, per, errs := ks.collect()
	if len(keys) == 0 {
		logWarn("users", "reload produced 0 keys, keeping previous (%d); errors: %v", len(ks.snapshot()), joinErrs(errs))
		ks.reSigs()
		return false
	}
	for _, err := range errs {
		logWarn("users", "%v", err)
	}
	ks.mu.Lock()
	ks.keys = keys
	ks.policies = policies
	ks.mu.Unlock()
	ks.sigs = sigs
	logInfo("users", "hot-reloaded %d keys (%d device-limited) %v", len(keys), countLimited(policies), per)
	return true
}

func (ks *keyStore) collect() (keys []string, policies map[string]userPolicy, per map[string]int, errs []error) {
	set := map[string]bool{}
	policies = map[string]userPolicy{}
	per = map[string]int{}
	for _, p := range ks.paths {
		var us []string
		var ps map[string]userPolicy
		var err error
		if isSqliteFile(p) {
			us, ps, err = readUsersFromDB(p)
		} else {
			us, ps, err = loadUsersWithPolicies(p)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p, err))
			continue
		}
		per[p] = len(us)
		for _, k := range us {
			if k == "" || k == "null" {
				continue
			}
			set[k] = true
			if pol, ok := ps[k]; ok && pol.MaxDevices > 0 {
				if old, exists := policies[k]; !exists || old.MaxDevices == 0 || pol.MaxDevices > old.MaxDevices {
					policies[k] = pol
				}
			}
		}
	}
	keys = make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return
}

func joinErrs(errs []error) string {
	s := make([]string, 0, len(errs))
	for _, e := range errs {
		s = append(s, e.Error())
	}
	return strings.Join(s, "; ")
}

func isSqliteFile(p string) bool {
	f, err := os.Open(p)
	if err != nil {
		return false
	}
	defer f.Close()
	var magic [15]byte
	if _, err = f.Read(magic[:]); err != nil {
		return false
	}
	return string(magic[:]) == "SQLite format 3"
}

// readUsersFromDB reads keys and optional per-user policies from an s-ui
// style sqlite database by finding every table that has a "uuid" column
// (schema-adaptive, read-only). Optional policy columns: max_devices,
// max_clients, devices, device_limit.
func readUsersFromDB(path string) ([]string, map[string]userPolicy, error) {
	policies := map[string]userPolicy{}
	db, err := openReadonlyDB(path)
	if err != nil {
		return nil, policies, err
	}
	defer db.Close()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, policies, err
	}
	var tables []string
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			rows.Close()
			return nil, policies, err
		}
		tables = append(tables, t)
	}
	rows.Close()

	var out []string
	used := 0
	for _, t := range tables {
		if !tableHasColumn(db, t, "uuid") {
			continue
		}
		used++
		polCol := firstColumn(db, t, "max_devices", "max_clients", "devices", "device_limit")
		if polCol == "" {
			q := "SELECT uuid FROM " + quoteIdent(t)
			r2, err := db.Query(q)
			if err != nil {
				continue
			}
			for r2.Next() {
				var u sql.NullString
				if err = r2.Scan(&u); err != nil {
					break
				}
				k := strings.ToLower(strings.TrimSpace(u.String))
				if k != "" && k != "null" {
					out = append(out, k)
				}
			}
			r2.Close()
			continue
		}

		q := "SELECT uuid, " + quoteIdent(polCol) + " FROM " + quoteIdent(t)
		r2, err := db.Query(q)
		if err != nil {
			continue
		}
		for r2.Next() {
			var u sql.NullString
			var pol sql.NullInt64
			if err = r2.Scan(&u, &pol); err != nil {
				break
			}
			k := strings.ToLower(strings.TrimSpace(u.String))
			if k == "" || k == "null" {
				continue
			}
			out = append(out, k)
			if pol.Valid && pol.Int64 > 0 {
				policies[k] = userPolicy{MaxDevices: int(pol.Int64)}
			}
		}
		r2.Close()
	}
	if used == 0 {
		return nil, policies, fmt.Errorf("no table with a 'uuid' column in %s", path)
	}
	return out, policies, nil
}

func openReadonlyDB(path string) (*sql.DB, error) {
	uri := "file:" + filepath.ToSlash(path) + "?mode=ro&_pragma=query_only(1)"
	db, err := sql.Open("sqlite", uri)
	if err == nil {
		if err = db.Ping(); err == nil {
			return db, nil
		}
		db.Close()
	}
	// fallback for drivers/URIs without mode=ro support
	db, err = sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func tableHasColumn(db *sql.DB, table, col string) bool {
	rows, err := db.Query("PRAGMA table_info(" + quoteIdent(table) + ")")
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err = rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false
		}
		if strings.EqualFold(name, col) {
			return true
		}
	}
	return false
}

func firstColumn(db *sql.DB, table string, cols ...string) string {
	for _, c := range cols {
		if tableHasColumn(db, table, c) {
			return c
		}
	}
	return ""
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
