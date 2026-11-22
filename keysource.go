package zhdata

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
// sqlite databases) plus the admin API user set (admin.go PUT /v1/users).
// -users accepts a comma separated list of paths; each path is auto-detected
// (sqlite magic) and results are unioned, lowercased, deduplicated. Files are
// re-read when mtime/size changes (hot reload). The API user set is a third
// source: it is unioned with the file sources and NEVER cleared by a file
// hot-reload (reloadIfChanged only watches file signatures). Policy merge:
// the API entry wins for the same uuid (max_devices 0 = unlimited override).
// Alongside policies the store keeps optional per-uuid account metadata
// (expiry/quota/used, v0.15.4) that the in-tunnel account query answers with;
// sources are merged the same way (API account record overrides the file one).
type keyStore struct {
	paths []string

	mu           sync.RWMutex
	keys         []string               // effective view: file/sqlite sources ∪ API users (merged)
	policies     map[string]userPolicy  // effective view: uuid -> policy (0 max devices = unlimited)
	accounts     map[string]accountInfo // effective view: uuid -> account metadata (query handler)
	fileKeys     []string               // last collect() result from file/sqlite sources only
	filePolicies map[string]userPolicy  // policies from file/sqlite sources only
	fileAccounts map[string]accountInfo // account metadata from file/sqlite sources only
	apiPolicies  map[string]userPolicy  // admin API user set: uuid -> policy (0 = unlimited)
	apiAccounts  map[string]accountInfo // admin API user set: uuid -> account metadata
	sigs         map[string]string      // path -> "mtimeNano:size"
	srcStats     map[string]userSource  // path -> last load outcome (admin user_sources)
	loaded       bool
}

// userSource is the observability record for ONE -users path. Before this
// existed a misconfigured path (typo in -users, s-ui DB moved) only surfaced
// as a repeating "[users] [warning] ...: no such file or directory" line in
// journald; the panel could not see it at all. The admin API now exposes these
// records (GET /v1/stats "user_sources", POST /v1/reload likewise), so the
// operator sees the broken source in the UI.
//
// Semantics (what the JSON fields mean):
//
//	Keys      keys the source contributed at its LAST SUCCESSFUL load
//	          (0 if it never loaded; kept at that number while it is failing,
//	          so the panel still sees what the path last provided)
//	LastError error of the last attempt; "" when it loaded cleanly
//	At        unix seconds of the last load ATTEMPT (success or failure)
//
// Field names + json tags are exported so admin.go can serialize the snapshot
// as-is (additive fields: an older panel ignores them).
type userSource struct {
	Path      string `json:"path"`
	Keys      int    `json:"keys"`
	LastError string `json:"last_error"`
	At        int64  `json:"at"`
}

// apiUser is one entry of the admin API user set (zhdata-admin-api.md §1.2).
// Expiry/Quota/Used are optional account fields for the in-tunnel account
// query (0 = not provided).
type apiUser struct {
	UUID       string
	MaxDevices int
	Expiry     int64 // unix seconds
	Quota      int64 // bytes
	Used       int64 // bytes
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
	ks := &keyStore{
		paths:        paths,
		policies:     map[string]userPolicy{},
		filePolicies: map[string]userPolicy{},
		apiPolicies:  map[string]userPolicy{},
		accounts:     map[string]accountInfo{},
		fileAccounts: map[string]accountInfo{},
		apiAccounts:  map[string]accountInfo{},
		sigs:         map[string]string{},
		srcStats:     map[string]userSource{},
	}
	keys, policies, accounts, per, srcErrs := ks.collect()
	for _, err := range srcErrList(ks.paths, srcErrs) {
		logWarn("users", "%v", err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no user keys from %v", paths)
	}
	ks.fileKeys = keys
	ks.filePolicies = policies
	ks.fileAccounts = accounts
	// record the startup attempt in the per-source view too. At startup there
	// is no previous state, so noteSourceResultsLocked emits no transition
	// lines (the warnings above are the startup visibility); the record itself
	// is what GET /v1/stats later serves, which is how a wrong -users path
	// becomes visible in the panel.
	ks.mu.Lock()
	ks.noteSourceResultsLocked(per, srcErrs)
	ks.mu.Unlock()
	ks.reSigs()
	ks.loaded = true
	ks.mergeLocked()
	logInfo("users", "loaded %d keys (%d device-limited) %v", len(ks.keys), countLimited(ks.policies), per)
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

// account returns the account metadata (expiry unix seconds / quota bytes /
// used bytes) the sources provided for one uuid. ok=false when no source
// carried any of the fields — the query handler then falls back to
// -query-url / the 404 answer.
func (ks *keyStore) account(uuid string) (accountInfo, bool) {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	v, ok := ks.accounts[strings.ToLower(strings.TrimSpace(uuid))]
	return v, ok
}

// setAPIUsers atomically replaces the admin API user set (PUT /v1/users).
// An empty list clears the API set; file/sqlite sources are untouched. The
// effective key set stays the union of both worlds. Returns the API-set size
// and the merged total.
func (ks *keyStore) setAPIUsers(users []apiUser) (apiN, totalN int) {
	next := make(map[string]userPolicy, len(users))
	nextAcc := make(map[string]accountInfo)
	for _, u := range users {
		k := strings.ToLower(strings.TrimSpace(u.UUID))
		if k == "" || k == "null" {
			continue
		}
		dev := u.MaxDevices
		if dev < 0 {
			dev = 0
		}
		next[k] = userPolicy{MaxDevices: dev}
		acc := accountInfo{Expiry: u.Expiry, Quota: u.Quota, Used: u.Used}
		if acc != (accountInfo{}) {
			nextAcc[k] = acc
		}
	}
	ks.mu.Lock()
	ks.apiPolicies = next
	ks.apiAccounts = nextAcc
	ks.mergeLocked()
	apiN, totalN = len(next), len(ks.keys)
	limited := countLimited(ks.policies)
	ks.mu.Unlock()
	logInfo("users", "admin API users replaced: api=%d total=%d (%d device-limited)", apiN, totalN, limited)
	return apiN, totalN
}

// apiCount reports the size of the admin API user set.
func (ks *keyStore) apiCount() int {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return len(ks.apiPolicies)
}

// keysTotal reports the size of the effective key set (file/sqlite sources ∪
// API users) — what GET /v1/stats "keys_total" shows the panel.
func (ks *keyStore) keysTotal() int {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return len(ks.keys)
}

// sources snapshots the per-source records for the admin API, sorted by path:
// the panel renders this list directly, so the order must be stable across
// polls (same rationale as the /v1/sessions sort).
func (ks *keyStore) sources() []userSource {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	out := make([]userSource, 0, len(ks.srcStats))
	for _, st := range ks.srcStats {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// mergeLocked recomputes the effective keys/policies views from the file
// sources and the API user set. The API entry overrides the file policy for
// the same uuid, including "unlimited" (max_devices 0 removes a file limit).
func (ks *keyStore) mergeLocked() {
	set := make(map[string]bool, len(ks.fileKeys)+len(ks.apiPolicies))
	for _, k := range ks.fileKeys {
		set[k] = true
	}
	for k := range ks.apiPolicies {
		set[k] = true
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	policies := make(map[string]userPolicy, len(ks.filePolicies)+len(ks.apiPolicies))
	for k, p := range ks.filePolicies {
		policies[k] = p
	}
	for k, p := range ks.apiPolicies {
		if p.MaxDevices > 0 {
			policies[k] = p
		} else {
			delete(policies, k)
		}
	}
	// account records merge like policies: an API entry that carries account
	// fields overrides the file record; one without fields leaves it intact.
	accounts := make(map[string]accountInfo, len(ks.fileAccounts)+len(ks.apiAccounts))
	for k, v := range ks.fileAccounts {
		accounts[k] = v
	}
	for k, v := range ks.apiAccounts {
		accounts[k] = v
	}
	ks.keys = keys
	ks.policies = policies
	ks.accounts = accounts
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
	ks.mu.Lock()
	ks.sigs = sigs
	ks.mu.Unlock()
}

// reloadIfChanged re-reads sources if any file changed (or appeared/vanished).
// On total failure the previous key set is kept (fail-safe). The API user set
// is never touched here: it is not a file source.
func (ks *keyStore) reloadIfChanged() bool {
	ks.mu.RLock()
	prev := ks.sigs
	ks.mu.RUnlock()
	sigs := map[string]string{}
	changed := false
	for _, p := range ks.paths {
		if st, err := os.Stat(p); err == nil {
			s := fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
			sigs[p] = s
			if prev[p] != s {
				changed = true
			}
		} else if _, existed := prev[p]; existed {
			changed = true // file vanished
		}
	}
	if !changed {
		return false
	}
	keys, policies, accounts, per, srcErrs := ks.collect()
	if len(keys) == 0 {
		// record the attempt even though the fail-safe keeps the previous key
		// set: the per-source errors (and the timestamp) are exactly what the
		// panel needs to see why.
		ks.mu.Lock()
		warns, infos := ks.noteSourceResultsLocked(per, srcErrs)
		ks.mu.Unlock()
		logWarn("users", "reload produced 0 keys, keeping previous (%d); errors: %v", len(ks.snapshot()), joinErrs(srcErrList(ks.paths, srcErrs)))
		logSourceTransitions(warns, infos)
		ks.reSigs()
		return false
	}
	for _, err := range srcErrList(ks.paths, srcErrs) {
		logWarn("users", "%v", err)
	}
	ks.mu.Lock()
	ks.fileKeys = keys
	ks.filePolicies = policies
	ks.fileAccounts = accounts
	warns, infos := ks.noteSourceResultsLocked(per, srcErrs)
	ks.mergeLocked() // API user set survives file reloads (it is not file-derived)
	total, apiN := len(ks.keys), len(ks.apiPolicies)
	limited := countLimited(ks.policies)
	ks.sigs = sigs
	ks.mu.Unlock()
	logSourceTransitions(warns, infos)
	logInfo("users", "hot-reloaded %d keys = %d file + %d api (%d device-limited) %v", total, len(keys), apiN, limited, per)
	return true
}

// reloadNow re-reads every file/sqlite source immediately, bypassing the
// mtime/size signature check (admin POST /v1/reload, contract §1.5 — P1).
// Same fail-safe as reloadIfChanged: total failure keeps the previous keys.
func (ks *keyStore) reloadNow() (total int, errs []error) {
	keys, policies, accounts, per, srcErrs := ks.collect()
	errs = srcErrList(ks.paths, srcErrs)
	if len(keys) == 0 {
		ks.mu.Lock()
		warns, infos := ks.noteSourceResultsLocked(per, srcErrs)
		ks.mu.Unlock()
		logWarn("users", "forced reload produced 0 file keys, keeping previous; errors: %v", joinErrs(errs))
		logSourceTransitions(warns, infos)
		return len(ks.snapshot()), errs
	}
	ks.mu.Lock()
	ks.fileKeys = keys
	ks.filePolicies = policies
	ks.fileAccounts = accounts
	warns, infos := ks.noteSourceResultsLocked(per, srcErrs)
	ks.mergeLocked()
	total, apiN := len(ks.keys), len(ks.apiPolicies)
	limited := countLimited(ks.policies)
	ks.mu.Unlock()
	logSourceTransitions(warns, infos)
	ks.reSigs()
	logInfo("users", "reloaded on demand: %d keys = %d file + %d api (%d device-limited) %v", total, len(keys), apiN, limited, per)
	return total, errs
}

// collect re-reads every configured source once. per reports the keys each
// source contributed (paths that errored are absent from per); srcErrs maps
// the failing path -> its bare error (the path is the map key, callers do not
// need it wrapped twice).
func (ks *keyStore) collect() (keys []string, policies map[string]userPolicy, accounts map[string]accountInfo, per map[string]int, srcErrs map[string]error) {
	set := map[string]bool{}
	policies = map[string]userPolicy{}
	accounts = map[string]accountInfo{}
	per = map[string]int{}
	srcErrs = map[string]error{}
	for _, p := range ks.paths {
		var us []string
		var ps map[string]userPolicy
		var as map[string]accountInfo
		var err error
		if isSqliteFile(p) {
			us, ps, as, err = readUsersFromDB(p)
		} else {
			us, ps, as, err = loadUsersWithPolicies(p)
		}
		if err != nil {
			srcErrs[p] = err
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
			if acc, ok := as[k]; ok {
				if _, seen := accounts[k]; !seen {
					accounts[k] = acc // first source providing account fields wins
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

// noteSourceResultsLocked merges one collect() pass into the per-source
// observability records. Call with ks.mu held. Every configured path gets an
// updated record (keys from the last successful load, last error, attempt
// time), which is what makes all three load paths — startup, the 20s
// reloadIfChanged and the forced reloadNow — observable through the API.
//
// It returns transition lines for the caller to log once it has unlocked:
// a warning the FIRST time a previously-clean source starts failing and an
// info when a failing source loads clean again. A source that is still
// failing does not re-transition, so the 20s watch does not double up on the
// per-path warning the reload paths already log (those stay as they were).
// Startup passes have no previous state, so the newKeyStore warnings above
// remain the only startup line.
func (ks *keyStore) noteSourceResultsLocked(per map[string]int, srcErrs map[string]error) (warns, infos []string) {
	if ks.srcStats == nil {
		ks.srcStats = map[string]userSource{}
	}
	at := time.Now().Unix()
	for _, p := range ks.paths {
		prev, had := ks.srcStats[p]
		st := userSource{Path: p, Keys: prev.Keys, At: at}
		if err, failed := srcErrs[p]; failed {
			// Keys stays at the last successful load's count: while the
			// source is broken, the effective set comes from the fail-safe
			// (previous keys), so the last number it contributed is the
			// honest thing to show.
			st.LastError = err.Error()
			if had && prev.LastError == "" {
				// clean before, failing now: the one-time transition warning
				warns = append(warns, fmt.Sprintf("source %s failing: %v", p, err))
			}
		} else {
			st.Keys = per[p] // per holds 0 for a source that loaded but had no users
			if had && prev.LastError != "" {
				infos = append(infos, fmt.Sprintf("source %s recovered: %d keys", p, st.Keys))
			}
		}
		ks.srcStats[p] = st
	}
	return warns, infos
}

// logSourceTransitions emits the transition lines from noteSourceResultsLocked
// (one warning per newly-failing source, one info per recovered one).
func logSourceTransitions(warns, infos []string) {
	for _, w := range warns {
		logWarn("users", "%s", w)
	}
	for _, s := range infos {
		logInfo("users", "%s", s)
	}
}

// srcErrList unwraps collect's per-path error map back into a path-ordered
// []error: warnings (admin POST /v1/reload) and the [users] log lines must not
// shuffle between runs, and map iteration order is randomized.
func srcErrList(paths []string, srcErrs map[string]error) []error {
	var out []error
	for _, p := range paths {
		if err, ok := srcErrs[p]; ok {
			out = append(out, fmt.Errorf("%s: %w", p, err))
		}
	}
	return out
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

// readUsersFromDB reads keys, optional per-user policies and optional account
// metadata from an s-ui style sqlite database by finding every table that has
// a "uuid" column (schema-adaptive, read-only). Optional policy columns:
// max_devices, max_clients, devices, device_limit. Optional account columns
// (v0.15.4, in-tunnel query): expired_at|expired_time|expiry|expire for the
// expiry (unix seconds) and traffic|quota|volume|total_traffic for the quota
// (bytes); every optional column is skipped silently when absent.
func readUsersFromDB(path string) ([]string, map[string]userPolicy, map[string]accountInfo, error) {
	policies := map[string]userPolicy{}
	accounts := map[string]accountInfo{}
	db, err := openReadonlyDB(path)
	if err != nil {
		return nil, policies, accounts, err
	}
	defer db.Close()
	// Views count too: a panel can expose its users as a view (e.g. pulling the
	// uuid out of a JSON config column) instead of duplicating a table.
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type IN ('table','view')")
	if err != nil {
		return nil, policies, accounts, err
	}
	var tables []string
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			rows.Close()
			return nil, policies, accounts, err
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
		// Dynamic column list: uuid + whichever optional columns exist.
		cols := []string{quoteIdent("uuid")}
		idxPol, idxExp, idxQuota, idxUsed := -1, -1, -1, -1
		if c := firstColumn(db, t, "max_devices", "max_clients", "devices", "device_limit"); c != "" {
			idxPol = len(cols)
			cols = append(cols, quoteIdent(c))
		}
		if c := firstColumn(db, t, "expired_at", "expired_time", "expiry", "expire"); c != "" {
			idxExp = len(cols)
			cols = append(cols, quoteIdent(c))
		}
		if c := firstColumn(db, t, "traffic", "quota", "volume", "total_traffic"); c != "" {
			idxQuota = len(cols)
			cols = append(cols, quoteIdent(c))
		}
		if c := firstColumn(db, t, "used", "used_traffic", "traffic_used", "total_used"); c != "" {
			idxUsed = len(cols)
			cols = append(cols, quoteIdent(c))
		}
		q := "SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(t)
		r2, err := db.Query(q)
		if err != nil {
			continue
		}
		var u sql.NullString
		num := make([]sql.NullInt64, len(cols)-1)
		vals := make([]any, len(cols))
		vals[0] = &u
		for i := range num {
			vals[i+1] = &num[i]
		}
		for r2.Next() {
			if err = r2.Scan(vals...); err != nil {
				break
			}
			k := strings.ToLower(strings.TrimSpace(u.String))
			if k == "" || k == "null" {
				continue
			}
			out = append(out, k)
			if idxPol >= 0 && num[idxPol-1].Valid && num[idxPol-1].Int64 > 0 {
				policies[k] = userPolicy{MaxDevices: int(num[idxPol-1].Int64)}
			}
			acc := accountInfo{}
			if idxExp >= 0 && num[idxExp-1].Valid && num[idxExp-1].Int64 > 0 {
				acc.Expiry = num[idxExp-1].Int64
			}
			if idxQuota >= 0 && num[idxQuota-1].Valid && num[idxQuota-1].Int64 > 0 {
				acc.Quota = num[idxQuota-1].Int64
			}
			if idxUsed >= 0 && num[idxUsed-1].Valid && num[idxUsed-1].Int64 > 0 {
				acc.Used = num[idxUsed-1].Int64
			}
			if acc != (accountInfo{}) {
				accounts[k] = acc
			}
		}
		r2.Close()
	}
	if used == 0 {
		return nil, policies, accounts, fmt.Errorf("no table or view with a 'uuid' column in %s", path)
	}
	return out, policies, accounts, nil
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
