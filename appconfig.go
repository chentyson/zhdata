package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// appConfig is the JSON config accepted by `zhdata -c config.json`
// (zhenhanvn first-class-core mode; see zhdata-一级核心接入方案.md §5.7).
// Every field is optional. Unknown fields are ignored on purpose so a newer
// zhenhanvn can emit fields an older core has never seen without breaking it.
// Precedence: explicitly-set CLI flag > config file > flag default.
type appConfig struct {
	Mode       string          `json:"mode"`  // "client" (default) | "server"
	Listen     string          `json:"listen"` // client: local proxy port; server: comma-separated public ports
	Server     json.RawMessage `json:"server"` // "h:p,h:p" or ["h:p","h:p"] slot i dials servers[i%N]
	UUID       string          `json:"uuid"`   // client user id
	Users      string          `json:"users"`  // server: comma-separated txt/json/sqlite key sources
	Backend    string          `json:"backend"` // server: "direct" or socks addr
	MaxClients *int            `json:"max_clients"`
	Decoy      string          `json:"decoy"`  // server: host:port for non-protocol HTTP traffic
	Verbose    *bool           `json:"verbose"`
	Conns      *int            `json:"conns"`
	Spread     *int            `json:"spread"`
	Train      *int            `json:"train"`
	Migrate    *int            `json:"migrate"`
	Dlconns    *int            `json:"dlconns"`
	Slotbase   *int            `json:"slotbase"`
	Pprof      string          `json:"pprof"`
}

// ServerCSV renders the "server" field into the canonical comma-separated
// form used by -server. Both a comma-separated string and an array of
// "host:port" strings are accepted.
func (c *appConfig) ServerCSV() (string, error) {
	if len(c.Server) == 0 {
		return "", nil
	}
	var s string
	if json.Unmarshal(c.Server, &s) == nil {
		return strings.TrimSpace(s), nil
	}
	var arr []string
	if err := json.Unmarshal(c.Server, &arr); err != nil {
		return "", fmt.Errorf(`config "server" must be a comma-separated string or an array of "host:port" strings`)
	}
	parts := make([]string, 0, len(arr))
	for _, v := range arr {
		if v = strings.TrimSpace(v); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, ","), nil
}

func loadAppConfig(path string) (*appConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	var c appConfig
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &c, nil
}

// coreParams holds fully-resolved runtime parameters (the union of all CLI
// flags, possibly overridden by the JSON config).
type coreParams struct {
	mode       string
	listen     string
	server     string
	uuid       string
	users      string
	backend    string
	decoy      string
	pprof      string
	maxClients int
	conns      int
	spread     int
	train      int
	migrate    int
	dlconns    int
	slotbase   int
	verbose    bool
}

// resolveParams merges CLI flags with the optional config file. fl carries
// the parsed flag values (defaults included); set names the flags the user
// actually typed. cfg may be nil (pure CLI mode, fully backward compatible).
func resolveParams(fl coreParams, set map[string]bool, cfg *appConfig) (coreParams, error) {
	out := fl
	if cfg == nil {
		return out, nil
	}
	pick := func(flagName string, dst *string, v string) {
		if !set[flagName] {
			if v = strings.TrimSpace(v); v != "" {
				*dst = v
			}
		}
	}
	pickInt := func(flagName string, dst *int, v *int) {
		if !set[flagName] && v != nil {
			*dst = *v
		}
	}
	pick("mode", &out.mode, cfg.Mode)
	pick("listen", &out.listen, cfg.Listen)
	server, err := cfg.ServerCSV()
	if err != nil {
		return out, err
	}
	pick("server", &out.server, server)
	pick("uuid", &out.uuid, cfg.UUID)
	pick("users", &out.users, cfg.Users)
	pick("backend", &out.backend, cfg.Backend)
	pick("decoy", &out.decoy, cfg.Decoy)
	pick("pprof", &out.pprof, cfg.Pprof)
	pickInt("max-clients", &out.maxClients, cfg.MaxClients)
	pickInt("conns", &out.conns, cfg.Conns)
	pickInt("spread", &out.spread, cfg.Spread)
	pickInt("train", &out.train, cfg.Train)
	pickInt("migrate", &out.migrate, cfg.Migrate)
	pickInt("dlconns", &out.dlconns, cfg.Dlconns)
	pickInt("slotbase", &out.slotbase, cfg.Slotbase)
	if !set["v"] && cfg.Verbose != nil {
		out.verbose = *cfg.Verbose
	}
	return out, nil
}
