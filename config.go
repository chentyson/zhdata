package zhdata

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type userPolicy struct {
	MaxDevices int
	// v0.15.6 per-uuid bandwidth caps in kbit/s (1000-based), enforced by
	// bwlimit.go. 0 = that direction unlimited. The panel's own convention is
	// that a client's group ("2M", "500K") caps both directions, so a single
	// speed_limit value lands in both fields.
	UpKbps   int
	DownKbps int
}

// empty reports whether the policy limits nothing at all.
func (p userPolicy) empty() bool {
	return p.MaxDevices <= 0 && p.UpKbps <= 0 && p.DownKbps <= 0
}

// accountInfo is optional per-uuid account metadata answered by the
// in-tunnel account query (v0.15.4). All-zero = no source provided it.
type accountInfo struct {
	Expiry int64 // unix seconds
	Quota  int64 // bytes
	Used   int64 // bytes
}

type usersFile struct {
	Users []struct {
		UUID       string `json:"uuid"`
		Name       string `json:"name"`
		MaxDevices int    `json:"max_devices"`
		MaxClients int    `json:"max_clients"`
		Devices    int    `json:"devices"`
		// optional account fields for the in-tunnel query (v0.15.4); only
		// the JSON object format can carry them (see loadUsersRaw)
		Expiry int64 `json:"expiry"`
		Quota  int64 `json:"quota"`
		Used   int64 `json:"used"`
		// optional bandwidth caps (v0.15.6, bwlimit.go) in kbit/s; 0 = that
		// direction unlimited. speed_limit is the single-value form (what a
		// panel "2M" group means: both directions) and fills both.
		UpKbps     int `json:"up_kbps"`
		DownKbps   int `json:"down_kbps"`
		SpeedLimit int `json:"speed_limit"`
	} `json:"users"`
}

// loadUsers reads a users file. Supported formats:
//   - JSON object: {"users":[{"uuid":"...","name":"..."}]}  (s-ui export style)
//     optional policy fields max_devices|max_clients|devices, and (v0.15.6)
//     up_kbps/down_kbps/speed_limit in kbit/s
//   - JSON array:  ["uuid1","uuid2",...]
//   - plain text:  one uuid per line, '#' comments allowed; an optional second
//     field is the device limit, an optional third field the bandwidth cap
//     (kbit/s, both directions)
func loadUsers(path string) ([]string, error) {
	users, _, _, err := loadUsersWithPolicies(path)
	return users, err
}

func loadUsersWithPolicies(path string) ([]string, map[string]userPolicy, map[string]accountInfo, error) {
	policies := map[string]userPolicy{}
	accounts := map[string]accountInfo{}
	users, err := loadUsersRaw(path, policies, accounts)
	return users, policies, accounts, err
}

func loadUsersRaw(path string, policies map[string]userPolicy, accounts map[string]accountInfo) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s := strings.TrimSpace(string(raw))
	var out []string
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		if strings.HasPrefix(s, "{") {
			var uf usersFile
			if err = json.Unmarshal([]byte(s), &uf); err != nil {
				return nil, fmt.Errorf("users file: %w", err)
			}
			for _, u := range uf.Users {
				k := strings.ToLower(strings.TrimSpace(u.UUID))
				out = append(out, k)
				dev := u.MaxDevices
				if dev == 0 {
					dev = u.MaxClients
				}
				if dev == 0 {
					dev = u.Devices
				}
				if k != "" && k != "null" {
					up, down := u.UpKbps, u.DownKbps
					if u.SpeedLimit > 0 {
						if up == 0 {
							up = u.SpeedLimit
						}
						if down == 0 {
							down = u.SpeedLimit
						}
					}
					pol := userPolicy{MaxDevices: dev, UpKbps: up, DownKbps: down}
					if !pol.empty() {
						policies[k] = pol
					}
				}
				acc := accountInfo{Expiry: u.Expiry, Quota: u.Quota, Used: u.Used}
				if k != "" && k != "null" && acc != (accountInfo{}) {
					accounts[k] = acc
				}
			}
		} else {
			// JSON string arrays carry uuids only: no room for account
			// fields, so the txt and plain-array formats stay as-is.
			var arr []string
			if err = json.Unmarshal([]byte(s), &arr); err != nil {
				return nil, fmt.Errorf("users file: %w", err)
			}
			for _, u := range arr {
				out = append(out, strings.ToLower(strings.TrimSpace(u)))
			}
		}
	} else {
		for _, line := range strings.Split(s, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.Fields(line)
			k := strings.ToLower(strings.TrimSpace(fields[0]))
			out = append(out, k)
			pol := userPolicy{}
			if len(fields) > 1 {
				var dev int
				if _, err := fmt.Sscanf(fields[1], "%d", &dev); err == nil && dev > 0 {
					pol.MaxDevices = dev
				}
			}
			if len(fields) > 2 {
				// third field: bandwidth cap in kbit/s, both directions
				var kbps int
				if _, err := fmt.Sscanf(fields[2], "%d", &kbps); err == nil && kbps > 0 {
					pol.UpKbps, pol.DownKbps = kbps, kbps
				}
			}
			if !pol.empty() {
				policies[k] = pol
			}
		}
	}
	clean := out[:0:0]
	for _, u := range out {
		if u != "" && u != "null" {
			clean = append(clean, u)
		}
	}
	out = clean
	if len(out) == 0 {
		return nil, fmt.Errorf("users file %s: no users", path)
	}
	return out, nil
}
