package zhdata

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type userPolicy struct {
	MaxDevices int
}

type usersFile struct {
	Users []struct {
		UUID       string `json:"uuid"`
		Name       string `json:"name"`
		MaxDevices int    `json:"max_devices"`
		MaxClients int    `json:"max_clients"`
		Devices    int    `json:"devices"`
	} `json:"users"`
}

// loadUsers reads a users file. Supported formats:
//   - JSON object: {"users":[{"uuid":"...","name":"..."}]}  (s-ui export style)
//   - JSON array:  ["uuid1","uuid2",...]
//   - plain text:  one uuid per line, '#' comments allowed
func loadUsers(path string) ([]string, error) {
	users, _, err := loadUsersWithPolicies(path)
	return users, err
}

func loadUsersWithPolicies(path string) ([]string, map[string]userPolicy, error) {
	policies := map[string]userPolicy{}
	users, err := loadUsersRaw(path, policies)
	return users, policies, err
}

func loadUsersRaw(path string, policies map[string]userPolicy) ([]string, error) {
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
				if k != "" && k != "null" && dev > 0 {
					policies[k] = userPolicy{MaxDevices: dev}
				}
			}
		} else {
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
			if len(fields) > 1 {
				var dev int
				if _, err := fmt.Sscanf(fields[1], "%d", &dev); err == nil && dev > 0 {
					policies[k] = userPolicy{MaxDevices: dev}
				}
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
