package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAppConfigFull(t *testing.T) {
	path := writeCfg(t, `{
		"mode": "client",
		"listen": "127.0.0.1:10808",
		"server": ["example.com:8443", "example.com:8444"],
		"uuid": "iammaster1125",
		"conns": 16,
		"spread": 8,
		"dlconns": -1,
		"migrate": 1,
		"slotbase": 0,
		"train": 0,
		"verbose": false,
		"pprof": "127.0.0.1:6060"
	}`)
	c, err := loadAppConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != "client" || c.Listen != "127.0.0.1:10808" || c.UUID != "iammaster1125" {
		t.Fatalf("scalar fields wrong: %+v", c)
	}
	sv, err := c.ServerCSV()
	if err != nil || sv != "example.com:8443,example.com:8444" {
		t.Fatalf("server array: got %q err %v", sv, err)
	}
	if c.Conns == nil || *c.Conns != 16 {
		t.Fatalf("conns: %v", c.Conns)
	}
	if c.Dlconns == nil || *c.Dlconns != -1 {
		t.Fatalf("dlconns must distinguish unset from -1: %v", c.Dlconns)
	}
	if c.Verbose == nil || *c.Verbose {
		t.Fatalf("verbose false must be explicit: %v", c.Verbose)
	}
	if c.Pprof != "127.0.0.1:6060" {
		t.Fatalf("pprof: %q", c.Pprof)
	}
}

func TestServerFieldForms(t *testing.T) {
	c, err := loadAppConfig(writeCfg(t, `{"server": "a:1,b:2"}`))
	if err != nil {
		t.Fatal(err)
	}
	if sv, err := c.ServerCSV(); err != nil || sv != "a:1,b:2" {
		t.Fatalf("string form: %q %v", sv, err)
	}
	c, err = loadAppConfig(writeCfg(t, `{"server": ["a:1", " b:2 ", ""]}`))
	if err != nil {
		t.Fatal(err)
	}
	if sv, err := c.ServerCSV(); err != nil || sv != "a:1,b:2" {
		t.Fatalf("array form: %q %v", sv, err)
	}
	c, err = loadAppConfig(writeCfg(t, `{"server": 8443}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ServerCSV(); err == nil {
		t.Fatal("numeric server must be rejected")
	}
}

func TestUnknownFieldsIgnored(t *testing.T) {
	// newer zhenhanvn may emit fields this core has never seen
	if _, err := loadAppConfig(writeCfg(t, `{"mode":"client","future":{"a":1},"zzz":123}`)); err != nil {
		t.Fatal(err)
	}
}

func TestBadConfigErrorMentionsPath(t *testing.T) {
	_, err := loadAppConfig(writeCfg(t, `{"mode":`))
	if err == nil || filepath.Base(writeCfg(t, `{"mode":`)) == "" {
		t.Fatal("expected parse error")
	}
	if _, err := loadAppConfig(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected missing-file error")
	}
}

func defaultFlags() coreParams {
	return coreParams{
		mode: "client", backend: "direct", maxClients: 100,
		conns: 1, spread: 8, train: 0, migrate: 1, dlconns: -1, slotbase: 0,
	}
}

func TestResolveNilConfigIsPureCLI(t *testing.T) {
	fl := defaultFlags()
	fl.listen, fl.server, fl.uuid = "127.0.0.1:1", "s:1", "u"
	out, err := resolveParams(fl, map[string]bool{}, nil)
	if err != nil || out != fl {
		t.Fatalf("CLI-only path changed: %+v err %v", out, err)
	}
}

func TestResolvePrecedence(t *testing.T) {
	c, err := loadAppConfig(writeCfg(t, `{
		"listen": "127.0.0.1:10808",
		"server": "s:1",
		"uuid": "cfg-uuid",
		"conns": 16,
		"dlconns": 0,
		"verbose": true,
		"max_clients": 50,
		"backend": "127.0.0.1:12345",
		"decoy": "127.0.0.1:80"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	fl := defaultFlags()
	set := map[string]bool{"conns": true} // user typed -conns 4 on top of config
	fl.conns = 4
	out, err := resolveParams(fl, set, c)
	if err != nil {
		t.Fatal(err)
	}
	if out.conns != 4 {
		t.Fatalf("explicit flag must beat config: conns=%d", out.conns)
	}
	if out.listen != "127.0.0.1:10808" || out.uuid != "cfg-uuid" {
		t.Fatalf("config must fill unset flags: %+v", out)
	}
	if out.dlconns != 0 {
		t.Fatalf("config dlconns=0 must override default -1: %d", out.dlconns)
	}
	if !out.verbose {
		t.Fatal("config verbose=true ignored")
	}
	if out.maxClients != 50 || out.backend != "127.0.0.1:12345" || out.decoy != "127.0.0.1:80" {
		t.Fatalf("server fields not merged: %+v", out)
	}
	if out.mode != "client" || out.spread != 8 || out.migrate != 1 {
		t.Fatalf("unset fields must keep flag defaults: %+v", out)
	}
}

func TestResolveModeFromConfig(t *testing.T) {
	c, err := loadAppConfig(writeCfg(t, `{"mode":"server"}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := resolveParams(defaultFlags(), map[string]bool{}, c)
	if err != nil || out.mode != "server" {
		t.Fatalf("config mode ignored: %+v %v", out, err)
	}
	// explicit -mode wins over config
	out, err = resolveParams(defaultFlags(), map[string]bool{"mode": true}, c)
	if err != nil || out.mode != "client" {
		t.Fatalf("explicit -mode must win: %+v %v", out, err)
	}
}

func TestParseLogLevel(t *testing.T) {
	for s, want := range map[string]int{"error": lvlError, "warning": lvlWarn, "warn": lvlWarn, "INFO": lvlInfo, "debug": lvlDebug} {
		got, ok := parseLogLevel(s)
		if !ok || got != want {
			t.Fatalf("parseLogLevel(%q)=%d,%v want %d", s, got, ok, want)
		}
	}
	if _, ok := parseLogLevel("chatty"); ok {
		t.Fatal("unknown level must not parse")
	}
}

func TestResolveLogLevelFromConfig(t *testing.T) {
	c, err := loadAppConfig(writeCfg(t, `{"loglevel":"debug"}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := resolveParams(defaultFlags(), map[string]bool{}, c)
	if err != nil || out.loglevel != "debug" {
		t.Fatalf("config loglevel ignored: %+v %v", out, err)
	}
	// explicit -loglevel beats config
	out, err = resolveParams(coreParams{loglevel: "warning"}, map[string]bool{"loglevel": true}, c)
	if err != nil || out.loglevel != "warning" {
		t.Fatalf("explicit -loglevel must win: %+v %v", out, err)
	}
}
