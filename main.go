package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const version = "0.13.2" // zhenhanvn first-class core: -c config.json, stdout logs, mixed SOCKS5+HTTP local listener; loglevel now supports zhenhanvn's none/error/warning/info/debug levels

func main() {
	mode := flag.String("mode", "client", "client | server")
	listen := flag.String("listen", "", "listen address; comma separated for multi-port (client: local mixed socks5+http proxy; server: zhdata public ports)")
	server := flag.String("server", "", "zhdata server host:port, comma separated for multi-port slots (client)")
	uuid := flag.String("uuid", "", "user uuid (client)")
	users := flag.String("users", "", "user key sources: comma separated txt/sqlite paths, e.g. /etc/s-ui/private/s-ui.db,/opt/zhdata/users.txt (server)")
	backend := flag.String("backend", "direct", `backend: "direct" or socks addr e.g. 127.0.0.1:12345 (server)`)
	maxClients := flag.Int("max-clients", 100, "max concurrent sessions per server (sessions = users x client -conns)")
	decoy := flag.String("decoy", "", "optional host:port to splice non-protocol HTTP-looking traffic (server)")
	verbose := flag.Bool("v", false, "verbose logging")
	conns := flag.Int("conns", 1, "parallel tunnel sessions 1-64 (client; independent slots/buckets)")
	spread := flag.Int("spread", 8, "max lanes one bulk upload stream may fan across (client; v0.11 retransmit makes fan-out safe; auto-capped to conns/live-bulk-streams, set 1 to disable)")
	train := flag.Int("train", 0, "seconds between per-session idle training frames; 0 = off (v0.8 experiment: keep ISP shaping buckets warm)")
	migrate := flag.Int("migrate", 1, "stalled/slow-lane whole-backlog migration: 1 = on (v0.11.18 default; confined to upload lanes when split pools active), 0 = off")
	dlconns := flag.Int("dlconns", -1, "split pools: last N sessions are download-only homes, rest upload-only lanes (-1 = auto conns/2 (v0.11.18 default), 0 = shared)")
	slotbase := flag.Int("slotbase", 0, "first slot index for this instance (v0.11.20; run two clients side by side, e.g. -conns 32 -slotbase 32, without takeover conflicts)")
	pprofAddr := flag.String("pprof", "", "enable net/http/pprof (goroutine/heap dumps) on this address, e.g. 127.0.0.1:6060; empty = off")
	configPath := flag.String("c", "", "JSON config file (zhenhanvn core mode, see config.client.example.json); explicit flags below override it")
	logLevelFlag := flag.String("loglevel", "info", "log level: error | warning | info | debug (debug == same detail as -v)")
	showVer := flag.Bool("version", false, "print version")
	flag.Parse()

	if *showVer {
		// zhenhanvn UpdateService parses X.Y.Z out of this line.
		fmt.Println("zhdata", version)
		return
	}
	log.SetFlags(log.LstdFlags)
	// zhenhanvn core contract: logs on stdout so the GUI shows them.
	log.SetOutput(os.Stdout)

	flagSet := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { flagSet[f.Name] = true })
	var appCfg *appConfig
	if *configPath != "" {
		var err error
		if appCfg, err = loadAppConfig(*configPath); err != nil {
			fatal("%v", err)
		}
	}
	p, err := resolveParams(coreParams{
		mode:       *mode,
		listen:     *listen,
		server:     *server,
		uuid:       *uuid,
		users:      *users,
		backend:    *backend,
		decoy:      *decoy,
		pprof:      *pprofAddr,
		loglevel:   *logLevelFlag,
		maxClients: *maxClients,
		conns:      *conns,
		spread:     *spread,
		train:      *train,
		migrate:    *migrate,
		dlconns:    *dlconns,
		slotbase:   *slotbase,
		verbose:    *verbose,
	}, flagSet, appCfg)
	if err != nil {
		fatal("config: %v", err)
	}

	// log level gate: explicit -loglevel > config loglevel > (-v => debug) > info
	lvl, lvlOK := parseLogLevel(p.loglevel)
	switch {
	case flagSet["loglevel"]:
		if !lvlOK {
			fatal("unknown -loglevel %q (want error|warning|info|debug)", p.loglevel)
		}
		logLevel = lvl
	case p.verbose:
		logLevel = lvlDebug
	case lvlOK && lvl != lvlInfo:
		logLevel = lvl
	default:
		logLevel = lvlInfo
	}
	if !flagSet["loglevel"] && strings.TrimSpace(p.loglevel) != "" && !lvlOK {
		logWarn("main", "unknown loglevel %q in config, using %q", p.loglevel, lvlName[logLevel])
	}

	if p.pprof != "" {
		go func() {
			logInfo("pprof", "listening on http://%s/debug/pprof/", p.pprof)
			if err := http.ListenAndServe(p.pprof, nil); err != nil {
				logErr("pprof", "server: %v", err)
			}
		}()
	}

	switch p.mode {
	case "server":
		if p.listen == "" || p.users == "" {
			fatal("server mode requires -listen and -users (or config listen/users)")
		}
		app, err := newServerApp(serverConfig{
			listen:     p.listen,
			usersPath:  p.users,
			backend:    p.backend,
			maxClients: p.maxClients,
			decoy:      p.decoy,
			verbose:    p.verbose,
		})
		if err != nil {
			fatal("server: %v", err)
		}
		waitSignal()
		app.Close()
	case "client":
		if p.listen == "" || p.server == "" || p.uuid == "" {
			fatal("client mode requires -listen, -server and -uuid (or config listen/server/uuid)")
		}
		app, err := newClientApp(clientConfig{
			listen:   p.listen,
			server:   p.server,
			uuid:     p.uuid,
			conns:    p.conns,
			spread:   p.spread,
			train:    p.train,
			migrate:  p.migrate,
			dlconns:  p.dlconns,
			slotbase: p.slotbase,
			verbose:  p.verbose,
		})
		if err != nil {
			fatal("client: %v", err)
		}
		logInfo("client", "mixed socks5+http listening on %s -> %s", app.Addr(), p.server)
		waitSignal()
		app.Close()
	default:
		fatal("unknown mode %q", p.mode)
	}
}

func fatal(format string, v ...any) {
	logErr("main", format, v...)
	os.Exit(1)
}

func waitSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}
