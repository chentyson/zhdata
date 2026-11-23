package zhdata

// Byte-level pass-through relay backend (v0.15.2).
//
// With `-backend zhdata` + `-upstream h:p[,h:p...]` a zhdata server stops
// terminating the tunnel locally: after identifying the user from the 48B
// handshake it pipes the handshake and every following ciphertext byte to an
// upstream zhdata server. The upstream therefore sees exactly what a direct
// connection would show — same uuid, device_id, slot, keystream — so
// end-to-end encryption and (uuid,device,slot) takeover semantics survive the
// hop. The relay never parses a frame and never injects one: any byte it
// produced or consumed would desynchronise the AES-CTR keystreams.
//
// The control plane stays on the relay, exactly like the direct path:
//   - the user set (keysource: file/sqlite + admin API) still gates access;
//   - per-(uuid,device_id) device limits and max-sessions are enforced here,
//     rejecting with the same plaintext auth-reject reasons (2/3);
//   - tunnel bytes are counted into the same per-uuid traffic counters, so
//     GET /v1/stats covers relayed traffic;
//   - admin kick closes the pipes (clients back off 1-2s and reconnect; the
//     panel must remove the user from the pushed set first, or they just
//     reconnect upstream again).
//
// Bandwidth caps (v0.15.6): a relay meters the caps of the users it serves too,
// from its own user sources / admin push (bwlimit.go). The panel pushes the same
// table to the relay and to the terminating server, so a capped user passes two
// buckets of the same rate in series; a rate limiter in series does not stack --
// the steady rate stays the cap, each hop only gets its own one-second burst.
//
// P0 limits (documented): no retry on upstream dial failure (the downstream
// connection is closed), one upstream TCP connection per downstream session (no
// separate trunk session pool).

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	backendZhdata = "zhdata"        // -backend value that selects the relay
	relayBufSize  = 32 << 10        // copy buffer per direction
	relayIdleTO   = 2 * time.Minute // a relayed session pings every 30s; 2 min of silence = dead pipe
)

// relayState is the per-server relay configuration plus the live pipe set.
// It is hot-updatable through the admin API (PUT /v1/relay); changes only
// affect new connections, existing pipes keep their upstream.
type relayState struct {
	mu       sync.Mutex
	enabled  bool
	upstream []string
	rr       uint32                  // round-robin cursor over upstream
	pipes    map[*relayPipe]struct{} // live pipes
}

// relayPipe is one relayed downstream session: 1 downstream TCP = 1 upstream TCP.
type relayPipe struct {
	app       *serverApp
	uuid      string
	deviceID  [16]byte
	hasDevice bool
	slot      byte
	remote    string    // downstream peer address (the panel's client machine)
	started   time.Time // when the pipe was accepted
	down      net.Conn
	up        net.Conn
	once      sync.Once
}

func newRelayState(backend, upstream string) (*relayState, error) {
	rs := &relayState{pipes: map[*relayPipe]struct{}{}}
	if strings.TrimSpace(backend) != backendZhdata {
		return rs, nil
	}
	addrs, err := parseUpstream(upstream)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf(`-backend %q requires -upstream host:port[,...]`, backendZhdata)
	}
	rs.enabled = true
	rs.upstream = addrs
	logInfo("relay", "byte-level relay to %v (round-robin per connection)", addrs)
	return rs, nil
}

// parseUpstream normalises and validates a comma-separated host:port list.
func parseUpstream(spec string) ([]string, error) {
	var out []string
	for _, p := range splitCSV(spec) {
		if _, _, err := net.SplitHostPort(p); err != nil {
			return nil, fmt.Errorf("bad upstream address %q: %v", p, err)
		}
		out = append(out, p)
	}
	return out, nil
}

func (rs *relayState) enabledNow() bool {
	if rs == nil {
		return false
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.enabled
}

// target picks the next upstream (round-robin per connection).
func (rs *relayState) target() (string, error) {
	if rs == nil {
		return "", fmt.Errorf("relay backend is not active")
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.enabled || len(rs.upstream) == 0 {
		return "", fmt.Errorf("relay enabled but no upstream configured")
	}
	addr := rs.upstream[int(rs.rr)%len(rs.upstream)]
	rs.rr++
	return addr, nil
}

// snapshot reports the admin-visible state.
func (rs *relayState) snapshot() (enabled bool, upstream []string, pipes int) {
	if rs == nil {
		return false, nil, 0
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.enabled, append([]string(nil), rs.upstream...), len(rs.pipes)
}

// listPipes reports the live pipes as session rows for GET /v1/sessions. A
// relay process terminates no tunnel of its own, so this IS its session set
// (relay=true); read-only.
func (rs *relayState) listPipes() []sessionInfo {
	if rs == nil {
		return nil
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]sessionInfo, 0, len(rs.pipes))
	for p := range rs.pipes {
		dev := ""
		if p.hasDevice {
			dev = deviceHex(p.deviceID)
		}
		out = append(out, sessionInfo{
			UUID:      p.uuid,
			Device:    dev,
			Slot:      int(p.slot),
			Remote:    p.remote,
			StartedAt: p.started.Unix(),
			Relay:     true,
		})
	}
	return out
}

// apply is the PUT /v1/relay mutation: replace the upstream list and/or the
// enabled flag. Enabling without any upstream is rejected.
func (rs *relayState) apply(upstream *[]string, enabled *bool) (bool, int, error) {
	if rs == nil {
		return false, 0, fmt.Errorf("relay backend is not initialised on this server")
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if upstream == nil && enabled == nil {
		return rs.enabled, len(rs.upstream), fmt.Errorf(`body must carry "upstream" and/or "enabled"`)
	}
	next := rs.upstream
	if upstream != nil {
		parsed, err := parseUpstream(strings.Join(*upstream, ","))
		if err != nil {
			return rs.enabled, len(rs.upstream), err
		}
		next = parsed
	}
	on := rs.enabled
	if enabled != nil {
		on = *enabled
	}
	if on && len(next) == 0 {
		return rs.enabled, len(rs.upstream), fmt.Errorf("relay enabled but upstream list is empty")
	}
	rs.enabled = on
	rs.upstream = next
	return on, len(next), nil
}

func (rs *relayState) add(p *relayPipe) {
	if rs == nil {
		return
	}
	rs.mu.Lock()
	if rs.pipes == nil {
		rs.pipes = map[*relayPipe]struct{}{}
	}
	rs.pipes[p] = struct{}{}
	rs.mu.Unlock()
}

func (rs *relayState) remove(p *relayPipe) {
	if rs == nil {
		return
	}
	rs.mu.Lock()
	delete(rs.pipes, p)
	rs.mu.Unlock()
}

// kick closes every pipe whose uuid matches; returns how many were cut.
func (rs *relayState) kick(uuid string) int {
	if rs == nil {
		return 0
	}
	rs.mu.Lock()
	var targets []*relayPipe
	for p := range rs.pipes {
		if strings.EqualFold(p.uuid, uuid) {
			targets = append(targets, p)
		}
	}
	rs.mu.Unlock()
	for _, p := range targets {
		p.close()
	}
	return len(targets)
}

// handleRelay is the relay branch of the accept path: the 48B handshake is
// already parsed (uuid/version/slot/device_id in hand) and still buffered in
// head, so it can be replayed to the upstream verbatim.
func (a *serverApp) handleRelay(c net.Conn, head []byte, uuid, version string, slot byte, deviceID [16]byte, hasDevice bool, raddr string) {
	wantAuth := version == magic || version == "zhv6"
	if slot >= maxSlots {
		slot = 0
	}
	upAddr, err := a.relay.target()
	if err != nil {
		logWarn("server", "relay: %v, rejecting uuid=%s from %s", err, short(uuid), raddr)
		if wantAuth {
			writeAuthReject(c, authReasonGeneric)
		}
		c.Close()
		return
	}

	// Same session/device accounting as the direct path: the relay is where the
	// panel's user set and device limits live, so max-sessions (reason 2) and
	// max_devices (reason 3) are answered here, without touching the upstream.
	a.mu.Lock()
	if a.cfg.maxSessions > 0 && a.active >= a.cfg.maxSessions {
		a.mu.Unlock()
		logWarn("server", "max sessions reached, rejecting %s from %s (active=%d limit=%d) [relay]",
			short(uuid), raddr, a.active, a.cfg.maxSessions)
		if wantAuth {
			writeAuthReject(c, authReasonMaxSess)
		}
		c.Close()
		return
	}
	pol := a.ks.policy(uuid)
	if hasDevice && pol.MaxDevices > 0 {
		dm := a.devices[uuid]
		if _, ok := dm[deviceID]; !ok && len(dm) >= pol.MaxDevices {
			a.mu.Unlock()
			logWarn("server", "device limit reached, rejecting uuid=%s device=%s slot=%d from %s (devices=%d limit=%d) [relay]",
				short(uuid), shortDevice(deviceID), slot, raddr, len(dm), pol.MaxDevices)
			if wantAuth {
				writeAuthReject(c, authReasonDevice)
			}
			c.Close()
			return
		}
	}
	a.active++
	if hasDevice {
		dm := a.devices[uuid]
		if dm == nil {
			dm = map[[16]byte]int{}
			a.devices[uuid] = dm
		}
		dm[deviceID]++
	}
	a.mu.Unlock()

	uc, err := net.DialTimeout("tcp", upAddr, backendDialTimeout)
	if err != nil {
		a.relayRelease(uuid, deviceID, hasDevice)
		logWarn("server", "relay: dial upstream %s for uuid=%s from %s failed: %v (closing downstream, P0 does not retry)",
			upAddr, short(uuid), raddr, err)
		if wantAuth {
			writeAuthReject(c, authReasonGeneric)
		}
		c.Close()
		return
	}
	tuneTCP(uc)
	p := &relayPipe{
		app:       a,
		uuid:      uuid,
		deviceID:  deviceID,
		hasDevice: hasDevice,
		slot:      slot,
		remote:    c.RemoteAddr().String(), // downstream peer ip:port
		started:   time.Now(),
		down:      c,
		up:        uc,
	}
	a.relay.add(p)
	a.estab.Add(1)
	logInfo("server", "relay pipe uuid=%s device=%s slot=%d from %s -> %s", short(uuid), shortDevice(deviceID), slot, raddr, upAddr)

	if _, err := uc.Write(head); err != nil {
		logWarn("server", "relay: handshake replay to %s failed: %v", upAddr, err)
		p.close()
		return
	}

	done := make(chan struct{}, 2)
	go func() { relayCopy(a, uc, c, uuid, true); done <- struct{}{} }()  // client -> upstream (upload)
	go func() { relayCopy(a, c, uc, uuid, false); done <- struct{}{} }() // upstream -> client (download)
	<-done
	p.close()
	<-done
}

// relayCopy moves raw bytes and charges them to the uuid's counters. It never
// interprets what it copies (frame boundaries are meaningless at this layer)
// and gives up on idle pipes so a half-open TCP cannot pin a session slot.
func relayCopy(a *serverApp, dst, src net.Conn, uuid string, up bool) {
	buf := make([]byte, relayBufSize)
	for {
		src.SetReadDeadline(deadline(relayIdleTO))
		n, err := src.Read(buf)
		if n > 0 {
			a.addTraffic(uuid, up, int64(n))
			// capped user: park before the copy. The pipe has no stream to
			// watch, so a dead pipe is noticed by the write failing below.
			if !a.meterBW(uuid, up, int64(n), 0, nil) {
				return
			}
			dst.SetWriteDeadline(deadline(writeTimeout))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logDbg("relay", "idle %v, closing pipe uuid=%s", relayIdleTO, short(uuid))
			}
			return
		}
	}
}

// relayRelease undoes the session/device bookkeeping done by handleRelay.
func (a *serverApp) relayRelease(uuid string, deviceID [16]byte, hasDevice bool) {
	a.mu.Lock()
	a.active--
	if hasDevice {
		dm := a.devices[uuid]
		if dm != nil {
			if dm[deviceID] <= 1 {
				delete(dm, deviceID)
				if len(dm) == 0 {
					delete(a.devices, uuid)
				}
			} else {
				dm[deviceID]--
			}
		}
	}
	a.mu.Unlock()
}

// close tears down the pipe exactly once: both TCP ends die (no frames are
// injected, so a polite tGoaway is impossible at this layer — the client sees
// its session die and reconnects on its normal backoff).
func (p *relayPipe) close() {
	p.once.Do(func() {
		_ = p.down.Close()
		_ = p.up.Close()
		p.app.relay.remove(p)
		p.app.relayRelease(p.uuid, p.deviceID, p.hasDevice)
	})
}
