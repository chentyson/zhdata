package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type serverConfig struct {
	listen     string
	usersPath  string
	backend    string // "direct" or socks addr like 127.0.0.1:12345
	maxClients int
	decoy      string // optional host:port for fallback splice of non-protocol traffic
	verbose    bool
}

type serverApp struct {
	cfg serverConfig
	ks  *keyStore

	lns    []net.Listener
	mu     sync.Mutex
	conns  map[string]*sSession // uuid(+slot) -> current session
	active int
	estab  atomic.Uint64 // sessions established (for tests/observability)

	stmMu   sync.Mutex          // guards streams map + sStream.members
	streams map[string]*sStream // "uuid#sid" -> stream (GLOBAL per uuid: one app stream may span sessions)

	failMu sync.Mutex
	fails  map[string]*failWin

	dnsMu sync.Mutex // v0.9.1: multi-resolver fallback + cache (system 8.8.8.8 answers NXDOMAIN for many CN/CDN hosts under poison/throttle)
	dns   map[string]dnsEntry

	rstMu sync.Mutex       // v0.9.1: suppress duplicate tReset for late frames of already-dead sids (frame-storm amplification: one dead stream x hundreds of in-flight frames)
	rst   map[string]int64 // streamKey -> unixnano of last reset sent

	stopCh  chan struct{}
	stopped atomic.Bool
}

type dnsEntry struct {
	ips  []string
	expA int64 // unixnano expiry
}

type sSession struct {
	uuid string
	sess *session

	smu     sync.Mutex
	streams map[uint16]*sStream // membership view: sids carried by this session
	closed  bool
}

// sStream is one proxied TCP connection. v0.4: its UPLOAD frames may arrive
// on ANY session the client attached (in-stream spread); each DATA payload
// carries a 4-byte sequence number and is reassembled to order before being
// replayed to the backend. Downlink still flows through the owner session.
type sStream struct {
	owner *sSession
	uuid  string
	sid   uint16

	mu          sync.Mutex // reassembler + backend fields
	backend     net.Conn
	upq         chan []byte   // async upstream write queue (dispatch never blocks on a slow backend)
	dialDone    chan struct{} // closed when backend is set and pre-dial frames flushed
	rx          reassembler
	rxWake      chan struct{}               // fires when stream state advances (wakes parked dispatch lanes)
	laneHi      map[*sSession]*laneAckState // per-member upload frame counts (tAck credit)
	written     int64                       // forensic: bytes written upstream for this stream
	lastParkLog time.Time                   // rate-limit park logging (st.mu)
	closed      bool
	finDone     bool // CloseWrite already issued to backend

	members map[*sSession]bool // sessions carrying this stream's frames (guarded by stmMu)
}

// reassembler orders seq-tagged upload frames within one stream.
// next: next frame number to emit to the backend. buf holds out-of-order
// frames waiting for a hole to fill; bufN bounds the reorder window.
type reassembler struct {
	next uint32 // next sequence number to emit (first frame = 1)
	buf  map[uint32][]byte
	bufN int
	fin  uint32 // finSeq from tClose (0 = not received)
}

type failWin struct {
	n    int
	from time.Time
}

func newServerApp(cfg serverConfig) (*serverApp, error) {
	ks, err := newKeyStore(cfg.usersPath)
	if err != nil {
		return nil, err
	}
	if cfg.maxClients <= 0 {
		cfg.maxClients = 100
	}
	lns := []net.Listener(nil)
	for _, spec := range splitCSV(cfg.listen) {
		ln, err := net.Listen("tcp", spec)
		if err != nil {
			for _, l := range lns {
				l.Close()
			}
			return nil, err
		}
		lns = append(lns, ln)
	}
	a := &serverApp{
		cfg:     cfg,
		ks:      ks,
		lns:     lns,
		conns:   map[string]*sSession{},
		streams: map[string]*sStream{},
		fails:   map[string]*failWin{},
		dns:     map[string]dnsEntry{},
		rst:     map[string]int64{},
		stopCh:  make(chan struct{}),
	}
	addrs := make([]string, 0, len(lns))
	for _, ln := range lns {
		addrs = append(addrs, ln.Addr().String())
	}
	log.Printf("[server] listening on %v, users=%d, backend=%s, max-clients=%d",
		addrs, len(ks.snapshot()), cfg.backend, cfg.maxClients)
	for _, ln := range lns {
		go a.acceptLoop(ln)
	}
	go a.watchKeys()
	return a, nil
}

// splitCSV splits a comma-separated list, trimming spaces, dropping empties.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// watchKeys hot-reloads the user key sources every keyReloadInterval.
func (a *serverApp) watchKeys() {
	t := time.NewTicker(keyReloadInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			a.ks.reloadIfChanged()
		case <-a.stopCh:
			return
		}
	}
}

func (a *serverApp) Addr() string { return a.lns[0].Addr().String() }

// Addrs lists all bound listen addresses.
func (a *serverApp) Addrs() []string {
	out := make([]string, 0, len(a.lns))
	for _, ln := range a.lns {
		out = append(out, ln.Addr().String())
	}
	return out
}

func (a *serverApp) Close() {
	if a.stopped.CompareAndSwap(false, true) {
		close(a.stopCh)
		for _, ln := range a.lns {
			ln.Close()
		}
		a.mu.Lock()
		for _, sh := range a.conns {
			sh.closeAll(a)
		}
		a.mu.Unlock()
	}
}

func (a *serverApp) logf(format string, v ...any) {
	if a.cfg.verbose {
		log.Printf("[server] "+format, v...)
	}
}

func (a *serverApp) acceptLoop(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if !a.stopped.Load() {
				log.Printf("[server] accept: %v", err)
			}
			return
		}
		go a.handle(c)
	}
}

func (a *serverApp) handle(c net.Conn) {
	tuneTCP(c)
	raddr, _, _ := net.SplitHostPort(c.RemoteAddr().String())
	c.SetDeadline(deadline(handshakeTimeout))
	buf := make([]byte, handshakeLen)
	if _, err := io.ReadFull(c, buf); err != nil {
		a.logf("handshake read from %s: %v", raddr, err)
		c.Close()
		return
	}
	c.SetDeadline(noDeadline())
	salt, enc := buf[:16], buf[16:48]

	uuid, rx := "", cipherStream(nil)
	var slot byte
	for _, u := range a.ks.snapshot() {
		ctr := newCTR(u, salt)
		pt := make([]byte, 32)
		ctr.XORKeyStream(pt, enc)
		if string(pt[:4]) == magic {
			uuid, rx = u, ctr
			slot = pt[4] // handshake slot byte (0 = single-session legacy)
			break
		}
	}
	if uuid == "" {
		if !a.allowFail(raddr) {
			a.logf("handshake rate limited from %s", raddr)
			c.Close()
			return
		}
		if a.cfg.decoy != "" && looksHTTP(buf[:8]) {
			a.spliceDecoy(c, buf)
			return
		}
		a.logf("unknown handshake from %s", raddr)
		c.Close()
		return
	}

	// takeover + max clients. Sessions are keyed per (uuid,slot): a client
	// with -conns N holds N independent sessions that never take each other
	// over; slot 0 keeps the legacy single-session semantics.
	if slot >= maxSlots {
		slot = 0
	}
	connKey := uuid
	if slot > 0 {
		connKey = uuid + "#" + string(rune('0'+slot))
	}
	a.mu.Lock()
	if a.active >= a.cfg.maxClients {
		a.mu.Unlock()
		log.Printf("[server] max clients reached, rejecting %s from %s", uuid, raddr)
		c.Close()
		return
	}
	old := a.conns[connKey]
	a.active++
	a.mu.Unlock()

	if old != nil {
		log.Printf("[server] takeover uuid=%s slot=%d (old conn from different client)", short(uuid), slot)
		old.sess.sendCtrl(Frame{Typ: tGoaway})
		go func() {
			time.Sleep(300 * time.Millisecond)
			old.closeAll(a)
		}()
	}

	sess := newSession("srv:"+short(uuid), c, newCTR(uuid, salt), rx)
	sh := &sSession{uuid: uuid, sess: sess, streams: map[uint16]*sStream{}}
	a.mu.Lock()
	a.conns[connKey] = sh
	a.mu.Unlock()
	a.estab.Add(1)
	log.Printf("[server] session established uuid=%s slot=%d from %s", short(uuid), slot, raddr)

	err := sess.run(func(f *Frame) { a.dispatch(sh, f) })
	sh.closeAll(a)
	a.mu.Lock()
	a.active--
	if a.conns[connKey] == sh {
		delete(a.conns, connKey)
	}
	a.mu.Unlock()
	log.Printf("[server] session ended uuid=%s slot=%d: %v", short(uuid), slot, err)
}

func streamKey(uuid string, sid uint16) string {
	return uuid + "#" + strconv.FormatUint(uint64(sid), 10)
}

// wakeAll hands a wake token to every parked dispatch lane of this stream
// (buffered chan sized maxSlots = one token per possible parked session;
// a single token would thunder-herd and re-park, deadlocking the lane set).
func (st *sStream) wakeAll() {
	for i := 0; i < cap(st.rxWake); i++ {
		select {
		case st.rxWake <- struct{}{}:
		default:
			return
		}
	}
}

// laneAckState tracks how many upload frames one session has delivered for a
// stream; every ackEvery frames the server sends tAck(count) back so the
// client can pace each lane by its own drain speed (pull credit, v0.5).
type laneAckState struct {
	count uint32
	acked uint32
}

func (a *serverApp) dispatch(sh *sSession, f *Frame) {
	switch f.Typ {
	case tOpen:
		atyp, addr, port, err := decodeAddrPort(f.Payload)
		if err != nil {
			a.logf("tReset sid=%d reason=bad-topen (%v)", f.SID, err)
			sh.sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
			return
		}
		key := streamKey(sh.uuid, f.SID)
		a.stmMu.Lock()
		if sh.closed {
			a.stmMu.Unlock()
			a.logf("tReset sid=%d reason=session-closed", f.SID)
			sh.sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
			return
		}
		st := a.streams[key]
		if st == nil {
			if len(a.streams) >= maxStreams {
				a.stmMu.Unlock()
				a.logf("tReset sid=%d reason=max-streams", f.SID)
				sh.sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
				return
			}
			st = &sStream{
				owner:    sh,
				uuid:     sh.uuid,
				sid:      f.SID,
				upq:      make(chan []byte, 128),
				dialDone: make(chan struct{}),
				rx:       reassembler{next: 1, buf: map[uint32][]byte{}},
				rxWake:   make(chan struct{}, maxSlots),
				laneHi:   map[*sSession]*laneAckState{sh: {}},
				members:  map[*sSession]bool{sh: true},
			}
			a.streams[key] = st
			a.stmMu.Unlock()
			sh.smu.Lock()
			sh.streams[f.SID] = st
			sh.smu.Unlock()
			go a.openBackend(sh, st, atyp, addr, port)
			return
		}
		// in-stream spread: this session attaches an already-open stream
		st.members[sh] = true
		a.stmMu.Unlock()
		st.mu.Lock()
		st.laneHi[sh] = &laneAckState{} // fresh counters per attach/reopen
		st.mu.Unlock()
		sh.smu.Lock()
		sh.streams[f.SID] = st
		sh.smu.Unlock()

	case tData:
		if f.SID == 0 {
			return // v0.8 training padding: not a stream, discard silently
		}
		if len(f.Payload) < 4 {
			a.logf("tReset sid=%d reason=short-tdata", f.SID)
			sh.sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
			return
		}
		seq := binary.BigEndian.Uint32(f.Payload[:4])
		data := f.Payload[4:]
		sh.smu.Lock()
		st := sh.streams[f.SID]
		sh.smu.Unlock()
		if st == nil {
			if a.resetOnce(streamKey(sh.uuid, f.SID)) {
				a.logf("tReset sid=%d reason=unknown-stream (tdata for closed/unopened sid) seq=%d", f.SID, seq)
				sh.sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
			}
			return
		}
		for {
			st.mu.Lock()
			if st.closed {
				st.mu.Unlock()
				return
			}
			rx := &st.rx
			if seq < rx.next {
				st.mu.Unlock()
				return // already emitted (duplicate)
			}
			if rx.next == 1 && len(rx.buf) == 0 && seq > gapSkip {
				a.logf("reopen gap sid=%d seq %d->%d (frames lost with dead session)", f.SID, rx.next, seq)
				rx.next = seq
			}
			if _, dup := rx.buf[seq]; !dup {
				rx.buf[seq] = data
				rx.bufN += len(data)
			}
			needAck := false
			var ackN uint32
			if seq >= rx.next {
				la := st.laneHi[sh]
				if la == nil {
					la = &laneAckState{}
					st.laneHi[sh] = la
				}
				la.count++
				if la.count-la.acked >= ackEvery {
					la.acked = la.count
					needAck = true
					ackN = la.count
				}
			}
			var ready [][]byte
			if st.backend != nil {
				for {
					b, ok := rx.buf[rx.next]
					if !ok {
						break
					}
					delete(rx.buf, rx.next)
					rx.bufN -= len(b)
					rx.next++
					ready = append(ready, b)
				}
				if len(ready) > 0 {
					// progress: wake every lane parked on this stream
					st.wakeAll()
				}
			}
			finNow := st.backend != nil && rx.fin != 0 && rx.next >= rx.fin && !st.finDone
			if finNow {
				st.finDone = true
			}
			// Still overfull after best-effort drain: our lane raced far ahead
			// of a stalled one. Park THIS session's reader instead of killing
			// the stream (our TCP window closes toward the client -> true
			// per-stream flow control across spread lanes).
			parked := seq >= rx.next && rx.bufN > maxReorderBytes
			if parked && time.Since(st.lastParkLog) > 5*time.Second {
				st.lastParkLog = time.Now()
				a.logf("reorder park sid=%d bufN=%d next=%d (session reader blocked: HOL for all streams on this session)", f.SID, rx.bufN, rx.next)
			}
			ackBufN := rx.bufN // reorder pressure feedback for client lane credit
			upq := st.upq
			bc := st.backend
			st.mu.Unlock()
			if needAck {
				p := make([]byte, 8)
				binary.BigEndian.PutUint32(p, ackN)
				binary.BigEndian.PutUint32(p[4:], uint32(ackBufN))
				// credit is monotonic state, not ordered data: ctrlQ keeps it
				// flowing even when the session's downlink loQ is congested.
				sh.sess.sendCtrl(Frame{Typ: tAck, SID: f.SID, Payload: p})
			}
			stalled := false
			for _, b := range ready {
				select {
				case upq <- b:
				default:
					// Queue full: the app outruns this backend. Backpressure
					// the session (TCP window closes toward the client) rather
					// than destroying a perfectly valid stream. Only a hard
					// stall resets, and only this stream.
					select {
					case upq <- b:
					case <-sh.sess.done:
						return
					case <-time.After(streamStallTO):
						a.logf("backend stalled sid=%d", f.SID)
						stalled = true
					}
				}
				if stalled {
					break
				}
			}
			if stalled {
				a.closeStream(st, true)
				return
			}
			if finNow && bc != nil {
				if tc, ok := bc.(*net.TCPConn); ok {
					tc.CloseWrite()
				}
			}
			if parked {
				select {
				case <-st.rxWake:
					continue
				case <-sh.sess.done:
					return
				case <-time.After(gapWaitTO):
					a.logf("reorder gap stalled sid=%d (lane dead?)", f.SID)
					a.closeStream(st, true)
					return
				}
			}
			return
		}

	case tClose: // client half-close (payload = finSeq) -> CloseWrite backend after reorder
		var fin uint32
		if len(f.Payload) >= 4 {
			fin = binary.BigEndian.Uint32(f.Payload[:4])
		}
		sh.smu.Lock()
		st := sh.streams[f.SID]
		sh.smu.Unlock()
		if st == nil {
			return
		}
		st.mu.Lock()
		if st.rx.fin == 0 {
			st.rx.fin = fin
		}
		finNow := st.backend != nil && st.rx.next >= st.rx.fin && !st.finDone
		if finNow {
			st.finDone = true
		}
		bc := st.backend
		st.mu.Unlock()
		if finNow && bc != nil {
			if tc, ok := bc.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
		}

	case tReset:
		sh.smu.Lock()
		st := sh.streams[f.SID]
		sh.smu.Unlock()
		if st != nil {
			a.closeStream(st, false)
		}
	}
}

// lookupHost resolves via a chain of public resolvers (8.8.8.8 -> 1.1.1.1 ->
// 223.5.5.5) with a 5-minute cache. The host's default resolver (8.8.8.8)
// intermittently returns "no such host" for CN/CDN domains under poisoning
// or rate limiting; background Windows traffic (store/ads/telemetry) then
// storm-fails every dial. Fallback + cache fixes both latency and flapping.
func (a *serverApp) lookupHost(host string) ([]string, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}, nil
	}
	now := time.Now().UnixNano()
	a.dnsMu.Lock()
	e, ok := a.dns[host]
	a.dnsMu.Unlock()
	if ok && e.expA > now {
		return e.ips, nil
	}
	var lastErr error
	for _, rs := range []string{"8.8.8.8:53", "1.1.1.1:53", "223.5.5.5:53"} {
		r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, proto, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: 3 * time.Second}
			return d.DialContext(ctx, "udp", rs)
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ips, err := r.LookupHost(ctx, host)
		cancel()
		if err == nil && len(ips) > 0 {
			a.dnsMu.Lock()
			if len(a.dns) > 4096 {
				for k, old := range a.dns {
					if old.expA < now {
						delete(a.dns, k)
					}
				}
			}
			a.dns[host] = dnsEntry{ips: ips, expA: now + int64(5*time.Minute)}
			a.dnsMu.Unlock()
			return ips, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// resetOnce rate-limits "unknown-stream" resets: a dead sid with hundreds of
// late in-flight frames must not send hundreds of resets back.
func (a *serverApp) resetOnce(key string) bool {
	now := time.Now().UnixNano()
	a.rstMu.Lock()
	defer a.rstMu.Unlock()
	if t, ok := a.rst[key]; ok && now-t < int64(10*time.Second) {
		return false
	}
	if len(a.rst) > 4096 {
		for k, t := range a.rst {
			if now-t > int64(60*time.Second) {
				delete(a.rst, k)
			}
		}
	}
	a.rst[key] = now
	return true
}

func (a *serverApp) openBackend(sh *sSession, st *sStream, atyp byte, addr []byte, port uint16) {
	var bc net.Conn
	var err error
	if a.cfg.backend == "direct" {
		host := addrHost(atyp, addr)
		ips, lerr := a.lookupHost(host)
		if lerr != nil {
			a.logf("backend dial %s failed: lookup: %v", addrStr(atyp, addr, port), lerr)
			a.closeStream(st, true)
			return
		}
		for _, ip := range ips {
			bc, err = net.DialTimeout("tcp", net.JoinHostPort(ip, fmt.Sprint(port)), backendDialTimeout)
			if err == nil {
				break
			}
		}
	} else {
		bc, err = socksDial(a.cfg.backend, atyp, addr, port)
	}
	if err != nil {
		a.logf("backend dial %s failed: %v", addrStr(atyp, addr, port), err)
		a.closeStream(st, true)
		return
	}
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		bc.Close()
		return
	}
	st.backend = bc
	// replay pre-dial frames strictly in sequence order
	var werr error
	for {
		b, ok := st.rx.buf[st.rx.next]
		if !ok {
			break
		}
		bc.SetWriteDeadline(deadline(backendWriteTO))
		if _, err = bc.Write(b); err != nil {
			werr = err
			break
		}
		st.written += int64(len(b))
		delete(st.rx.buf, st.rx.next)
		st.rx.bufN -= len(b)
		st.rx.next++
	}
	finNow := werr == nil && st.rx.fin != 0 && st.rx.next >= st.rx.fin && !st.finDone
	if finNow {
		st.finDone = true
	}
	close(st.dialDone) // after this, dispatch enqueues to upq; no orphan frames
	st.mu.Unlock()
	// wake any lane parked pre-dial (nothing could emit until now)
	st.wakeAll()
	if werr != nil {
		a.logf("backend pre-flush err sid=%d: %v", st.sid, werr)
		a.closeStream(st, true)
		return
	}
	if finNow {
		if tc, ok := bc.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}
	sh.sess.sendCtrl(Frame{Typ: tOpenAck, SID: st.sid})
	a.logf("backend connected %s (%s)", addrStr(atyp, addr, port), a.cfg.backend)
	go a.pumpUp(st, sh)

	// pump backend -> client (downlink flows through the owner session)
	buf := make([]byte, maxPayload)
	total := 0
	var rerr error
	for {
		bc.SetReadDeadline(noDeadline())
		n, err := bc.Read(buf)
		if n > 0 {
			total += n
			payload := make([]byte, n)
			copy(payload, buf[:n])
			sh.sess.sendData(Frame{Typ: tData, SID: st.sid, Payload: payload})
		}
		if err != nil {
			rerr = err
			break
		}
	}
	// backend EOF/error: notify client
	sh.sess.sendOrdered(Frame{Typ: tClose, SID: st.sid})
	a.logf("backend pump done sid=%d bytes=%d err=%v", st.sid, total, rerr)
	a.closeStream(st, false)
}

// pumpUp drains one stream's upload queue into its backend on a dedicated
// goroutine. A stalled backend can then never block the session's dispatch
// (other streams' data, OPEN/ACK, control plane) — the head-of-line fix.
func (a *serverApp) pumpUp(st *sStream, sh *sSession) {
	for {
		select {
		case p := <-st.upq:
			st.mu.Lock()
			bc := st.backend
			st.mu.Unlock()
			if bc == nil {
				continue
			}
			bc.SetWriteDeadline(deadline(backendWriteTO))
			n, err := bc.Write(p)
			st.mu.Lock()
			st.written += int64(n)
			st.mu.Unlock()
			if err != nil {
				a.logf("pumpUp write err sid=%d written=%d err=%v", st.sid, st.written, err)
				a.closeStream(st, true)
				return
			}
		case <-sh.sess.done:
			a.logf("pumpUp exit sid=%d written=%d", st.sid, st.written)
			return
		}
	}
}

// closeStream removes a stream from the global map, detaches every member
// session, closes the backend, and (optionally) resets on every member.
func (a *serverApp) closeStream(st *sStream, sendReset bool) {
	a.stmMu.Lock()
	if a.streams[streamKey(st.uuid, st.sid)] != st {
		a.stmMu.Unlock()
		return // already replaced/closed
	}
	delete(a.streams, streamKey(st.uuid, st.sid))
	members := make([]*sSession, 0, len(st.members))
	for m := range st.members {
		members = append(members, m)
	}
	st.members = nil
	a.stmMu.Unlock()

	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true
	bc := st.backend
	st.backend = nil
	st.mu.Unlock()
	st.wakeAll() // release any parked dispatch lanes
	if bc != nil {
		bc.Close()
	}
	if sendReset {
		a.logf("closeStream sid=%d RESET sent written=%d members=%d", st.sid, st.written, len(members))
	}
	for _, m := range members {
		m.smu.Lock()
		if m.streams[st.sid] == st {
			delete(m.streams, st.sid)
		}
		m.smu.Unlock()
		if sendReset && !m.closed {
			m.sess.sendCtrl(Frame{Typ: tReset, SID: st.sid})
		}
	}
}

// detach removes one session from a stream; if the session owned the stream,
// the whole stream dies (downlink flows only through the owner). Otherwise
// the stream lives on the remaining sessions (spread resilience).
func (a *serverApp) detach(sh *sSession, st *sStream) {
	if st.owner == sh {
		a.closeStream(st, true)
		return
	}
	a.stmMu.Lock()
	delete(st.members, sh)
	empty := len(st.members) == 0
	a.stmMu.Unlock()
	if empty {
		a.closeStream(st, false)
		return
	}
	// A spread lane died: its in-flight frames are gone, so any hole they
	// left can never be filled. Truncate the gap immediately (documented
	// loss semantics, same as v0.3 in-flight loss) and wake parked lanes;
	// higher-seq frames already buffered keep flowing.
	st.mu.Lock()
	if !st.closed {
		maxSeq := st.rx.next
		for s := range st.rx.buf {
			if s > maxSeq {
				maxSeq = s
			}
		}
		skipped := 0
		for st.rx.next <= maxSeq {
			if _, ok := st.rx.buf[st.rx.next]; ok {
				break
			}
			skipped++
			st.rx.next++
		}
		if skipped > 0 {
			a.logf("gap truncate sid=%d skip=%d frames (lane lost)", st.sid, skipped)
			st.wakeAll()
		}
	}
	st.mu.Unlock()
}

func (sh *sSession) closeAll(a *serverApp) {
	sh.smu.Lock()
	sh.closed = true
	sts := make([]*sStream, 0, len(sh.streams))
	for sid, st := range sh.streams {
		sts = append(sts, st)
		delete(sh.streams, sid)
	}
	sh.smu.Unlock()
	for _, st := range sts {
		if a != nil {
			a.detach(sh, st)
		}
	}
	if sh.sess != nil {
		sh.sess.close()
	}
}

// ---------------- anti-probe: per-IP handshake failure rate limit ----------------

func (a *serverApp) allowFail(ip string) bool {
	a.failMu.Lock()
	defer a.failMu.Unlock()
	w := a.fails[ip]
	if w == nil || time.Since(w.from) > time.Minute {
		a.fails[ip] = &failWin{n: 1, from: time.Now()}
		return true
	}
	w.n++
	if len(a.fails) > 4096 { // opportunistic cleanup
		for k, v := range a.fails {
			if time.Since(v.from) > time.Minute {
				delete(a.fails, k)
			}
		}
	}
	return w.n <= 10
}

func looksHTTP(b []byte) bool {
	s := string(b)
	for _, m := range []string{"GET ", "POST ", "HEAD ", "PUT ", "HTTP/1"} {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

func (a *serverApp) spliceDecoy(c net.Conn, head []byte) {
	dc, err := net.DialTimeout("tcp", a.cfg.decoy, 5*time.Second)
	if err != nil {
		c.Close()
		return
	}
	dc.Write(head)
	done := make(chan struct{})
	go func() { io.Copy(dc, c); dc.(*net.TCPConn).CloseWrite(); close(done) }()
	io.Copy(c, dc)
	c.Close()
	dc.Close()
	<-done
}

// ---------------- helpers ----------------

func addrHost(atyp byte, addr []byte) string {
	switch atyp {
	case atypIPv4:
		return net.IPv4(addr[0], addr[1], addr[2], addr[3]).String()
	case atypIPv6:
		return net.IP(addr).String()
	default:
		return string(addr)
	}
}

func addrStr(atyp byte, addr []byte, port uint16) string {
	return fmt.Sprintf("%s:%d", addrHost(atyp, addr), port)
}

func short(uuid string) string {
	if len(uuid) <= 8 {
		return uuid
	}
	return uuid[:8]
}
