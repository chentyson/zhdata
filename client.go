package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type clientConfig struct {
	listen  string // local socks5 listen addr
	server  string // zhdata server host:port; comma separated for multi-port
	uuid    string
	conns   int // parallel tunnel sessions (1..maxSlots)
	spread  int // max lanes one bulk stream may use (1 disables in-stream spread)
	train   int // seconds between per-session training frames; 0 = off (v0.8 experiment)
	verbose bool
}

// clientApp keeps cfg.conns parallel zhdata sessions (slots 0..conns-1) to
// the server. Local socks connections are assigned to the least loaded
// session. Each slot has independent reconnect/takeover state; the server
// applies takeover per (uuid,slot), so parallel slots do not GOAWAY each other.
//
// v0.4 in-stream parallelism: a single bulk upload stream is spread across
// several sessions so it can fill more of the line's per-flow shaping buckets.
// Every upload frame carries a stream-global sequence number; the server
// reorders by seq before replaying to the backend. Stream ids (sid) are
// globally unique per uuid so one sid can appear on multiple sessions.
type clientApp struct {
	cfg      clientConfig
	listener net.Listener

	mu      sync.Mutex
	conns   []*cConn
	servers []string            // dial targets; slot i dials servers[i % len(servers)]
	streams map[uint32]*cStream // all live streams
	sidSeq  uint16              // sid allocator, unique per uuid across sessions
	bulk    int                 // live streams transferring bulk upload; drives the auto spread budget
	lanes   int                 // total spread lanes attached across all streams; hard cap = cfg.conns (never oversubscribe the slot pool)

	stopCh  chan struct{}
	stopped atomic.Bool

	train []byte // periodic per-session training payload (keeps shaping buckets + congestion state warm)
}

// cConn is one tunnel session slot and its stream bookkeeping.
type cConn struct {
	app *clientApp
	idx byte

	// guarded by app.mu
	sess    *session
	gen     uint64
	sessMap map[uint16]*cStream // sid -> stream (this session's namespace)
	members map[uint32]*cStream // streams that use this slot (home or spread)
}

type cStream struct {
	app   *clientApp
	id    uint32
	sid   uint16   // globally unique per uuid; same sid rides every attached session
	home  *cConn   // owns the stream (downlink flows here)
	conns []*cConn // home + extra spread slots, all carrying this sid

	local net.Conn
	atyp  byte
	addr  []byte
	port  uint16

	rr       int               // (kept for diagnostics; lane choice is credit-based)
	bulk     bool              // stream has exceeded the bulk threshold (counts toward spread budget)
	attached int               // spread lanes beyond home held by this stream (counts toward global lane cap)
	laneSent map[*cConn]uint32 // frames sent per attached lane (app.mu)
	laneAck  map[*cConn]uint32 // frames confirmed per lane via tAck (app.mu)
	laneCap  map[*cConn]int    // per-lane in-flight credit, AIMD (app.mu)
	rq       chan []byte       // remote -> local queue
	done     chan struct{}
	doneOne  sync.Once
}

// ensureLaneLocked initializes per-lane credit state for an attached slot.
// Caller must hold app.mu.
func (s *cStream) ensureLaneLocked(cn *cConn) {
	if s.laneSent == nil {
		s.laneSent = map[*cConn]uint32{}
		s.laneAck = map[*cConn]uint32{}
		s.laneCap = map[*cConn]int{}
	}
	if _, ok := s.laneCap[cn]; !ok {
		s.laneCap[cn] = laneCapInit
	}
}

func newClientApp(cfg clientConfig) (*clientApp, error) {
	// key comparison is case-insensitive (server users list is normalized to
	// lowercase); normalize here so "Iammaster1125" == "iammaster1125".
	cfg.uuid = strings.ToLower(strings.TrimSpace(cfg.uuid))
	if cfg.conns < 1 {
		cfg.conns = 1
	}
	if cfg.conns > maxSlots {
		cfg.conns = maxSlots
	}
	if cfg.spread < 1 {
		cfg.spread = 8 // v0.5 pull credit: robust across uneven lanes
	}
	if cfg.spread > cfg.conns {
		cfg.spread = cfg.conns
	}
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return nil, err
	}
	servers := splitCSV(cfg.server)
	if len(servers) == 0 {
		return nil, fmt.Errorf("no server address")
	}
	a := &clientApp{
		cfg:      cfg,
		listener: ln,
		servers:  servers,
		streams:  map[uint32]*cStream{},
		stopCh:   make(chan struct{}),
	}
	for i := 0; i < cfg.conns; i++ {
		a.conns = append(a.conns, &cConn{
			app:     a,
			idx:     byte(i),
			sessMap: map[uint16]*cStream{},
			members: map[uint32]*cStream{},
		})
	}
	go a.acceptLoop()
	for _, cn := range a.conns {
		go cn.sessionLoop()
	}
	a.train = make([]byte, trainPayload)
	if cfg.train > 0 {
		go a.trainLoop()
	}
	return a, nil
}

// trainLoop pushes one padding frame per live session on each tick. The
// non-blocking push means an already-busy session is skipped: training only
// spends bytes when the session is idle, which is exactly when its TCP
// congestion state and the ISP's per-flow shaping bucket go cold.
func (a *clientApp) trainLoop() {
	t := time.NewTicker(time.Duration(a.cfg.train) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
		}
		a.mu.Lock()
		for _, cn := range a.conns {
			sess := cn.sess
			if sess == nil {
				continue
			}
			for k := 0; k < trainFrames; k++ {
				select {
				case sess.loQ <- Frame{Typ: tData, SID: 0, Payload: a.train}:
				default:
				}
			}
		}
		a.mu.Unlock()
	}
}

func (a *clientApp) Addr() string { return a.listener.Addr().String() }

func (a *clientApp) Close() {
	if a.stopped.CompareAndSwap(false, true) {
		close(a.stopCh)
		a.listener.Close()
		a.mu.Lock()
		for _, cn := range a.conns {
			if cn.sess != nil {
				cn.sess.close()
			}
		}
		a.mu.Unlock()
	}
}

func (a *clientApp) logf(format string, v ...any) {
	if a.cfg.verbose {
		log.Printf("[client] "+format, v...)
	}
}

// ---------------- local socks side ----------------

func (a *clientApp) acceptLoop() {
	var nextID uint32
	for {
		c, err := a.listener.Accept()
		if err != nil {
			if !a.stopped.Load() {
				log.Printf("[client] accept: %v", err)
			}
			return
		}
		c.SetDeadline(deadline(handshakeTimeout))
		atyp, addr, port, err := socksAccept(c)
		c.SetDeadline(noDeadline())
		if err != nil {
			a.logf("socks accept: %v", err)
			c.Close()
			continue
		}
		s := &cStream{
			app:   a,
			id:    atomic.AddUint32(&nextID, 1),
			local: c,
			atyp:  atyp, addr: addr, port: port,
			rq:   make(chan []byte, 32),
			done: make(chan struct{}),
		}
		a.mu.Lock()
		a.streams[s.id] = s
		s.sid = a.allocSidLocked()
		home := a.pickConnLocked()
		s.home = home
		s.conns = []*cConn{home}
		home.members[s.id] = s
		s.ensureLaneLocked(home)
		if home.sess != nil {
			home.openStreamLocked(s)
		}
		a.mu.Unlock()
		go s.pumpUp()   // local -> remote (spreads over conns, survives reconnects)
		go s.pumpDown() // remote -> local
	}
}

// pickConnLocked chooses the slot with the fewest assigned streams.
// Caller holds a.mu.
func (a *clientApp) pickConnLocked() *cConn {
	best := a.conns[0]
	for _, cn := range a.conns[1:] {
		if len(cn.members) < len(best.members) {
			best = cn
		}
	}
	return best
}

// allocSidLocked returns a sid unused in any current session. Caller holds a.mu.
func (a *clientApp) allocSidLocked() uint16 {
	for n := 0; n < 0xFFFF; n++ {
		a.sidSeq++
		if a.sidSeq == 0 {
			a.sidSeq = 1
		}
		sid := a.sidSeq
		inUse := false
		for _, cn := range a.conns {
			if _, dup := cn.sessMap[sid]; dup {
				inUse = true
				break
			}
		}
		if !inUse {
			return sid
		}
	}
	return 0 // sid space exhausted (pathological)
}

// ---------------- per-slot session management ----------------

// openStreamLocked registers stream s's sid in this slot's session and sends
// OPEN. Caller holds a.mu.
func (cn *cConn) openStreamLocked(s *cStream) {
	if s.sid == 0 || cn.sess == nil {
		return
	}
	cn.sessMap[s.sid] = s
	cn.sess.sendCtrl(Frame{Typ: tOpen, SID: s.sid, Payload: encodeAddrPort(s.atyp, s.addr, s.port)})
}

// currentSidLocked resolves the sid of stream s in this slot's session.
// Caller holds a.mu.
func (cn *cConn) currentSidLocked(s *cStream) uint16 {
	for sid, st := range cn.sessMap {
		if st == s {
			return sid
		}
	}
	return 0
}

// waitOpen blocks until stream s has a live session+sid on slot cn.
func (cn *cConn) waitOpen(s *cStream) bool {
	for {
		cn.app.mu.Lock()
		sess := cn.sess
		sid := cn.currentSidLocked(s)
		cn.app.mu.Unlock()
		if sess != nil && sid != 0 {
			return true
		}
		select {
		case <-s.done:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (cn *cConn) dialSession() (*session, error) {
	a := cn.app
	target := a.servers[int(cn.idx)%len(a.servers)]
	c, err := net.DialTimeout("tcp", target, handshakeTimeout)
	if err != nil {
		return nil, err
	}
	tuneTCP(c)
	c.SetDeadline(deadline(handshakeTimeout))
	salt := randBytes(16)
	tx := newCTR(a.cfg.uuid, salt)
	hp := make([]byte, 32)
	copy(hp, magic)
	hp[4] = cn.idx // session slot: server keys takeover per (uuid,slot)
	copy(hp[5:], randBytes(8))
	tx.XORKeyStream(hp, hp) // tx consumed 32 keystream bytes; frames continue after
	if _, err = c.Write(append(salt, hp...)); err != nil {
		c.Close()
		return nil, err
	}
	c.SetDeadline(noDeadline())
	rx := newCTR(a.cfg.uuid, salt)
	return newSession("client->"+target, c, tx, rx), nil
}

func (cn *cConn) sessionLoop() {
	a := cn.app
	backoff := time.Second
	goawayStreak := 0
	for {
		select {
		case <-a.stopCh:
			return
		default:
		}
		sess, err := cn.dialSession()
		if err != nil {
			a.logf("slot %d dial: %v (retry in %v)", cn.idx, err, backoff)
			if !sleepSelect(a.stopCh, backoff) {
				return
			}
			backoff = minDur(backoff*2, 30*time.Second)
			continue
		}
		a.mu.Lock()
		cn.sess = sess
		cn.gen++
		cn.sessMap = map[uint16]*cStream{}
		for _, s := range cn.members {
			s.ensureLaneLocked(cn)
			s.laneSent[cn] = 0 // fresh session: server lane counters restart too
			s.laneAck[cn] = 0
			cn.openStreamLocked(s)
		}
		gen := cn.gen
		nstreams := len(cn.members)
		a.mu.Unlock()
		backoff = time.Second
		if a.cfg.conns == 1 {
			log.Printf("[client] session established (gen=%d, reopen streams=%d)", gen, nstreams)
		} else {
			log.Printf("[client] session established slot=%d (gen=%d, reopen streams=%d)", cn.idx, gen, nstreams)
		}
		estStart := time.Now()
		err = sess.run(func(f *Frame) { cn.dispatch(gen, f) })
		a.mu.Lock()
		if cn.gen == gen {
			cn.sess = nil
			cn.sessMap = map[uint16]*cStream{}
		}
		a.mu.Unlock()
		switch err {
		case errGoaway:
			// another connection holds this (uuid,slot). If our session died
			// fast, we are the losing side of a takeover war: back off
			// exponentially instead of churn-reconnecting at 1/s.
			if time.Since(estStart) < 15*time.Second {
				goawayStreak++
			} else {
				goawayStreak = 0
			}
			shift := goawayStreak
			if shift > 6 {
				shift = 6
			}
			d := minDur(time.Duration(1<<shift)*time.Second, 60*time.Second)
			log.Printf("[client] slot %d taken over (goaway), reconnect in %v", cn.idx, d)
			if !sleepSelect(a.stopCh, d) {
				return
			}
		default:
			goawayStreak = 0
			a.logf("slot %d session ended: %v (retry in %v)", cn.idx, err, backoff)
			if !sleepSelect(a.stopCh, backoff) {
				return
			}
			backoff = minDur(backoff*2, 30*time.Second)
		}
	}
}

func (cn *cConn) dispatch(gen uint64, f *Frame) {
	a := cn.app
	a.mu.Lock()
	if gen != cn.gen {
		a.mu.Unlock()
		return
	}
	s := cn.sessMap[f.SID]
	sess := cn.sess
	a.mu.Unlock()

	switch f.Typ {
	case tOpenAck:
		a.logf("open ack slot=%d sid=%d", cn.idx, f.SID)
	case tData:
		if s == nil {
			if sess != nil {
				sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
			}
			return
		}
		select {
		case s.rq <- f.Payload:
		case <-s.done:
		}
	case tClose:
		if s != nil {
			// nil sentinel rides the same FIFO lane as DATA so we never
			// CloseWrite the local socket before queued chunks are flushed.
			select {
			case s.rq <- nil:
			case <-s.done:
			}
		}
	case tReset:
		if s != nil {
			s.app.logf("tReset received sid=%d slot=%d -> teardown local conn", f.SID, cn.idx)
			s.teardown()
		}
	case tAck:
		if s != nil && len(f.Payload) >= 4 {
			var bufN uint32
			if len(f.Payload) >= 8 {
				bufN = binary.BigEndian.Uint32(f.Payload[4:])
			}
			s.applyAck(cn, binary.BigEndian.Uint32(f.Payload), bufN)
		}
	}
}

// ---------------- stream pumps ----------------

func (s *cStream) teardown() {
	s.doneOne.Do(func() {
		close(s.done)
		s.local.Close()
		s.app.mu.Lock()
		if s.bulk {
			s.bulk = false
			if s.app.bulk > 0 {
				s.app.bulk--
			}
		}
		if s.attached > 0 {
			s.app.lanes -= s.attached
			if s.app.lanes < 0 {
				s.app.lanes = 0
			}
			s.attached = 0
		}
		delete(s.app.streams, s.id)
		for _, cn := range s.conns {
			delete(cn.members, s.id)
			for sid, st := range cn.sessMap {
				if st == s {
					delete(cn.sessMap, sid)
				}
			}
		}
		s.app.mu.Unlock()
	})
}

func (s *cStream) closeWriteLocal() {
	s.app.logf("stream %d closeWriteLocal", s.id)
	if tc, ok := s.local.(*net.TCPConn); ok {
		tc.CloseWrite()
	} else {
		s.teardown()
	}
}

// dbgLog prints per-stream upload pacing state (ZHD_DEBUG2=1): bytes/frames
// actually pushed into lanes, average frame size, and every attached lane's
// credit window (cap) and outstanding (out). This is what separates "app is
// slow" from "credit clock limits the lane" in the field.
func (s *cStream) dbgLog(sent, frames int64, t0 time.Time) {
	s.app.mu.Lock()
	var b strings.Builder
	for _, cn := range s.conns {
		out := int(s.laneSent[cn]) - int(s.laneAck[cn])
		fmt.Fprintf(&b, " s%d:cap%d/out%d", cn.idx, s.laneCap[cn], out)
	}
	lanes := len(s.conns)
	s.app.mu.Unlock()
	avg := int64(0)
	if frames > 0 {
		avg = sent / frames
	}
	log.Printf("[dbg] sid=%d sent=%d frames=%d avg=%dB lanes=%d el=%s%s",
		s.sid, sent, frames, avg, lanes, time.Since(t0).Round(time.Second), b.String())
}

// pickLane chooses the attached lane with the most credit
// (laneCap - in-flight frames) and takes one frame of credit atomically.
// Returns (nil, true) when live lanes exist but all are saturated (caller
// should wait: pull pacing, slow lanes naturally take fewer frames), and
// (nil, false) when no attached lane has a live session (reconnect window).
// Caller must NOT hold app.mu.
func (s *cStream) pickLane() (*cConn, bool) {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	var best *cConn
	bestCredit := int64(0)
	live := false
	for _, cn := range s.conns {
		if cn.sess == nil || cn.currentSidLocked(s) == 0 {
			continue
		}
		live = true
		out := int64(s.laneSent[cn]) - int64(s.laneAck[cn])
		credit := int64(s.laneCap[cn]) - out
		if credit > bestCredit {
			best, bestCredit = cn, credit
		}
	}
	if best != nil {
		s.laneSent[best]++
	}
	return best, live
}

// applyAck records a lane's delivered-frame count and adapts its credit
// window (v0.5 semantics, restored as the stable baseline): drained fast ->
// grow, backing up -> shrink toward the lane's own bandwidth-delay product.
// v0.7's reorder-pressure (bufN) variant is kept on the wire (tAck 8B) but
// ignored here; it oscillated on this link (deep dips / stall cycles).
func (s *cStream) applyAck(cn *cConn, n uint32, bufN uint32) {
	_ = bufN
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if n > s.laneAck[cn] {
		s.laneAck[cn] = n
	}
	out := int64(s.laneSent[cn]) - int64(s.laneAck[cn])
	cn2 := s.laneCap[cn]
	switch {
	case out*4 >= int64(cn2)*3:
		cn2 = cn2 * 3 / 4
		if cn2 < laneCapMin {
			cn2 = laneCapMin
		}
	case out*2 <= int64(cn2):
		cn2 *= 2
		if cn2 > laneCapMax {
			cn2 = laneCapMax
		}
	}
	s.laneCap[cn] = cn2
}

// anyLiveLane reports whether at least one attached slot has a live
// session+sid for this stream.
func (s *cStream) anyLiveLane() bool {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	for _, cn := range s.conns {
		if cn.sess != nil && cn.currentSidLocked(s) != 0 {
			return true
		}
	}
	return false
}

// waitOpenAny blocks until at least one attached slot has a live session+sid
// for this stream. Returns false if the stream is done.
func (s *cStream) waitOpenAny() bool {
	for {
		if s.anyLiveLane() {
			return true
		}
		select {
		case <-s.done:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// trySpread grows the set of slots carrying this stream so a single bulk
// upload fills multiple per-flow shaping buckets. The lane budget is
// AUTO: min(cfg.spread, conns / live bulk streams) — a lone bulk stream
// may fan across all slots, but when a speedtest floods 15+ big streams
// each stream keeps a dedicated lane (spreading would just slice every
// lane into unusable crumbs). Adds one slot per spreadThreshold bytes sent.
// detachLastDrainedLocked trims one spread lane (searching newest-first)
// whose frames are fully acked. Pure client-side op: the lane is drained,
// so the server simply stops seeing frames for this sid on that session;
// its stale laneHi entry is reclaimed at stream end. No wire signal and
// no fin semantics involved.
func (s *cStream) detachLastDrainedLocked() bool {
	for i := len(s.conns) - 1; i >= 1; i-- {
		cn := s.conns[i]
		if s.laneSent[cn] == s.laneAck[cn] {
			delete(cn.members, s.id)
			s.conns = append(s.conns[:i], s.conns[i+1:]...)
			s.attached--
			if s.attached < 0 {
				s.attached = 0
			}
			s.app.lanes--
			if s.app.lanes < 0 {
				s.app.lanes = 0
			}
			s.app.logf("stream %d rebalance: detach slot %d (lanes now %d)", s.id, cn.idx, s.app.lanes)
			return true
		}
	}
	return false
}

// rebalanceLocked enforces the fair per-stream lane budget against EARLY
// HOGGING: a stream that spread while few bulk streams existed keeps its
// lanes when new bulk streams arrive, starving them (latecomers find the
// global lane pool exhausted). Trim everyone toward the current budget.
func (a *clientApp) rebalanceLocked(budget int) {
	for _, st := range a.streams {
		for st.bulk && len(st.conns) > budget {
			if !st.detachLastDrainedLocked() {
				break
			}
		}
	}
}

func (s *cStream) trySpread(sent int64) {
	s.app.mu.Lock()
	marked := false
	if sent >= spreadThreshold && !s.bulk {
		s.bulk = true
		s.app.bulk++
		marked = true
	}
	budget := s.app.cfg.spread
	if s.app.bulk > 0 {
		if b := s.app.cfg.conns / s.app.bulk; b < budget {
			budget = b
		}
	}
	if budget < 1 {
		budget = 1
	}
	if marked {
		s.app.rebalanceLocked(budget) // fair share just shrank: trim hogs immediately
	}
	if len(s.conns) < budget && s.app.lanes >= s.app.cfg.conns {
		s.app.rebalanceLocked(budget) // pool saturated: take from over-budget streams first
	}
	if sent >= spreadThreshold*int64(len(s.conns)) && len(s.conns) < budget && s.app.lanes < s.app.cfg.conns {
		// pick the least-loaded slot not yet carrying this stream
		var best *cConn
		for _, cn := range s.app.conns {
			if _, has := cn.members[s.id]; has {
				continue
			}
			if best == nil || len(cn.members) < len(best.members) {
				best = cn
			}
		}
		if best != nil {
			s.conns = append(s.conns, best)
			s.attached++
			s.app.lanes++
			best.members[s.id] = s
			s.ensureLaneLocked(best)
			if best.sess != nil {
				best.openStreamLocked(s)
			}
			s.app.logf("stream %d spread to %d slots (sid=%d, sent=%d)", s.id, len(s.conns), s.sid, sent)
		}
	}
	s.app.mu.Unlock()
}

// sendFin sends a half-close carrying finSeq (the next seq value) over every
// attached slot that has a live session, so the server can finish reorder and
// CloseWrite the backend.
func (s *cStream) sendFin(finSeq uint32) {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, finSeq)
	s.app.mu.Lock()
	for _, cn := range s.conns {
		if cn.sess != nil && cn.currentSidLocked(s) != 0 {
			cn.sess.sendOrdered(Frame{Typ: tClose, SID: s.sid, Payload: payload})
		}
	}
	s.app.mu.Unlock()
}

// pumpUp: local -> remote. One goroutine per stream for its whole life;
// chunks are sequence-numbered and fanned out across attached sessions.
func (s *cStream) pumpUp() {
	buf := make([]byte, maxPayload)
	var sent int64
	var frames int64
	var seq uint32 // next sequence number (1-based)
	dbgOn := os.Getenv("ZHD_DEBUG2") != ""
	t0 := time.Now()
	dbgLast := time.Now()
	for {
		// wait until this stream is OPEN on at least one live session
		if !s.waitOpenAny() {
			if s.app.cfg.verbose {
				log.Printf("[client] pumpUp exit stream=%d sent=%d (session gone)", s.id, sent)
			}
			return
		}
		s.local.SetReadDeadline(deadline(2 * time.Minute))
		n, err := s.local.Read(buf)
		if n > 0 {
			sent += int64(n)
			seq++
			payload := make([]byte, n+4)
			binary.BigEndian.PutUint32(payload[:4], seq)
			copy(payload[4:], buf[:n])
			// Wait for lane credit; during a session-reconnect window keep the
			// chunk (retrying the same seq) instead of silently dropping it:
			// the server dedupes by seq, and a dropped chunk would permanently
			// corrupt the app stream. Give up only after a hard 2min stall.
			waitStart := time.Now()
			for {
				cn, live := s.pickLane()
				if cn != nil {
					cn.sess.sendData(Frame{Typ: tData, SID: s.sid, Payload: payload})
					frames++
					break
				}
				if !live && time.Since(waitStart) > 2*time.Minute {
					s.teardown()
					return
				}
				// all lanes saturated (or reconnecting): wait for tAck credit
				if !sleepSelect(s.done, 20*time.Millisecond) {
					return
				}
			}
			// else: reconnect window, chunk dropped (in-flight loss, documented)
			s.trySpread(sent)
			if dbgOn && time.Since(dbgLast) > 2*time.Second {
				dbgLast = time.Now()
				s.dbgLog(sent, frames, t0)
			}
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue // idle read timeout, keep pumping
			}
			// EOF / error: half-close (finSeq = next seq) then teardown.
			// EOF is the NORMAL end of an app stream (speedtests open
			// hundreds of tiny ones); only log abnormal terminations.
			if s.app.cfg.verbose && !streamChurn(err) {
				log.Printf("[client] pumpUp err stream=%d sent=%d err=%v", s.id, sent, err)
			}
			if s.app.cfg.verbose && sent >= spreadThreshold {
				avg := int64(0)
				if frames > 0 {
					avg = sent / frames
				}
				log.Printf("[client] pumpUp done sid=%d sent=%d frames=%d avg=%dB dur=%s",
					s.sid, sent, frames, avg, time.Since(t0).Round(time.Millisecond))
			}
			s.sendFin(seq + 1)
			s.teardown()
			return
		}
	}
}

// pumpDown: remote queue -> local socket.
func (s *cStream) pumpDown() {
	for {
		select {
		case chunk := <-s.rq:
			if chunk == nil {
				s.closeWriteLocal()
				return
			}
			s.local.SetWriteDeadline(deadline(localWriteTO))
			if _, err := s.local.Write(chunk); err != nil {
				s.app.mu.Lock()
				sess := s.home.sess
				sid := s.sid
				s.app.mu.Unlock()
				if sess != nil && sid != 0 {
					sess.sendCtrl(Frame{Typ: tReset, SID: sid})
				}
				s.teardown()
				return
			}
		case <-s.done:
			return
		}
	}
}

// streamChurn reports errors that are the normal noise of apps (especially
// speedtest probe floods) aborting streams: clean EOF, our own teardown
// racing a pending read (ErrClosed), and app-side RSTs (ECONNRESET).
func streamChurn(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "forcibly closed")
}

func sleepSelect(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
