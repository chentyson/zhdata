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

	mu            sync.Mutex
	conns         []*cConn
	servers       []string            // dial targets; slot i dials servers[i % len(servers)]
	streams       map[uint32]*cStream // all live streams
	sidSeq        uint16              // sid allocator, unique per uuid across sessions
	bulk          int                 // live streams transferring bulk upload; drives the auto spread budget
	lanes         int                 // total spread lanes attached across all streams; hard cap = cfg.conns (never oversubscribe the slot pool)
	rr            uint32              // rotating cursor for home-slot selection (spreads streams over all slots)
	inflightBytes int64               // v0.11: bytes enqueued to sessions but not yet DELIVERED by the server, all upload streams (cap globalWindowBytes)

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

	bulk     bool // stream has exceeded the bulk threshold (counts toward spread budget)
	attached int  // spread lanes beyond home held by this stream (counts toward global lane cap)

	// v0.11 (route A) upload send window; all fields guarded by app.mu.
	// A frame stays in sentq until the server reports it DELIVERED (tAck's
	// second word = rx.next-1), so the server reassembler can never be fed
	// more than streamWindowBytes out of order. Frames on a lane that stops
	// acking are retransmitted over the other lanes; the server dedupes by seq.
	sentq         []*sentFrame      // pending frames, ascending seq
	ackedSeq      uint32            // server delivered prefix (last in-order frame)
	inflightBytes int64             // bytes in sentq
	frontSince    time.Time         // when sentq[0] became the front (fast hole-retransmit timer)
	laneAck       map[*cConn]uint32 // per-lane arrival counter (liveness)
	laneBytes     map[*cConn]int64  // pending bytes currently assigned to a lane
	laneSent      map[*cConn]uint32 // frames sent on this lane (for true in-TCP backlog = laneSent-laneAck)
	// v0.11 pacing: in-order delivery of ONE app stream over N lanes can only
	// advance as fast as the lane carrying the next sequence number, so a
	// naive round-robin caps a stream at N x slowest-lane rate. Assign each
	// frame to the lane with the earliest estimated completion (pending/rate);
	// slow lanes then receive proportionally fewer frames and the stream rate
	// becomes ~sum(lane rates) instead of N x min(lane rates).
	laneRate map[*cConn]float64   // EWMA of delivered frames/s per lane
	laneCnt  map[*cConn]uint32    // last arrival count used for the rate EWMA
	laneT    map[*cConn]time.Time // last rate sample time

	rq      chan []byte // remote -> local queue
	done    chan struct{}
	doneOne sync.Once

	resendCount atomic.Int64 // diagnostics: frames retransmitted on another lane
}

// sentFrame is one upload DATA frame kept for retransmission until delivered.
type sentFrame struct {
	seq     uint32
	payload []byte // includes the 4B seq prefix; retransmitted verbatim
	lane    *cConn // lane it is currently assigned to; nil once released
}

// ensureLaneLocked initializes per-lane bookkeeping for an attached slot.
// Caller must hold app.mu.
func (s *cStream) ensureLaneLocked(cn *cConn) {
	if s.laneAck == nil {
		s.laneAck = map[*cConn]uint32{}
		s.laneBytes = map[*cConn]int64{}
		s.laneSent = map[*cConn]uint32{}
		s.laneRate = map[*cConn]float64{}
		s.laneCnt = map[*cConn]uint32{}
		s.laneT = map[*cConn]time.Time{}
	}
	if _, ok := s.laneAck[cn]; !ok {
		s.laneAck[cn] = 0
		s.laneCnt[cn] = 0
		s.laneT[cn] = time.Now()
		s.laneRate[cn] = laneRateInit
	}
	if _, ok := s.laneBytes[cn]; !ok {
		s.laneBytes[cn] = 0
	}
	if _, ok := s.laneSent[cn]; !ok {
		s.laneSent[cn] = 0
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
	go a.stallSweepLoop()
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
			rq:   make(chan []byte, 64),
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

// pickConnLocked chooses the slot with the fewest assigned streams, breaking
// ties with a rotating cursor. Without the rotation every new stream picked
// conns[0] while all slots were idle, so a browser's 4-6 streams all landed on
// the first few slots: those long-lived TCP flows accumulated loss/reordering
// history (client-side CUBIC cwnd collapse, which we cannot tune on Windows)
// while the other ~26 sessions sat unused. Rotating spreads new streams over
// all slots, so every stream starts on a TCP flow with fresh congestion state,
// like the fresh connections the raw-TCP baseline uses.
// Caller holds a.mu.
func (a *clientApp) pickConnLocked() *cConn {
	n := len(a.conns)
	start := int(a.rr % uint32(n))
	a.rr++
	best := a.conns[start]
	for i := 1; i < n; i++ {
		cn := a.conns[(start+i)%n]
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
		// Frames queued on the previous (dead) connection of this lane are gone;
		// collect them before the lane looks alive again, then re-send over the
		// surviving lanes (the server dedupes by seq).
		type laneLost struct {
			s      *cStream
			frames []*sentFrame
		}
		var lost []laneLost
		for _, s := range cn.members {
			if s.home == cn || s.laneBytes[cn] <= 0 {
				continue
			}
			var frs []*sentFrame
			for _, fr := range s.sentq {
				if fr.lane == cn {
					frs = append(frs, fr)
				}
			}
			if len(frs) > 0 {
				lost = append(lost, laneLost{s: s, frames: frs})
			}
		}
		cn.sess = sess
		cn.gen++
		cn.sessMap = map[uint16]*cStream{}
		for _, s := range cn.members {
			s.ensureLaneLocked(cn)
			// Fresh session: the server restarts this lane's arrival counter and
			// the old rate estimate says nothing about the new TCP connection.
			s.laneSent[cn] = 0
			s.laneAck[cn] = 0
			s.laneCnt[cn] = 0
			s.laneT[cn] = time.Now()
			s.laneRate[cn] = laneRateInit
			cn.openStreamLocked(s)
		}
		gen := cn.gen
		nstreams := len(cn.members)
		a.mu.Unlock()
		for _, l := range lost {
			for _, fr := range l.frames {
				l.s.resendFrame(fr)
			}
		}
		backoff = time.Second
		if a.cfg.conns == 1 {
			log.Printf("[client] session established (gen=%d, reopen streams=%d)", gen, nstreams)
		} else {
			log.Printf("[client] session established slot=%d (gen=%d, reopen streams=%d)", cn.idx, gen, nstreams)
		}
		estStart := time.Now()
		err = sess.run(func(f *Frame) {
			t0 := time.Now()
			cn.dispatch(gen, f)
			if d := time.Since(t0); d > 500*time.Millisecond && a.cfg.verbose {
				log.Printf("[client] dispatch slow slot=%d typ=%d sid=%d took=%v", cn.idx, f.Typ, f.SID, d.Round(time.Millisecond))
			}
		})
		a.mu.Lock()
		if cn.gen == gen {
			cn.sess = nil
			cn.sessMap = map[uint16]*cStream{}
		}
		// A dead session cannot be resumed mid-stream: the server-side backend
		// connection and reassembler state died with it, so "re-opening" the
		// stream would dial the target again and splice a fresh response into
		// a half-consumed app socket (downloads hang/corrupt) or silently lose
		// the middle of an upload. Fail the streams homed on this slot fast so
		// the app retries; spread lanes (home elsewhere) keep the stream alive.
		var dead []*cStream
		for _, s := range cn.members {
			if s.home == cn {
				dead = append(dead, s)
			}
		}
		a.mu.Unlock()
		for _, s := range dead {
			a.logf("slot %d lost: failing stream %d (sid=%d) instead of resuming corrupt", cn.idx, s.id, s.sid)
			s.teardown()
		}
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
		default:
			// The app-side reader is momentarily behind. Wait a short, bounded
			// time (a browser JS stall is normally <100ms), then fail ONLY this
			// stream. Never block the session reader indefinitely: that used to
			// freeze every stream on the slot until the peer's 15s write
			// timeout killed the whole TCP session (and the app hung).
			select {
			case s.rq <- f.Payload:
			case <-s.done:
			case <-time.After(2 * time.Second):
				a.logf("downlink queue full sid=%d slot=%d for 2s -> fail stream (app not reading)", f.SID, cn.idx)
				if sess != nil {
					sess.sendCtrl(Frame{Typ: tReset, SID: f.SID})
				}
				s.teardown()
			}
		}
	case tClose:
		if s != nil {
			// nil sentinel rides the same FIFO lane as DATA so we never
			// CloseWrite the local socket before queued chunks are flushed.
			select {
			case s.rq <- nil:
			case <-s.done:
			default:
				// Bounded wait like DATA: a slow app reader must not stall the
				// whole session reader (peer write timeout -> session death).
				select {
				case s.rq <- nil:
				case <-s.done:
				case <-time.After(2 * time.Second):
					a.logf("downlink close queue full sid=%d slot=%d for 2s -> fail stream", f.SID, cn.idx)
					s.teardown()
				}
			}
		}
	case tReset:
		if s != nil {
			s.app.logf("tReset received sid=%d slot=%d -> teardown local conn", f.SID, cn.idx)
			s.teardown()
		}
	case tAck:
		if s != nil && len(f.Payload) >= 4 {
			var delivered uint32
			if len(f.Payload) >= 8 {
				// v0.11: second word is the server's delivered prefix (rx.next-1),
				// not the old reorder-pressure byte count.
				delivered = binary.BigEndian.Uint32(f.Payload[4:])
			}
			s.applyAck(cn, binary.BigEndian.Uint32(f.Payload), delivered)
		}
	}
}

// ---------------- stream pumps ----------------

func (s *cStream) teardown() {
	s.doneOne.Do(func() {
		close(s.done)
		s.local.Close()
		s.app.mu.Lock()
		// Release the upload window accounting held by pending frames.
		for _, fr := range s.sentq {
			n := int64(len(fr.payload))
			s.inflightBytes -= n
			if fr.lane != nil {
				s.laneBytes[fr.lane] -= n
			}
			fr.lane = nil
		}
		s.app.inflightBytes -= s.inflightBytes
		if s.app.inflightBytes < 0 {
			s.app.inflightBytes = 0
		}
		s.sentq = nil
		s.inflightBytes = 0
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
// pushed into lanes, average frame size, delivered prefix, window usage, and
// every lane's pending bytes / ack counter / measured delivery rate.
func (s *cStream) dbgLog(sent, frames int64, t0 time.Time) {
	s.app.mu.Lock()
	var b strings.Builder
	for _, cn := range s.conns {
		fmt.Fprintf(&b, " s%d:pend%dB/ack%d/sent%d/r%.0f", cn.idx, s.laneBytes[cn], s.laneAck[cn], s.laneSent[cn], s.laneRate[cn])
	}
	lanes := len(s.conns)
	inflight, acked := s.inflightBytes, s.ackedSeq
	s.app.mu.Unlock()
	avg := int64(0)
	if frames > 0 {
		avg = sent / frames
	}
	log.Printf("[dbg] sid=%d sent=%d frames=%d avg=%dB lanes=%d inflight=%dB ackedSeq=%d el=%s%s",
		s.sid, sent, frames, avg, lanes, inflight, acked, time.Since(t0).Round(time.Second), b.String())
}

// pickLaneSendLocked picks the live lane whose backlog will drain first:
// score = (true in-TCP backlog frames + 1) / measured lane rate. The backlog is
// laneSent-laneAck (frames handed to the lane's TCP minus frames it has
// already delivered to the server): this is the queue delay a frame assigned
// now will actually experience. (A lane's "unreleased" window bytes are NOT a
// good score: when one hole stalls stream delivery, every lane's frames stay
// unreleased, so that metric would deprioritise the healthy lanes exactly when
// they are needed.) Slow lanes get proportionally fewer frames, so an in-order
// multi-lane stream advances at ~sum(lane rates) instead of N x min(lane rate).
// Returns (nil, true) when lanes exist but none is usable yet.
// Caller must hold app.mu.
func (s *cStream) pickLaneSendLocked() (*cConn, bool) {
	var best *cConn
	bestETA := float64(1 << 62)
	live := false
	now := time.Now()
	for _, cn := range s.conns {
		if cn.sess == nil || cn.currentSidLocked(s) == 0 {
			continue
		}
		live = true
		r := s.laneRate[cn]
		backlog := int64(s.laneSent[cn]) - int64(s.laneAck[cn])
		if backlog < 0 {
			backlog = 0
		}
		// A lane is suspect only when it has frames IN FLIGHT that have not
		// been delivered for a while. An idle lane (nothing outstanding) is
		// NOT slow: v0.11.1 demoted any lane without recent ack progress, so
		// every lane not currently loaded starved (no frames -> no acks ->
		// stale laneT -> min rate -> no frames), collapsing an 8-lane stream
		// to ~1 lane (tunnel x1 12.6M vs raw x1 20.4M).
		if backlog > 0 && now.Sub(s.laneT[cn]) > 400*time.Millisecond {
			r = laneRateMin
		}
		if r < laneRateMin {
			r = laneRateMin
		}
		eta := (float64(backlog) + 1) / r
		if eta < bestETA {
			best, bestETA = cn, eta
		}
	}
	return best, live
}

// applyAck records a lane's arrival counter and delivery rate, and the server's
// delivered-seq prefix. Frames with seq <= delivered are released from the
// window; this is the reliable-delivery signal that makes fan-out safe.
func (s *cStream) applyAck(cn *cConn, count, delivered uint32) {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if count > s.laneAck[cn] {
		now := time.Now()
		dt := now.Sub(s.laneT[cn]).Seconds()
		delta := int(count) - int(s.laneCnt[cn])
		s.laneT[cn] = now // keep "last progress" fresh even for short gaps
		if dt >= 0.02 && delta > 0 {
			r := float64(delta) / dt
			old := s.laneRate[cn]
			if old <= 0 {
				old = r
			}
			// EWMA: react within a few acks but do not chase single bursts.
			s.laneRate[cn] = 0.6*old + 0.4*r
		}
		if delta > 0 {
			s.laneCnt[cn] = count
		}
		s.laneAck[cn] = count
	}
	if delivered > s.ackedSeq {
		s.ackedSeq = delivered
		s.releaseDeliveredLocked()
	}
}

// releaseDeliveredLocked frees every queued frame the server has delivered in
// order. Caller holds app.mu.
func (s *cStream) releaseDeliveredLocked() {
	released := false
	for len(s.sentq) > 0 && s.sentq[0].seq <= s.ackedSeq {
		fr := s.sentq[0]
		s.sentq[0] = nil
		s.sentq = s.sentq[1:]
		n := int64(len(fr.payload))
		s.inflightBytes -= n
		if fr.lane != nil {
			s.laneBytes[fr.lane] -= n
			fr.lane = nil
		}
		s.app.inflightBytes -= n
		if s.app.inflightBytes < 0 {
			s.app.inflightBytes = 0
		}
		released = true
	}
	if released && len(s.sentq) > 0 {
		s.frontSince = time.Now() // a new front frame starts the hole timer
	}
}

// sendFrame enqueues one seq-tagged DATA frame, respecting the per-stream and
// global unacked windows (server-delivered feedback). It blocks (with short
// polls) while the window is full, which is the true end-to-end backpressure.
// Returns false if the stream was torn down meanwhile.
func (s *cStream) sendFrame(seq uint32, payload []byte) bool {
	n := int64(len(payload))
	fr := &sentFrame{seq: seq, payload: payload}
	for {
		s.app.mu.Lock()
		over := s.inflightBytes+n > streamWindowBytes || s.app.inflightBytes+n > globalWindowBytes
		if over {
			s.app.mu.Unlock()
			select {
			case <-s.done:
				return false
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		cn, live := s.pickLaneSendLocked()
		if cn == nil {
			s.app.mu.Unlock()
			if !live {
				if !s.waitOpenAny() {
					return false
				}
			} else {
				if !sleepSelect(s.done, time.Millisecond) {
					return false
				}
			}
			continue
		}
		// Reserve the window slot before enqueueing so accounting is exact.
		s.inflightBytes += n
		s.app.inflightBytes += n
		s.laneBytes[cn] += n
		s.laneSent[cn]++
		fr.lane = cn
		wasEmpty := len(s.sentq) == 0
		s.sentq = append(s.sentq, fr)
		if wasEmpty {
			s.frontSince = time.Now()
		}
		s.app.mu.Unlock()

		if cn.sess.trySendData(Frame{Typ: tData, SID: s.sid, Payload: payload}) {
			return true
		}
		// Lane queue is full (slow lane): give the slot back and pick another.
		s.app.mu.Lock()
		if last := len(s.sentq) - 1; last >= 0 && s.sentq[last] == fr {
			s.sentq[last] = nil
			s.sentq = s.sentq[:last]
		}
		s.inflightBytes -= n
		s.app.inflightBytes -= n
		s.laneBytes[cn] -= n
		s.laneSent[cn]--
		fr.lane = nil
		s.app.mu.Unlock()
		if !sleepSelect(s.done, time.Millisecond) {
			return false
		}
	}
}

// stallSweepLoop periodically runs stall detection over streams with in-flight
// upload data. One app-wide sweep instead of a ticker goroutine per stream
// (browsers/speedtests open hundreds of short streams).
func (a *clientApp) stallSweepLoop() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
		}
		a.mu.Lock()
		sts := make([]*cStream, 0, len(a.streams))
		for _, s := range a.streams {
			if s.inflightBytes > 0 {
				sts = append(sts, s)
			}
		}
		a.mu.Unlock()
		for _, s := range sts {
			select {
			case <-s.done:
				continue
			default:
			}
			s.checkStalls()
		}
	}
}

// checkStalls handles two cases:
//  1. a lane whose session DIED: its in-flight frames are gone forever, so they
//     are re-sent over the surviving lanes and the dead lane is detached once
//     empty;
//  2. a reorder hole: the front (lowest undelivered) frame blocks delivery of
//     everything behind it, so it is fast-retransmitted after ~2 RTT.
//
// A live-but-slow lane is deliberately NOT migrated: TCP does not lose bytes on
// a live connection, its frames will arrive, and migrating them used to pile
// whole windows onto one lane / create a moving hole (livelock). Only lost
// frames (dead session) or an actual hole need action.
func (s *cStream) checkStalls() {
	now := time.Now()
	var frames []*sentFrame
	var dead []*cConn
	s.app.mu.Lock()
	for _, cn := range s.conns {
		if cn.sess != nil || cn == s.home {
			// live lane (slowness is handled by the hole path), or the home
			// session whose death makes sessionLoop fail the whole stream.
			continue
		}
		for _, fr := range s.sentq {
			if fr.lane == cn {
				frames = append(frames, fr)
			}
		}
		dead = append(dead, cn)
	}
	// Fast hole recovery: re-send the front burst as soon as it has been
	// undelivered for ~2 RTT; if both copies were lost the next sweep retries.
	// Only when frames are queued BEHIND the front: a lone pending frame that
	// the backend has not delivered yet is not a hole, and blindly re-sending
	// it every sweep just floods duplicates.
	if len(s.sentq) > 1 && now.Sub(s.frontSince) > holeResendTO {
		n := holeResendBurst
		for i := 0; i < len(s.sentq) && n > 0; i++ {
			if s.sentq[i].lane != nil {
				frames = append(frames, s.sentq[i])
				n--
			}
		}
		s.frontSince = now
	}
	s.app.mu.Unlock()

	for _, fr := range frames {
		s.resendFrame(fr)
	}
	if len(frames) > 0 && os.Getenv("ZHD_DEBUG2") != "" {
		log.Printf("[client] retransmit sid=%d frames=%d lanes=%d total=%d",
			s.sid, len(frames), len(s.conns), s.resendCount.Load())
	}
	for _, cn := range dead {
		s.app.mu.Lock()
		done := s.laneBytes[cn] <= 0
		s.app.mu.Unlock()
		if done {
			s.detachLane(cn)
		}
	}
}

// resendFrame retransmits one pending frame on the live lane that is expected
// to drain first (same ETA score as normal pacing, excluding its current lane).
// Frames stay in the window until delivered, so duplicate copies are harmless
// (the server reassembler dedupes by seq).
func (s *cStream) resendFrame(fr *sentFrame) {
	n := int64(len(fr.payload))
	s.app.mu.Lock()
	if fr.lane == nil { // already released/acked
		s.app.mu.Unlock()
		return
	}
	old := fr.lane
	now := time.Now()
	var best *cConn
	bestETA := float64(1 << 62)
	for _, cn := range s.conns {
		if cn == old || cn.sess == nil || cn.currentSidLocked(s) == 0 {
			continue
		}
		r := s.laneRate[cn]
		backlog := int64(s.laneSent[cn]) - int64(s.laneAck[cn])
		if backlog < 0 {
			backlog = 0
		}
		// Idle lanes stay eligible (see pickLaneSendLocked): only a lane with
		// outstanding frames and no delivery progress is suspect.
		if backlog > 0 && now.Sub(s.laneT[cn]) > 400*time.Millisecond {
			r = laneRateMin
		}
		if r < laneRateMin {
			r = laneRateMin
		}
		eta := (float64(backlog) + 1) / r
		if eta < bestETA {
			best, bestETA = cn, eta
		}
	}
	if best == nil {
		s.app.mu.Unlock()
		return
	}
	s.laneBytes[old] -= n
	s.laneBytes[best] += n
	s.laneSent[best]++
	fr.lane = best
	s.app.mu.Unlock()
	if best.sess.trySendData(Frame{Typ: tData, SID: s.sid, Payload: fr.payload}) {
		s.resendCount.Add(1)
		return
	}
	s.app.mu.Lock()
	s.laneBytes[best] -= n
	s.laneBytes[old] += n
	s.laneSent[best]--
	fr.lane = old
	s.app.mu.Unlock()
}

// detachLane removes a dead spread lane from the stream (bookkeeping only; the
// server already dropped that session's stream membership).
func (s *cStream) detachLane(cn *cConn) {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if cn == s.home {
		return
	}
	for i, c := range s.conns {
		if c == cn {
			s.conns = append(s.conns[:i], s.conns[i+1:]...)
			delete(cn.members, s.id)
			s.attached--
			if s.attached < 0 {
				s.attached = 0
			}
			s.app.lanes--
			if s.app.lanes < 0 {
				s.app.lanes = 0
			}
			delete(s.laneAck, cn)
			delete(s.laneSent, cn)
			delete(s.laneBytes, cn)
			delete(s.laneRate, cn)
			delete(s.laneCnt, cn)
			delete(s.laneT, cn)
			s.app.logf("stream %d lane slot %d lost: detached (%d lanes left)", s.id, cn.idx, len(s.conns))
			return
		}
	}
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
// AUTO: min(cfg.spread, conns / live bulk streams) 闂?a lone bulk stream
// may fan across all slots, but when a speedtest floods 15+ big streams
// each stream keeps a dedicated lane (spreading would just slice every
// lane into unusable crumbs). Adds one slot per spreadThreshold bytes sent.
// detachLastDrainedLocked trims one spread lane (newest-first) that has no
// pending bytes left (everything it carried was delivered). Pure client-side
// op; the server simply stops seeing frames for this sid on that session.
func (s *cStream) detachLastDrainedLocked() bool {
	for i := len(s.conns) - 1; i >= 1; i-- {
		cn := s.conns[i]
		if s.laneBytes[cn] <= 0 {
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
			delete(s.laneAck, cn)
			delete(s.laneSent, cn)
			delete(s.laneBytes, cn)
			delete(s.laneRate, cn)
			delete(s.laneCnt, cn)
			delete(s.laneT, cn)
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
	// Collect targets under app.mu, but enqueue OUTSIDE the lock: a blocking
	// send on a congested lane used to hold app.mu (and with it every session
	// reader / ack handler) for the socket write timeout.
	s.app.mu.Lock()
	targets := make([]*session, 0, len(s.conns))
	for _, cn := range s.conns {
		if cn.sess != nil && cn.currentSidLocked(s) != 0 {
			targets = append(targets, cn.sess)
		}
	}
	s.app.mu.Unlock()
	for _, sess := range targets {
		sess.sendOrderedTimeout(Frame{Typ: tClose, SID: s.sid, Payload: payload}, 2*time.Second)
	}
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
			frames++
			payload := make([]byte, n+4)
			binary.BigEndian.PutUint32(payload[:4], seq)
			copy(payload[4:], buf[:n])
			// sendFrame applies the delivered-based window (blocks while full)
			// and keeps the frame for retransmission until the server delivered
			// it in order. Data is never dropped: a session-reconnect window
			// just delays the frame until a lane is back.
			if !s.sendFrame(seq, payload) {
				return
			}
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
			// Wait (bounded) for the tail to be DELIVERED before tearing down:
			// teardown releases pending frames, so closing immediately would
			// silently drop the end of the upload. The server also reports
			// delivery on the FIN path, so this normally settles in one RTT.
			deadline := time.Now().Add(20 * time.Second)
			waitStart := time.Now()
			var acked uint32
			for {
				s.app.mu.Lock()
				acked = s.ackedSeq
				delivered := acked >= seq
				s.app.mu.Unlock()
				if delivered || time.Now().After(deadline) {
					break
				}
				select {
				case <-s.done:
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
			if waited := time.Since(waitStart); waited > 2*time.Second && s.app.cfg.verbose {
				log.Printf("[client] pumpUp finish slow sid=%d lastSeq=%d ackedSeq=%d waited=%v",
					s.sid, seq, acked, waited.Round(time.Millisecond))
			}
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
			w0 := time.Now()
			_, werr := s.local.Write(chunk)
			if d := time.Since(w0); d > 2*time.Second && s.app.cfg.verbose {
				log.Printf("[client] pumpDown slow sid=%d len=%d took=%v", s.sid, len(chunk), d.Round(time.Millisecond))
			}
			if werr != nil {
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
