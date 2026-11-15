package main

// stress_repro_test.go — deterministic regression tests for the upload collapse
// that used to happen on high-RTT (~170-240ms), lossy/reordering lines.
//
// History: one app stream was spread over many sessions (lanes); one stalled
// lane left a hole in the server reassembler (rx.next small, rx.bufN growing),
// and when rx.bufN crossed maxReorderBytes the old code PARKED the session
// reader — freezing every stream sharing that session (cross-stream HOL) until
// gapWaitTO=15s, then broadcasting tReset. v0.10 removed the park: an overflow
// now resets only the affected stream and the reader keeps dispatching.
//
// These tests pin the new contract:
//  1. reorder overflow -> dispatch returns immediately, stream is reset;
//  2. a stream overflow does NOT block other streams on the same session;
//  3. client per-lane credit bookkeeping still works (static window).
//
// Whitebox on purpose: builds sStream/reassembler state and drives
// serverApp.dispatch() directly (plus one real session read loop), so the tests
// are deterministic and finish in well under a second. No WAN traffic.

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// stressNewServerApp builds the minimum serverApp needed to drive dispatch():
// no listeners, no key store.
func stressNewServerApp() *serverApp {
	return &serverApp{
		cfg:     serverConfig{},
		conns:   map[string]*sSession{},
		streams: map[string]*sStream{},
		fails:   map[string]*failWin{},
		dns:     map[string]dnsEntry{},
		rst:     map[string]int64{},
		stopCh:  make(chan struct{}),
	}
}

// stressIdleSession is a session with no reader/writer goroutines: enough for
// sendCtrl() traffic (ctrlQ is buffered) and for tests to close/release it.
func stressIdleSession(t *testing.T, name string) *session {
	t.Helper()
	c1, c2 := net.Pipe()
	s := newSession(name, c1, nopStream{}, nopStream{})
	t.Cleanup(func() {
		s.close()
		c2.Close()
	})
	return s
}

// stressShim wraps an idle session in the server-side per-connection state.
func stressShim(t *testing.T, uuid, name string) *sSession {
	t.Helper()
	return &sSession{
		uuid:    uuid,
		sess:    stressIdleSession(t, name),
		streams: map[uint16]*sStream{},
	}
}

// stressStream constructs the sStream that a tOpen would create and registers
// it exactly like dispatch() does.
func stressStream(a *serverApp, owner *sSession, sid uint16) *sStream {
	st := &sStream{
		owner:    owner,
		uuid:     owner.uuid,
		sid:      sid,
		upq:      make(chan []byte, 128),
		dialDone: make(chan struct{}),
		rx:       reassembler{next: 1, buf: map[uint32][]byte{}},
		rxWake:   make(chan struct{}, maxSlots),
		laneHi:   map[*sSession]*laneAckState{owner: {}},
		members:  map[*sSession]bool{owner: true},
	}
	owner.smu.Lock()
	owner.streams[sid] = st
	owner.smu.Unlock()
	a.stmMu.Lock()
	a.streams[streamKey(owner.uuid, sid)] = st
	a.stmMu.Unlock()
	return st
}

// stressAttach mirrors the in-stream spread branch of tOpen: an additional
// session becomes a member/lane of an already-open stream.
func stressAttach(a *serverApp, st *sStream, sh *sSession) {
	a.stmMu.Lock()
	st.members[sh] = true
	a.stmMu.Unlock()
	st.mu.Lock()
	st.laneHi[sh] = &laneAckState{}
	st.mu.Unlock()
	sh.smu.Lock()
	sh.streams[st.sid] = st
	sh.smu.Unlock()
}

// stressDataPayload builds a client DATA frame payload: 4B stream-global
// sequence prefix + data bytes.
func stressDataPayload(seq uint32, dataLen int) []byte {
	p := make([]byte, 4+dataLen)
	binary.BigEndian.PutUint32(p[:4], seq)
	return p
}

// fillReorderWindow lands seq 2..windowFrames+1 (16KiB each) on the fast lane
// while seq 1 stays lost on the stalled lane, so rx.next stays 1 and rx.bufN
// reaches exactly maxReorderBytes.
func fillReorderWindow(a *serverApp, st *sStream, fast *sSession) (windowFrames int) {
	chunk := maxPayload
	windowFrames = maxReorderBytes / chunk
	for seq := uint32(2); seq <= uint32(windowFrames+1); seq++ {
		a.dispatch(fast, &Frame{Typ: tData, SID: st.sid, Payload: stressDataPayload(seq, chunk)})
	}
	return windowFrames
}

// TestStressReorderOverflowResetsOnlyTheStream proves that crossing the server
// reorder window no longer blocks the session reader: dispatch returns at once
// and closes just that stream (the app retries; other streams keep flowing).
func TestStressReorderOverflowResetsOnlyTheStream(t *testing.T) {
	const uuid = "stress-overflow-uuid"
	const sid = 7

	a := stressNewServerApp()
	owner := stressShim(t, uuid, "stream-owner")
	stalled := stressShim(t, uuid, "lane-1-stalled")
	fast := stressShim(t, uuid, "lane-2-fast")

	st := stressStream(a, owner, sid)
	stressAttach(a, st, stalled) // seq 1 never arrives on this lane
	stressAttach(a, st, fast)

	windowFrames := fillReorderWindow(a, st, fast)
	st.mu.Lock()
	bufN, next := st.rx.bufN, st.rx.next
	st.mu.Unlock()
	if bufN != maxReorderBytes || next != 1 {
		t.Fatalf("precondition: bufN=%d next=%d, want %d/1", bufN, next, maxReorderBytes)
	}

	// One more 16KiB frame crosses the window: dispatch must return quickly
	// (no reader park) and reset the stream.
	done := make(chan struct{})
	go func() {
		a.dispatch(fast, &Frame{Typ: tData, SID: sid, Payload: stressDataPayload(uint32(windowFrames+2), maxPayload)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch blocked on reorder overflow: the session reader is parked again")
	}

	a.stmMu.Lock()
	_, stillThere := a.streams[streamKey(uuid, sid)]
	a.stmMu.Unlock()
	if stillThere {
		t.Fatal("stream still registered after reorder overflow; expected reset")
	}
	st.mu.Lock()
	closed := st.closed
	st.mu.Unlock()
	if !closed {
		t.Fatal("stream not marked closed after reorder overflow")
	}
	if frames := windowFrames + 1; frames <= laneCapMax {
		t.Fatalf("model invalid: %d buffered frames <= laneCapMax=%d", frames, laneCapMax)
	}
}

// TestStressOverflowDoesNotBlockOtherStreams proves the cross-stream HOL
// cascade is gone: with a REAL session read loop, stream A overflows the window
// and is reset, and stream B's frame queued behind A on the same wire is still
// read and dispatched immediately (the old code froze the reader for 15s).
func TestStressOverflowDoesNotBlockOtherStreams(t *testing.T) {
	const uuid = "stress-cascade-uuid"
	const sidA, sidB = 0xA1, 0xB2

	a := stressNewServerApp()
	owner := stressShim(t, uuid, "A-owner")

	srvConn, cliConn := net.Pipe()
	salt := randBytes(16)
	srvSess := newSession("srv-shared", srvConn, newCTR(uuid, salt), newCTR(uuid, salt))
	cliSess := newSession("cli-shared", cliConn, newCTR(uuid, salt), newCTR(uuid, salt))
	t.Cleanup(func() {
		srvSess.close()
		cliSess.close()
	})
	shared := &sSession{uuid: uuid, sess: srvSess, streams: map[uint16]*sStream{}}

	// Stream A spreads over owner + shared; seq 1 is lost (stalled lane).
	stA := stressStream(a, owner, sidA)
	stressAttach(a, stA, shared)

	// Stream B shares the same session, queued after A's flood.
	stB := stressStream(a, shared, sidB)

	go srvSess.run(func(f *Frame) { a.dispatch(shared, f) })
	go cliSess.run(func(*Frame) {})

	windowFrames := maxReorderBytes / maxPayload
	for seq := uint32(2); seq <= uint32(windowFrames+2); seq++ {
		cliSess.sendData(Frame{Typ: tData, SID: sidA, Payload: stressDataPayload(seq, maxPayload)})
	}
	cliSess.sendData(Frame{Typ: tData, SID: sidB, Payload: stressDataPayload(1, 8)})

	// A must be reset (overflow) rather than parked.
	waitFor(t, func() bool {
		stA.mu.Lock()
		closed := stA.closed
		stA.mu.Unlock()
		return closed
	}, "stream A to be reset on reorder overflow")

	// B must be dispatched right after A's reset frame, never blocked.
	waitFor(t, func() bool {
		stB.mu.Lock()
		defer stB.mu.Unlock()
		return stB.rx.bufN > 0
	}, "stream B to be dispatched after A's overflow reset")
}

// stressNewClientApp builds a clientApp whose N slots are already "live" for
// whitebox credit tests.
func stressNewClientApp(t *testing.T, lanes int) *clientApp {
	t.Helper()
	a := &clientApp{
		cfg:     clientConfig{conns: lanes, spread: lanes},
		streams: map[uint32]*cStream{},
		stopCh:  make(chan struct{}),
	}
	for i := 0; i < lanes; i++ {
		a.conns = append(a.conns, &cConn{
			app:     a,
			idx:     byte(i),
			sess:    stressIdleSession(t, fmt.Sprintf("cli-lane-%d", i)),
			sessMap: map[uint16]*cStream{},
			members: map[uint32]*cStream{},
		})
	}
	return a
}

// TestStressClientLaneCreditIsStaticWindow pins the v0.10 credit contract: the
// per-lane window is a large constant (laneCapInit/Min/Max equal), because the
// old out-based AIMD self-limited to ~24 frames (384KiB), clocking one lane to
// ~2.5Mbps on a 200ms path and forcing the (now disabled) 16-lane spread.
func TestStressClientLaneCreditIsStaticWindow(t *testing.T) {
	if laneCapInit != laneCapMin || laneCapMin != laneCapMax {
		t.Fatalf("credit window is not static: init=%d min=%d max=%d",
			laneCapInit, laneCapMin, laneCapMax)
	}
	if laneCapMin < ackEvery {
		t.Fatalf("invariant broken: laneCapMin=%d < ackEvery=%d", laneCapMin, ackEvery)
	}

	const lanes = 2
	a := stressNewClientApp(t, lanes)
	const sid = 1
	s := &cStream{
		app:      a,
		id:       1,
		sid:      sid,
		home:     a.conns[0],
		conns:    append([]*cConn(nil), a.conns...),
		laneSent: map[*cConn]uint32{},
		laneAck:  map[*cConn]uint32{},
		laneCap:  map[*cConn]int{},
	}
	s.attached = lanes - 1
	for _, cn := range a.conns {
		s.ensureLaneLocked(cn)
		cn.members[s.id] = s
		cn.sessMap[sid] = s
	}

	// A drained fast lane keeps the full window: applyAck must not shrink it.
	for _, cn := range a.conns {
		for i := uint32(1); i <= 4; i++ {
			s.laneSent[cn] = i
			s.laneAck[cn] = i - 1
			s.applyAck(cn, i, 0)
		}
		if got := s.laneCap[cn]; got != laneCapMax {
			t.Fatalf("lane %d credit = %d, want %d", cn.idx, got, laneCapMax)
		}
	}

	// With a full window a lane still consumes exactly laneCapMax frames.
	frames := 0
	for {
		cn, live := s.pickLane()
		if cn == nil {
			if !live {
				t.Fatal("all lanes lost liveness")
			}
			break
		}
		frames++
		if frames > lanes*laneCapMax*2 {
			t.Fatalf("runaway credit: %d frames", frames)
		}
	}
	if want := lanes * laneCapMax; frames != want {
		t.Fatalf("frames handed out = %d, want %d", frames, want)
	}
}

// nopConn is a net.Conn whose methods are no-ops, for whitebox stream tests
// that only need teardown() to be able to Close() the local socket.
type nopConn struct{}

func (nopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (nopConn) Write(p []byte) (int, error)      { return len(p), nil }
func (nopConn) Close() error                     { return nil }
func (nopConn) LocalAddr() net.Addr              { return nil }
func (nopConn) RemoteAddr() net.Addr             { return nil }
func (nopConn) SetDeadline(time.Time) error      { return nil }
func (nopConn) SetReadDeadline(time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(time.Time) error { return nil }

// TestStressDownlinkQueueFullFailsStream pins the v0.10.1 downlink contract: a
// stalled app reader must not block the session's frame reader indefinitely.
// The old code blocked forever on a full rq, which froze every stream on the
// slot and (after the peer's 15s write timeout) killed the whole TCP session —
// in the field that produced hung downloads. Now dispatch waits at most ~2s,
// then fails only that stream.
func TestStressDownlinkQueueFullFailsStream(t *testing.T) {
	a := stressNewClientApp(t, 1)
	cn := a.conns[0]

	s := &cStream{
		app:      a,
		id:       42,
		sid:      4242,
		home:     cn,
		conns:    []*cConn{cn},
		local:    nopConn{},
		rq:       make(chan []byte, 2),
		done:     make(chan struct{}),
		laneSent: map[*cConn]uint32{},
		laneAck:  map[*cConn]uint32{},
		laneCap:  map[*cConn]int{},
	}
	a.mu.Lock()
	a.streams[s.id] = s
	cn.members[s.id] = s
	cn.sessMap[s.sid] = s
	cn.gen = 1
	a.mu.Unlock()

	// Fill the downlink queue so the next DATA frame has nowhere to go.
	s.rq <- []byte("x")
	s.rq <- []byte("y")

	start := time.Now()
	done := make(chan struct{})
	go func() {
		cn.dispatch(1, &Frame{Typ: tData, SID: s.sid, Payload: []byte("z")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch blocked on a full downlink queue: session reader can stall again")
	}
	if waited := time.Since(start); waited > 4*time.Second {
		t.Fatalf("dispatch took %v, want bounded (~2s)", waited)
	}
	select {
	case <-s.done:
	default:
		t.Fatal("stream was not failed after downlink queue stayed full")
	}
	a.mu.Lock()
	_, still := a.streams[s.id]
	a.mu.Unlock()
	if still {
		t.Fatal("stream still registered after downlink queue overflow")
	}
}
