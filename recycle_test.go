package zhdata

import (
	"bytes"
	"errors"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

// Regression tests for the idle-session recycle log noise (v0.15.5).
//
// Symptom (zhenhanvn, idle client, loglevel=debug, one line every ~5s forever):
//
//	[client] [debug] slot 12 session ended: read tcp 192.168.3.153:51082->
//	    154.12.53.217:49900: use of closed network connection (retry in 1s)
//
// The client closed that session itself (sessionRecycleLoop, anti-shaper pool
// refresh), but Go reports a self-close as a bare closed-socket error, so the
// planned refresh was logged as a session death.

// recycleTestSession builds a session over an arbitrary conn with an identity
// cipher (no traffic is expected to flow through it in these tests).
func recycleTestSession(name string, c net.Conn) *session {
	return newSession(name, c, nopStream{}, nopStream{})
}

// TestSessionRunReportsPlannedCloseCause: a close we initiated must surface its
// reason to the reader instead of net.ErrClosed / io.ErrClosedPipe noise.
func TestSessionRunReportsPlannedCloseCause(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })

	s := recycleTestSession("t-cause", c1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.closeWithCause(errIdleRecycle)
	}()

	err := s.run(func(*Frame) {})
	if !errors.Is(err, errIdleRecycle) {
		t.Fatalf("run() returned %v, want errIdleRecycle", err)
	}
}

// failingWriteConn is a conn whose Write always fails (peer not reading, socket
// gone): the writer's own error must become the session's reported cause.
type failingWriteConn struct {
	net.Conn
}

func (c *failingWriteConn) Write(p []byte) (int, error) {
	return 0, errors.New("socket not writable")
}

func TestSessionRunReportsWriteErrorCause(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })

	wf := &failingWriteConn{Conn: c1}
	s := recycleTestSession("t-werr", wf)
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.sendCtrl(Frame{Typ: tPing})
	}()

	oldOut, oldFlags := log.Writer(), log.Flags()
	t.Cleanup(func() { log.SetOutput(oldOut); log.SetFlags(oldFlags) })
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)

	err := s.run(func(*Frame) {})
	if err == nil || !strings.Contains(err.Error(), "local write err") ||
		!strings.Contains(err.Error(), "socket not writable") {
		t.Fatalf("run() returned %v, want the local write error as the cause", err)
	}
}

// TestPlannedRecycleRedialsQuietly runs a real server+client pair and forces the
// idle sweep: the pool must still refresh (that is what keeps ISP buckets warm),
// but nothing may be logged that reads like a failure.
func TestPlannedRecycleRedialsQuietly(t *testing.T) {
	const uuid = "aaaaaaaa-1111-2222-3333-444444444444"

	oldLevel := logLevel
	oldOut, oldFlags := log.Writer(), log.Flags()
	t.Cleanup(func() {
		logLevel = oldLevel
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	logLevel = lvlDebug // the noise only existed at debug; assert at debug

	srv, err := newServerApp(serverConfig{
		listen:    "127.0.0.1:0",
		usersPath: usersFileWith(t, uuid),
		backend:   "direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli, err := newClientApp(clientConfig{
		listen: "127.0.0.1:0",
		server: srv.Addr(),
		uuid:   uuid,
		conns:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	waitFor(t, func() bool { return srv.estab.Load() >= 2 }, "two sessions established")

	gens := func() int {
		cli.mu.Lock()
		defer cli.mu.Unlock()
		n := 0
		for _, cn := range cli.conns {
			n += int(cn.gen)
		}
		return n
	}
	base := gens()

	// Pretend no user data has moved for a minute: the next sweep must re-dial a
	// session, silently.
	cli.mu.Lock()
	for _, cn := range cli.conns {
		cn.lastAct.Store(time.Now().Add(-time.Minute).UnixNano())
	}
	cli.mu.Unlock()

	waitFor(t, func() bool { return gens() > base }, "idle recycle re-dialed a session")

	out := buf.String()

	// The server legitimately sees our planned hangup as a plain EOF and logs
	// that at debug; what must be gone is the CLIENT-side incident line.
	var cliLines []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "[client]") {
			cliLines = append(cliLines, line)
		}
	}
	for _, bad := range []string{"session ended", "use of closed network connection"} {
		for _, line := range cliLines {
			if strings.Contains(line, bad) {
				t.Fatalf("planned recycle logged %q on the client:\n%s", bad, out)
			}
		}
	}
	redialed := false
	for _, line := range cliLines {
		if strings.Contains(line, "[debug] session established") {
			redialed = true
		}
	}
	if !redialed {
		t.Fatalf("the planned re-dial left no debug trace (was it logged as info news?):\n%s", out)
	}
}

// TestPickRecycleVictimPrefersStreamlessSlot pins the victim policy: refresh a
// slot that owns no app stream, and only touch a loaded slot once it is far past
// its own idle threshold.
func TestPickRecycleVictimPrefersStreamlessSlot(t *testing.T) {
	a := &clientApp{}
	idleTO := func(*cConn) time.Duration { return 25 * time.Second }

	mk := func(idx byte, idleFor time.Duration, homed int) *cConn {
		c1, c2 := net.Pipe()
		t.Cleanup(func() { c1.Close(); c2.Close() })
		cn := &cConn{
			app:     a,
			idx:     idx,
			sess:    recycleTestSession("t-victim", c1),
			sessMap: map[uint16]*cStream{},
			members: map[uint32]*cStream{},
		}
		cn.lastAct.Store(time.Now().Add(-idleFor).UnixNano())
		for i := 0; i < homed; i++ {
			sid := uint32(len(cn.members) + 1)
			cn.members[sid] = &cStream{app: a, id: sid, home: cn}
		}
		return cn
	}

	now := time.Now()

	// Slot 0 owns streams and is MORE idle; slot 1 owns none and must be picked.
	streamless := mk(1, 30*time.Second, 0)
	a.conns = []*cConn{mk(0, 60*time.Second, 2), streamless}
	v, vsess, _, homed, idle := a.pickRecycleVictimLocked(now, idleTO)
	if v != streamless || vsess != streamless.sess || homed != 0 || idle != 2 {
		t.Fatalf("idle pool: picked slot %v (homed=%d, idle=%d), want streamless slot 1", v.idx, homed, idle)
	}

	// Every idle slot owns live streams but none is past the loaded hard cap
	// (2x its own threshold): no refresh yet, because re-dialing those sessions
	// RSTs quiet app connections (v0.11.9: 246 stream losses in 20 minutes).
	a.conns = []*cConn{mk(0, 40*time.Second, 1), mk(1, 30*time.Second, 2)}
	if v, _, _, _, idle := a.pickRecycleVictimLocked(now, idleTO); v != nil || idle != 2 {
		t.Fatalf("loaded pool below hard cap: picked slot %v (idle=%d), want no victim", v.idx, idle)
	}

	// A streamless idle slot still wins over a loaded one past the cap.
	a.conns = []*cConn{mk(0, 40*time.Second, 1), mk(1, 30*time.Second, 0)}
	v, _, _, homed, idle = a.pickRecycleVictimLocked(now, idleTO)
	if v.idx != 1 || homed != 0 || idle != 2 {
		t.Fatalf("mixed pool: picked slot %v (homed=%d, idle=%d), want streamless slot 1", v.idx, homed, idle)
	}

	// Past the cap (2x the slot's own threshold) the most idle loaded slot is
	// refreshed anyway — v0.12.4 showed that never refreshing loaded sessions
	// costs 3x on the next download (stale ISP bucket).
	aged := mk(0, 60*time.Second, 1)
	a.conns = []*cConn{aged, mk(1, 55*time.Second, 2)}
	v, _, _, homed, idle = a.pickRecycleVictimLocked(now, idleTO)
	if v != aged || homed != 1 || idle != 2 {
		t.Fatalf("aged pool: picked slot %v (homed=%d, idle=%d), want most idle loaded slot 0", v.idx, homed, idle)
	}

	// Nothing idle enough: no victim, and a slot with a dead session is skipped.
	a.conns = []*cConn{mk(0, 5*time.Second, 0)}
	if v, _, _, _, idle := a.pickRecycleVictimLocked(now, idleTO); v != nil || idle != 0 {
		t.Fatalf("fresh pool: picked slot %v (idle=%d), want no victim", v.idx, idle)
	}
	dead := mk(1, 60*time.Second, 0)
	dead.sess = nil
	a.conns = []*cConn{dead}
	if v, _, _, _, idle := a.pickRecycleVictimLocked(now, idleTO); v != nil || idle != 0 {
		t.Fatalf("dead session: picked slot %v (idle=%d), want it skipped", v.idx, idle)
	}
}
