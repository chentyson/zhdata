package main

import (
	"crypto/cipher"
	"net"
	"sync"
	"time"
)

type cipherStream = cipher.Stream

const (
	pingInterval       = 30 * time.Second
	idleTimeout        = 90 * time.Second
	writeTimeout       = 15 * time.Second
	handshakeTimeout   = 10 * time.Second
	backendDialTimeout = 10 * time.Second
	backendWriteTO     = 30 * time.Second
	streamStallTO      = 3 * time.Second // one upq enqueue blocked this long = dead backend
	localWriteTO       = 30 * time.Second
	maxSlots           = 64 // client-side parallel session slots (handshake byte)
	maxStreams         = 1024
	maxReorderBytes    = 16 * 1024 * 1024 // v0.8.1: per-stream reassembly window. 8MB parked constantly on desynced lines (harsh-hour TCP shaping skews lanes -> silent park storms -> tunnel hits 0 while raw keeps ~5M); 16MB absorbs skew; bound = active bulk streams x 16MB.
	gapSkip            = 4096            // tolerate lost frames after owner-session death
	gapWaitTO          = 15 * time.Second // park a fast lane this long waiting for a slow lane's gap
	spreadThreshold    = 128 * 1024      // client: attach one more session per N bytes sent
	laneCapInit        = 8               // v0.5 baseline (restored): start small; out-based AIMD grows fast lanes.
	laneCapMin         = 8               // v0.5: AIMD shrink floor. INVARIANT: MUST be >= ackEvery. If cap < ackEvery a single-lane bulk stream can deadlock: client exhausts credit at out==cap (< ackEvery) and waits for a tAck, while the server never sends one because count-acked < ackEvery. laneCapMin >= ackEvery guarantees credit exhaustion implies the peer already has enough unacked frames to ack.
	laneCapMax         = 96              // v0.5: AIMD growth ceiling (~1.5MB)
	ackEvery           = 8               // v0.5: tAck every N frames per lane
	trainEvery         = 2 * time.Second // v0.8 (EXPERIMENT, off by default via -train 0): per-session training padding (sid=0, server discards) to keep shaper buckets warm. Did not show benefit on this link; costs uplink.
	trainPayload       = 16 * 1024       // v0.8: frame size
	trainFrames        = 1               // v0.8: frames per session per tick
)

func deadline(d time.Duration) time.Time { return time.Now().Add(d) }
func noDeadline() time.Time               { return time.Time{} }

// session is one zhdata connection (TCP + continuous AES-CTR keystream).
// Frames are written through three priority queues: ctrl > small DATA > bulk DATA.
type session struct {
	name      string
	conn      net.Conn
	tx        cipherStream // write direction (independent counter state)
	rx        cipherStream // read direction (independent counter state)
	ctrlQ     chan Frame
	hiQ       chan Frame
	loQ       chan Frame
	done      chan struct{}
	closeOnce sync.Once
	writerEnd chan struct{}
}

func newSession(name string, c net.Conn, tx, rx cipherStream) *session {
	return &session{
		name:      name,
		conn:      c,
		tx:        tx,
		rx:        rx,
		ctrlQ:     make(chan Frame, 256),
		hiQ:       make(chan Frame, 1024),
		loQ:       make(chan Frame, 1024),
		done:      make(chan struct{}),
		writerEnd: make(chan struct{}),
	}
}

// tuneTCP enlarges kernel socket buffers so a single session can keep a
// bandwidth-delay product of many MB in flight on high-RTT lossy paths
// (100Mbps x 170ms ~ 2MB BDP; auto-tuning caps lower on several platforms).
func tuneTCP(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(16 << 20)
		_ = tc.SetWriteBuffer(16 << 20)
	}
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.conn.Close()
	})
}

func (s *session) sendCtrl(f Frame) {
	select {
	case s.ctrlQ <- f:
	case <-s.done:
	}
}

// sendData enqueues a DATA frame. All DATA rides the one FIFO queue:
// choosing hiQ by frame size would let a small frame jump ahead of a bulk
// frame of the same stream when the link is slow (streams have no sequence
// numbers, so any jump = corruption). Interactivity isolation is done at
// the client level via multi-slot sessions (pickConnLocked), not by queue
// priority. Control frames (ctrlQ) still preempt DATA safely.
func (s *session) sendData(f Frame) {
	select {
	case s.loQ <- f:
	case <-s.done:
	}
}

// sendOrdered enqueues on the bulk data path (loQ) so the frame stays in
// order with previously queued DATA. CLOSE must use this: an out-of-band
// CLOSE via ctrlQ overtakes buffered bulk data and truncates downloads.
func (s *session) sendOrdered(f Frame) {
	select {
	case s.loQ <- f:
	case <-s.done:
	}
}

func (s *session) writer() {
	defer close(s.writerEnd)
	for {
		var f Frame
		// priority: ctrl > hi > lo; fall through to blocking select when idle
		select {
		case f = <-s.ctrlQ:
		default:
			select {
			case f = <-s.hiQ:
			default:
				select {
				case f = <-s.loQ:
				default:
					select {
					case f = <-s.ctrlQ:
					case f = <-s.hiQ:
					case f = <-s.loQ:
					case <-s.done:
						return
					}
				}
			}
		}
		buf := encodeFrame(&f)
		s.tx.XORKeyStream(buf, buf)
		s.conn.SetWriteDeadline(deadline(writeTimeout))
		if _, err := s.conn.Write(buf); err != nil {
			s.close()
			return
		}
	}
}

func (s *session) keepalive() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.sendCtrl(Frame{Typ: tPing})
		case <-s.done:
			return
		}
	}
}

// run reads and dispatches frames on the calling goroutine until the
// session ends. Control frames (PING/PONG/GOAWAY) are handled here.
func (s *session) run(dispatch func(*Frame)) error {
	go s.writer()
	go s.keepalive()
	var lastErr error
	for {
		s.conn.SetReadDeadline(deadline(idleTimeout))
		f, err := readFrameEnc(s.conn, s.rx)
		if err != nil {
			lastErr = err
			break
		}
		switch f.Typ {
		case tPing:
			s.sendCtrl(Frame{Typ: tPong})
		case tPong:
			// activity only
		case tGoaway:
			lastErr = errGoaway
			s.close()
			break
		default:
			dispatch(f)
		}
		if lastErr != nil {
			break
		}
	}
	s.close()
	<-s.writerEnd
	return lastErr
}
