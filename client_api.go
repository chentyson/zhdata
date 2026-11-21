package zhdata

// Embedded outbound client (v0.15.2): the API the s-ui panel uses to route a
// single application connection through the zhdata tunnel without running a
// local SOCKS/HTTP proxy. It reuses the whole client stack (multi-session
// slots, in-stream spread, per-frame retransmission, tAck credit windows,
// zhv5 bucketing) — only the local end of the stream is an in-memory conn
// instead of a proxy socket.
//
//	Client, _ := zhdata.NewClient(&zhdata.ClientConfig{Servers: []string{...}, UUID: "..."})
//	conn, err := Client.DialContext(ctx, "example.com", 443)
//	// conn is a net.Conn; it also implements CloseWrite() error (half-close,
//	// carried to the server as tClose with finSeq).
//
// Because streams ride the long-lived tunnel sessions, a DialContext during a
// reconnect simply waits for the slot to come back (bounded by ctx).

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ClientConfig configures an embedded zhdata outbound.
type ClientConfig struct {
	Servers  []string             // zhdata server host:port list (multi-port = slots)
	UUID     string               // user uuid (keysource side must know it)
	Conns    int                  // parallel tunnel sessions; <=0 -> 16
	Spread   int                  // max lanes one bulk stream may fan across; <=0 -> 8
	DLPool   int                  // split pools: last N sessions are download-only homes (0 = shared)
	SlotBase int                  // first slot index (run several outbounds without takeover conflicts)
	Train    int                  // seconds between idle training frames; 0 = off
	DeviceID []byte               // 16-byte device id; empty -> random per process
	LogLevel string               // none|error|warning|info|debug ("" = keep current)
	Logf     func(string, ...any) // optional sink for core log lines (printf-style)
}

// Client is an embedded zhdata outbound: one set of tunnel sessions shared by
// every DialContext connection.
type Client struct {
	app *clientApp
}

// ErrClientClosed is returned by DialContext after Close.
var ErrClientClosed = errors.New("zhdata: client closed")

// NewClient starts the tunnel sessions (they reconnect on their own forever)
// and returns the outbound handle.
func NewClient(cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("zhdata: ClientConfig required")
	}
	if strings.TrimSpace(cfg.UUID) == "" {
		return nil, errors.New("zhdata: ClientConfig.UUID required")
	}
	servers := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		if s = strings.TrimSpace(s); s != "" {
			servers = append(servers, s)
		}
	}
	if len(servers) == 0 {
		return nil, errors.New("zhdata: ClientConfig.Servers required")
	}
	if lvl := strings.TrimSpace(cfg.LogLevel); lvl != "" {
		n, ok := parseLogLevel(lvl)
		if !ok {
			return nil, errors.New("zhdata: unknown ClientConfig.LogLevel " + lvl)
		}
		SetLogLevel(n)
	}
	if cfg.Logf != nil {
		fn := cfg.Logf
		SetLogSink(func(level int, tag, msg string) {
			fn("zhdata[%s/%s] %s", tag, lvlName[level], msg)
		})
	}
	conns, spread := cfg.Conns, cfg.Spread
	if conns <= 0 {
		conns = 16 // embedded default: enough slots for spread + takeover headroom
	}
	if spread <= 0 {
		spread = 8
	}
	o := &ClientOptions{
		Listen:   "", // no local proxy listener: streams come from DialContext
		Server:   strings.Join(servers, ","),
		UUID:     cfg.UUID,
		Conns:    conns,
		Spread:   spread,
		DLConns:  cfg.DLPool,
		SlotBase: cfg.SlotBase,
		Train:    cfg.Train,
	}
	if len(cfg.DeviceID) > 0 {
		if len(cfg.DeviceID) != deviceIDLen {
			return nil, errors.New("zhdata: ClientConfig.DeviceID must be 16 bytes")
		}
		o.DeviceID = hex.EncodeToString(cfg.DeviceID)
	}
	app, err := NewClientApp(o)
	if err != nil {
		return nil, err
	}
	return &Client{app: app}, nil
}

// Close tears down the tunnel sessions and every DialContext connection on them.
func (c *Client) Close() {
	if c != nil && c.app != nil {
		c.app.Close()
	}
}

// DeviceID reports the 16-byte device id this outbound authenticates with.
func (c *Client) DeviceID() []byte {
	if c == nil || c.app == nil {
		return nil
	}
	id := c.app.deviceID
	return id[:]
}

// DialContext opens one application connection through the tunnel. The target
// is resolved and dialed by the zhdata *server* (the tunnel terminations), so
// `host` may be a domain name. The returned conn is in-memory: it carries the
// app stream over the tunnel and implements CloseWrite() error.
func (c *Client) DialContext(ctx context.Context, host string, port int) (net.Conn, error) {
	if c == nil || c.app == nil {
		return nil, ErrClientClosed
	}
	a := c.app
	if a.stopped.Load() {
		return nil, ErrClientClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if port < 1 || port > 65535 {
		return nil, errors.New("zhdata: bad port " + strconv.Itoa(port))
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, errors.New("zhdata: empty host")
	}
	atyp, addr := targetAtyp(host)

	mc := newMemConn()
	s := &cStream{
		app:    a,
		id:     atomic.AddUint32(&a.streamID, 1),
		local:  &pumpSide{m: mc},
		atyp:   atyp,
		addr:   addr,
		port:   uint16(port),
		rq:     make(chan []byte, 64),
		done:   make(chan struct{}),
		noPump: true, // no local proxy protocol to sniff: bytes go straight to lanes
	}
	mc.stream = s

	a.mu.Lock()
	a.streams[s.id] = s
	s.sid = a.allocSidLocked()
	home := a.pickConnLocked()
	s.home = home
	s.conns = []*cConn{home}
	home.members[s.id] = s
	s.ensureLaneLocked(home)
	if s.sid != 0 && home.sess != nil {
		home.openStreamLocked(s)
	}
	a.mu.Unlock()

	if s.sid == 0 { // sid space exhausted (pathological: 65k live streams)
		s.teardown()
		return nil, errors.New("zhdata: stream id space exhausted")
	}

	go s.pumpUp()
	go s.pumpDown()

	// Wait until the server confirms the target is connected (tOpenAck). A
	// tReset (target refused / stream reclaimed), a dead session, or ctx
	// cancellation ends the dial with an error.
	for {
		a.mu.Lock()
		acked := s.openAcked
		a.mu.Unlock()
		if acked {
			return &appConn{m: mc}, nil
		}
		select {
		case <-s.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a.mu.Lock()
			reset := s.reset
			a.mu.Unlock()
			if reset {
				return nil, errors.New("zhdata: server could not connect to " + net.JoinHostPort(host, strconv.Itoa(port)))
			}
			return nil, errors.New("zhdata: tunnel closed while connecting to " + net.JoinHostPort(host, strconv.Itoa(port)))
		case <-ctx.Done():
			s.teardown()
			return nil, ctx.Err()
		case <-a.stopCh:
			return nil, ErrClientClosed
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// targetAtyp maps a host to the tunnel address type (the server resolves
// domains, so no client-side DNS is needed).
func targetAtyp(host string) (byte, []byte) {
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return atypIPv4, v4
		}
		return atypIPv6, ip.To16()
	}
	return atypDomain, []byte(host)
}

// ---------------- in-memory conn (the local end of an embedded stream) ----------------

// memConn is the in-memory full-duplex pair behind an embedded stream, built
// from two unbuffered io.Pipes (same backpressure shape as a local TCP socket:
// an application write blocks until the stream's upload pump takes the bytes).
//
// Two opposite views sit on it, exactly like the two ends of a socket:
//
//	pumpSide (cStream.local): Read drains what the app wrote, Write feeds
//	                          what the app will read
//	appConn  (what DialContext returns): Read gets the tunnel downlink,
//	                          Write sends the tunnel uplink
//
// CloseWrite on appConn closes the app's send side: pumpUp then sees EOF and
// sends tClose with finSeq (the half-close the server forwards as CloseWrite
// to the target), while the downlink keeps flowing. CloseWrite on pumpSide is
// the mirror (the server sent tClose): the application's reader sees EOF while
// its own write side stays open.
type memConn struct {
	toAppR   *io.PipeReader // app Read
	toAppW   *io.PipeWriter // pumpDown -> app
	fromAppR *io.PipeReader // pumpUp <- app
	fromAppW *io.PipeWriter // app Write

	closeOnce sync.Once
	stream    *cStream // set by DialContext: Close() also tears the stream down
}

func newMemConn() *memConn {
	toAppR, toAppW := io.Pipe()
	fromAppR, fromAppW := io.Pipe()
	return &memConn{toAppR: toAppR, toAppW: toAppW, fromAppR: fromAppR, fromAppW: fromAppW}
}

// closeAll ends both directions. It deliberately does NOT tear the stream
// down: cStream.teardown closes this conn, and sync.Once is not reentrant, so
// calling back into teardown from here would deadlock (the app-side Close
// calls teardown itself, after closeAll).
func (m *memConn) closeAll() {
	m.closeOnce.Do(func() {
		m.fromAppW.Close() // pumpUp read side -> io.EOF -> tClose(finSeq)
		m.fromAppR.CloseWithError(net.ErrClosed)
		m.toAppW.CloseWithError(net.ErrClosed)
		m.toAppR.CloseWithError(net.ErrClosed)
	})
}

// appConn is the handle the embedder gets from DialContext.
type appConn struct{ m *memConn }

func (a *appConn) Read(p []byte) (int, error)  { return a.m.toAppR.Read(p) }
func (a *appConn) Write(p []byte) (int, error) { return a.m.fromAppW.Write(p) }

// CloseWrite half-closes app->tunnel (tClose with finSeq on the wire).
func (a *appConn) CloseWrite() error {
	a.m.fromAppW.Close()
	return nil
}

// Close ends the stream: half-close the uplink, fail the downlink, and
// release the stream (which closes this conn's pump side too).
func (a *appConn) Close() error {
	a.m.closeAll()
	if a.m.stream != nil {
		a.m.stream.teardown()
	}
	return nil
}

// Deadlines: the pumps set deadlines on proxy sockets; an in-memory conn
// blocks in Read/Write (io.Pipe) instead, which is the same observable
// behaviour for both sides (block until data / block until drained).
func (a *appConn) SetDeadline(time.Time) error      { return nil }
func (a *appConn) SetReadDeadline(time.Time) error  { return nil }
func (a *appConn) SetWriteDeadline(time.Time) error { return nil }
func (a *appConn) LocalAddr() net.Addr              { return tunnelAddr{} }
func (a *appConn) RemoteAddr() net.Addr             { return tunnelAddr{} }

// pumpSide is the local end the stream pumps use (cStream.local).
type pumpSide struct{ m *memConn }

func (ps *pumpSide) Read(buf []byte) (int, error)  { return ps.m.fromAppR.Read(buf) }
func (ps *pumpSide) Write(buf []byte) (int, error) { return ps.m.toAppW.Write(buf) }

// CloseWrite ends the delivery direction (the server's tClose): the app's
// reader sees io.EOF, its own send side stays open.
func (ps *pumpSide) CloseWrite() error {
	ps.m.toAppW.Close()
	return nil
}

func (ps *pumpSide) Close() error { ps.m.closeAll(); return nil }

func (ps *pumpSide) SetDeadline(time.Time) error      { return nil }
func (ps *pumpSide) SetReadDeadline(time.Time) error  { return nil }
func (ps *pumpSide) SetWriteDeadline(time.Time) error { return nil }
func (ps *pumpSide) LocalAddr() net.Addr              { return tunnelAddr{} }
func (ps *pumpSide) RemoteAddr() net.Addr             { return tunnelAddr{} }

// tunnelAddr is the placeholder address pair of an in-memory stream.
type tunnelAddr struct{}

func (tunnelAddr) Network() string { return "zhdata" }
func (tunnelAddr) String() string  { return "zhdata:memory" }

var (
	_ net.Conn = (*appConn)(nil)
	_ net.Conn = (*pumpSide)(nil)
)
