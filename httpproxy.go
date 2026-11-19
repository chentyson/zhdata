package main

// Mixed local listener (xray "mixed" equivalent): ONE local port serves both
// SOCKS5 and an HTTP proxy, dispatched on the first byte of the connection
// (0x05 = SOCKS5 greeting, ASCII A-Z = HTTP method token). Both paths return
// the target as (atyp, addr, port) in the exact shape socksAccept produces,
// so the existing stream-open path (encodeAddrPort/tOpen) is reused as-is.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// httpHeadMax caps the buffered request line + headers of the local HTTP
// proxy path (the read deadline set by acceptLoop bounds the time).
const httpHeadMax = 16 * 1024

// httpMethods are the methods accepted on the local HTTP proxy path.
var httpMethods = map[string]bool{
	"GET": true, "POST": true, "PUT": true, "PATCH": true, "HEAD": true,
	"OPTIONS": true, "DELETE": true, "TRACE": true, "CONNECT": true,
}

// bufferedConn keeps the bytes a bufio.Reader already consumed from the
// connection (the sniffed first byte, header overshoot) ahead of later reads,
// while writes, deadlines and close still act on the underlying connection.
type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func (w bufferedConn) Read(p []byte) (int, error) { return w.br.Read(p) }

func (w bufferedConn) CloseWrite() error { return closeWriteConn(w.Conn) }

// replayConn re-feeds buffered bytes — the rewritten request head plus any
// bytes read past the headers during parsing — before passing reads to the
// underlying connection, so pumpUp never sees a lost/corrupted prefix and no
// pump logic has to know about the rewrite.
type replayConn struct {
	net.Conn
	buf []byte
}

func (w *replayConn) Read(p []byte) (int, error) {
	if len(w.buf) > 0 {
		n := copy(p, w.buf)
		w.buf = w.buf[n:]
		return n, nil
	}
	return w.Conn.Read(p)
}

func (w *replayConn) CloseWrite() error { return closeWriteConn(w.Conn) }

// closeWriteConn half-closes the write side when the conn supports it; the
// stream wrappers delegate here so wrapping never loses CloseWrite (the
// half-close that signals tClose to the server).
func closeWriteConn(c net.Conn) error {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Close()
}

// localAccept sniffs the first byte and runs the matching local proxy
// handshake. It returns the connection to use as the stream's local side —
// possibly a wrapper that re-feeds bytes the parser consumed — plus the
// target (atyp, addr, port) exactly as socksAccept returns them.
// 0x05 = SOCKS5 greeting; an ASCII uppercase letter = an HTTP method token
// (GET/POST/PUT/PATCH/HEAD/OPTIONS/DELETE/TRACE/CONNECT all start A-Z);
// anything else is closed like before. This is the default (Mixed) behavior.
func localAccept(br *bufio.Reader, c net.Conn) (local net.Conn, atyp byte, addr []byte, port uint16, err error) {
	b, err := br.Peek(1)
	if err != nil {
		return nil, 0, nil, 0, err
	}
	if b[0] == 5 {
		bc := bufferedConn{Conn: c, br: br}
		atyp, addr, port, err = socksAccept(bc)
		if err != nil {
			return nil, 0, nil, 0, err
		}
		return bc, atyp, addr, port, nil
	}
	if b[0] >= 'A' && b[0] <= 'Z' {
		return httpAccept(br, c)
	}
	return nil, 0, nil, 0, fmt.Errorf("unknown local protocol first byte 0x%02x", b[0])
}

// httpAccept parses one local HTTP proxy request from br (bound to c).
// CONNECT replies "200 Connection established" and hands back the raw (still
// buffered) connection for opaque two-way relay. Plain methods require an
// absolute-URI request line (http://host[:port]/path, default port 80;
// https:// -> 443), which is rewritten to origin-form and replayed into the
// tunnel together with any bytes already read; an origin-form request may
// take its target from the Host: header. A malformed request line gets a 400
// and the connection is closed by the caller.
func httpAccept(br *bufio.Reader, c net.Conn) (local net.Conn, atyp byte, addr []byte, port uint16, err error) {
	head, err := readHTTPHead(br)
	if err != nil {
		writeHTTPReply(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, 0, nil, 0, err
	}
	line, term, hdrs, ok := splitFirstLine(head)
	if !ok {
		writeHTTPReply(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, 0, nil, 0, errors.New("http proxy: request head without a line terminator")
	}
	fields := strings.SplitN(string(line), " ", 3)
	if len(fields) != 3 || !strings.HasPrefix(fields[2], "HTTP/1.") || !httpMethods[fields[0]] {
		writeHTTPReply(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, 0, nil, 0, fmt.Errorf("http proxy: bad request line %q", firstN(string(line), 120))
	}
	method, uri := fields[0], fields[1]
	host, port, newPath, perr := parseProxyTarget(method, uri, hdrs)
	if perr != nil {
		writeHTTPReply(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, 0, nil, 0, perr
	}
	atyp, addr, aerr := httpTargetAddr(host)
	if aerr != nil {
		writeHTTPReply(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return nil, 0, nil, 0, aerr
	}
	if method == "CONNECT" {
		if err = writeHTTPReply(c, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
			return nil, 0, nil, 0, err
		}
		return bufferedConn{Conn: c, br: br}, atyp, addr, port, nil
	}
	// Plain method: absolute-URI request lines are rewritten to origin-form;
	// origin-form (Host-header target) is relayed unchanged. Everything the
	// parser already consumed (rewritten head + headers + body head bytes
	// sitting in br) is replayed ahead of live socket reads.
	buf := head
	if newPath != "" && newPath != uri {
		buf = append([]byte(method+" "+newPath+" "+fields[2]), term...)
		buf = append(buf, hdrs...)
	}
	if n := br.Buffered(); n > 0 {
		rest, derr := br.Peek(n)
		if derr != nil {
			return nil, 0, nil, 0, derr
		}
		buf = append(buf, rest...)
	}
	return &replayConn{Conn: c, buf: buf}, atyp, addr, port, nil
}

// readHTTPHead accumulates bytes until the terminating blank line
// (CRLF CRLF, tolerant of bare-LF endings), capped at httpHeadMax.
func readHTTPHead(br *bufio.Reader) ([]byte, error) {
	var head []byte
	for !bytes.HasSuffix(head, crlfcrlf) && !bytes.HasSuffix(head, []byte("\n\n")) {
		line, err := br.ReadSlice('\n')
		head = append(head, line...)
		if len(head) > httpHeadMax {
			return nil, fmt.Errorf("http proxy: request head over %d bytes", httpHeadMax)
		}
		if err != nil {
			return nil, err
		}
	}
	return head, nil
}

// splitFirstLine cuts the request line off the head, returning the exact line
// terminator so a rewritten request can keep it.
func splitFirstLine(head []byte) (line, term, rest []byte, ok bool) {
	if i := bytes.Index(head, []byte("\r\n")); i >= 0 {
		return head[:i], head[i : i+2], head[i+2:], true
	}
	if i := bytes.IndexByte(head, '\n'); i >= 0 {
		return head[:i], head[i : i+1], head[i+1:], true
	}
	return nil, nil, nil, false
}

// parseProxyTarget resolves the tunnel target for one request.
// CONNECT: the URI itself is host[:port] (port required).
// Plain methods: absolute-URI (http://host[:port]/path or https://... with
// defaults 80/443) -> newPath is the origin-form path (caller rewrites the
// request line); origin-form ("/path") -> target from the Host: header and
// newPath is "" (relay the request unchanged). Anything else -> error (400).
func parseProxyTarget(method, uri string, hdrs []byte) (host string, port uint16, newPath string, err error) {
	if method == "CONNECT" {
		host, port, err = splitAuthority(uri, 0)
		return host, port, "", err
	}
	if scheme, rest, ok := splitSchemeURI(uri); ok {
		if scheme != "http" && scheme != "https" {
			return "", 0, "", fmt.Errorf("http proxy: unsupported uri scheme %q", scheme)
		}
		def := uint16(80)
		if scheme == "https" {
			def = 443
		}
		auth, path := rest, "/"
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			auth, path = rest[:i], rest[i:]
		}
		host, port, err = splitAuthority(auth, def)
		return host, port, path, err
	}
	if strings.HasPrefix(uri, "/") {
		h := strings.TrimSpace(hostHeader(hdrs))
		if h == "" {
			return "", 0, "", errors.New("http proxy: origin-form request without Host header")
		}
		host, port, err = splitAuthority(h, 80)
		return host, port, "", err
	}
	return "", 0, "", fmt.Errorf("http proxy: request target %q is neither absolute-URI nor origin-form", firstN(uri, 120))
}

// splitSchemeURI splits "scheme://rest" (lowercasing the scheme).
func splitSchemeURI(uri string) (scheme, rest string, ok bool) {
	i := strings.Index(uri, "://")
	if i <= 0 {
		return "", "", false
	}
	return strings.ToLower(uri[:i]), uri[i+3:], true
}

// splitAuthority parses host[:port], accepting [IPv6] and [IPv6]:port forms.
// defPort 0 means the port is required.
func splitAuthority(s string, defPort uint16) (host string, port uint16, err error) {
	if s == "" {
		return "", 0, errors.New("http proxy: empty host")
	}
	if s[0] == '[' {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", 0, fmt.Errorf("http proxy: unterminated IPv6 literal %q", firstN(s, 60))
		}
		host = s[1:end]
		tail := s[end+1:]
		if tail == "" {
			if defPort == 0 {
				return "", 0, fmt.Errorf("http proxy: missing port for %q", firstN(s, 60))
			}
			return host, defPort, nil
		}
		if tail[0] != ':' {
			return "", 0, fmt.Errorf("http proxy: bad IPv6 literal %q", firstN(s, 60))
		}
		port, err = parsePort(tail[1:])
		return host, port, err
	}
	if i := strings.IndexByte(s, ':'); i >= 0 {
		if strings.Contains(s[i+1:], ":") {
			return "", 0, fmt.Errorf("http proxy: unbracketed IPv6 literal %q", firstN(s, 60))
		}
		host = s[:i]
		port, err = parsePort(s[i+1:])
		return host, port, err
	}
	if defPort == 0 {
		return "", 0, fmt.Errorf("http proxy: missing port for %q", firstN(s, 60))
	}
	return s, defPort, nil
}

func parsePort(s string) (uint16, error) {
	v, err := strconv.ParseUint(s, 10, 16)
	if err != nil || v == 0 {
		return 0, fmt.Errorf("http proxy: bad port %q", firstN(s, 20))
	}
	return uint16(v), nil
}

// hostHeader returns the Host: header value from the header block (the first
// match wins; header names are case-insensitive per RFC 9110).
func hostHeader(hdrs []byte) string {
	for _, line := range strings.Split(string(hdrs), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if len(line) > 5 && strings.EqualFold(line[:5], "host:") {
			return line[5:]
		}
	}
	return ""
}

// httpTargetAddr maps a proxy target host to the tunnel atyp/addr encoding
// (the same shapes socksAccept yields; the server resolves domains).
func httpTargetAddr(host string) (atyp byte, addr []byte, err error) {
	if host == "" {
		return 0, nil, errors.New("http proxy: empty host")
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return atypIPv4, v4, nil
		}
		return atypIPv6, ip.To16(), nil
	}
	// domain atyp carries a 1-byte length (see encodeAddrPort).
	if len(host) > 255 {
		return 0, nil, fmt.Errorf("http proxy: host too long (%d bytes)", len(host))
	}
	return atypDomain, []byte(host), nil
}

func writeHTTPReply(c net.Conn, s string) error {
	_, err := c.Write([]byte(s))
	return err
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
