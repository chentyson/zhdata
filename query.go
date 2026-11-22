package zhdata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// ---------------- in-tunnel account query (v0.15.4) ----------------
//
// zhenhanvn asks for the account (expiry date + traffic quota/used) of the
// uuid that authenticated its tunnel by sending a plain HTTP GET for the
// reserved hostname query.zhdata through the local socks port. The stream
// terminates on the final zhdata server, which answers it HERE instead of
// dialing a backend: an out-of-band query to the node's panel port does not
// work when that node is only reachable through the (possibly multi-hop)
// tunnel itself. Relay hops parse no frames, so they pass this through
// untouched and the final server answers (see relay.go).
//
// The request bytes are ignored on purpose: the session is already
// authenticated, so the answer is always for the session's own uuid — a
// key= in the URL cannot be used to query another user. Whether the first
// bytes are a recognisable HTTP request is irrelevant; the response is
// always a well-formed HTTP/1.1 response because the GUI writes its own
// request on this stream and parses a standard reply.

// queryHost is the reserved hostname; -query-host overrides it, "" = off.
const queryHost = DefaultQueryHost

const queryBodyLimit = 1 << 20 // account source response body cap

// queryHTTP is the dedicated client for -query-url fetches: fixed 5s timeout,
// Proxy nil so it never inherits HTTP(S)_PROXY (the account source normally
// lives on loopback next to the server).
var queryHTTP = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{Proxy: nil},
}

// queryAccountData is the "data" object zhenhanvn parses: lowercase,
// case-sensitive JSON names; the GUI tolerates extra/missing fields.
type queryAccountData struct {
	Expiry           int64  `json:"expiry"` // unix seconds
	ExpiryStr        string `json:"expiry_str"`
	UnlimitedTime    bool   `json:"unlimited_time"`
	Volume           int64  `json:"volume"` // quota bytes
	VolumeStr        string `json:"volume_str"`
	UnlimitedTraffic bool   `json:"unlimited_traffic"`
	Used             int64  `json:"used"` // bytes
	UsedStr          string `json:"used_str"`
	Up               int64  `json:"up"`
	Down             int64  `json:"down"`
	Devs             int    `json:"devs"`
}

// answerQuery serves one query stream: the JSON answer travels the normal
// backend write path (serveBackend with a local conn), so tOpenAck, tData
// ordering, downlink flow control, the final tAck/tClose and traffic
// accounting behave exactly like a real backend stream.
func (a *serverApp) answerQuery(sh *sSession, st *sStream) {
	body := a.queryAnswer(sh.uuid)
	a.serveBackend(sh, st, &queryConn{r: bytes.NewReader(queryResponse(body))}, a.cfg.queryHost, "query")
}

// queryAnswer returns just the JSON body, by priority:
// (a) account fields the key store knows for this uuid;
// (b) the configured -query-url account source, upstream body verbatim
//
//	(whatever code the panel itself returns); fetch failure -> code 502;
//
// (c) neither -> code 404.
// One info line per query with the short uuid and the outcome; the panel
// polls, so nothing else is logged.
func (a *serverApp) queryAnswer(uuid string) []byte {
	if acc, ok := a.ks.account(uuid); ok {
		up, down, devs := a.countersFor(uuid)
		data := queryAccountData{
			Expiry:           acc.Expiry,
			UnlimitedTime:    acc.Expiry <= 0,
			Volume:           acc.Quota,
			UnlimitedTraffic: acc.Quota <= 0,
			Used:             acc.Used,
			Up:               up,
			Down:             down,
			Devs:             devs,
		}
		if acc.Expiry > 0 {
			data.ExpiryStr = time.Unix(acc.Expiry, 0).Format("2006-01-02 15:04:05")
		}
		if acc.Quota > 0 {
			data.VolumeStr = humanBytes(acc.Quota)
		}
		if acc.Used > 0 {
			data.UsedStr = humanBytes(acc.Used)
		}
		db, _ := json.Marshal(data)
		logInfo("query", "uuid=%s answered from key store expiry=%d quota=%d used=%d", short(uuid), acc.Expiry, acc.Quota, acc.Used)
		return append(append([]byte(`{"code":200,"msg":"ok","data":`), db...), '}')
	}
	if src := strings.TrimSpace(a.cfg.queryURL); src != "" {
		if body, err := fetchAccount(src, uuid); err == nil {
			logInfo("query", "uuid=%s answered from account source (%d bytes)", short(uuid), len(body))
			return body
		} else {
			logInfo("query", "uuid=%s account source unreachable: %v", short(uuid), err)
			return []byte(`{"code":502,"msg":"account source unreachable"}`)
		}
	}
	logInfo("query", "uuid=%s no account info", short(uuid))
	return []byte(`{"code":404,"msg":"no account info for this uuid"}`)
}

// queryResponse frames a JSON body as the HTTP/1.1 response the GUI's HTTP
// client expects on this stream.
func queryResponse(body []byte) []byte {
	hdr := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(body))
	return append([]byte(hdr), body...)
}

// fetchAccount GETs the account source (-query-url, %s replaced with the
// uuid) and returns its body bytes for verbatim pass-through.
func fetchAccount(src, uuid string) ([]byte, error) {
	resp, err := queryHTTP.Get(strings.Replace(src, "%s", uuid, 1))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, queryBodyLimit))
}

// countersFor reads one uuid's live tunnel counters and active device count
// (the same accessors GET /v1/stats uses) for the query answer.
func (a *serverApp) countersFor(uuid string) (up, down int64, devs int) {
	a.traffMu.Lock()
	if c := a.traffic[uuid]; c != nil {
		up, down = c.up.Load(), c.down.Load()
	}
	a.traffMu.Unlock()
	a.mu.Lock()
	devs = len(a.devices[uuid])
	a.mu.Unlock()
	return
}

// humanBytes formats a byte count the way panels print quotas.
func humanBytes(n int64) string {
	div, name := int64(1), "B"
	for _, u := range []string{"KB", "MB", "GB", "TB", "PB"} {
		if n < div*1024 {
			break
		}
		div *= 1024
		name = u
	}
	if div == 1 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), name)
}

// queryConn stands in for a backend conn on a query stream: Read yields the
// canned response then EOF, Write discards the request bytes.
type queryConn struct{ r *bytes.Reader }

func (c *queryConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *queryConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *queryConn) Close() error                     { return nil }
func (c *queryConn) LocalAddr() net.Addr              { return nil }
func (c *queryConn) RemoteAddr() net.Addr             { return nil }
func (c *queryConn) SetDeadline(time.Time) error      { return nil }
func (c *queryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *queryConn) SetWriteDeadline(time.Time) error { return nil }
