package zhdata

import (
	"strings"
	"sync"
	"time"
)

// Per-uuid bandwidth caps (v0.15.6).
//
// A cap travels with the user, not with the connection: it comes from the admin
// API user set (PUT /v1/users "up_kbps"/"down_kbps"/"speed_limit", §3.6), from a
// users JSON entry, or from policy columns in an s-ui style sqlite source. 0 in
// both directions means unlimited, and an API entry with 0 lifts a cap the file
// source carried -- the same override rule as max_devices.
//
// Enforcement is one token bucket per uuid per direction, shared by EVERY
// session and every stream that uuid owns. That is the whole point: the client
// runs -conns 32, so a per-session cap would hand out 32 times the bandwidth it
// is supposed to meter.
//
// Like the panel's own limiter this is a userspace cap. A bucket holds at most
// one second of its cap, so an idle client may open with a one-second burst and
// a client that keeps the pipe full is held at the cap. Bytes are charged where
// the server actually hands them over -- upload when a frame is written to the
// backend, download when backend bytes go toward the client -- so one buffer's
// worth (plus the kernel and the tunnel's own downlink window) is always in
// flight past the cap. The cap is a rate, not a shaper: expect the average to
// settle on it, not each second to be exactly it.
//
// A cap that changes (file hot reload, panel push) reshapes the live buckets,
// and a cap that disappears stops limiting on the next frame. No session
// restart, no reconnect.
//
// Bytes are charged as debt: reserve() subtracts the request and reports how
// long the caller must wait for the bucket to pay for it. Concurrent streams of
// the same uuid therefore add up to the cap instead of each getting the cap.
const (
	bwBurstSeconds = 1.0                    // a bucket accumulates at most one second of its cap
	bwMinBurst     = 32 << 10               // even a tiny cap gets one frame of headroom
	bwParkBound    = 250 * time.Millisecond // longest single sleep while holding a stream lock
	bwWaitSlice    = 25 * time.Millisecond  // sleep granularity while parked
)

// bwBucket is one direction of one uuid's cap, in bytes.
type bwBucket struct {
	mu    sync.Mutex
	bps   float64 // cap in bytes/second
	burst float64 // ceiling for saved tokens
	avail float64 // may go negative: the debt a later waiter pays for
	last  time.Time
}

func newBWBucket(kbps int) *bwBucket {
	bps := float64(kbps) * 1000 / 8 // kilobit/s -> bytes/s (1000-based, like the panel's kbit)
	burst := bps * bwBurstSeconds
	if burst < bwMinBurst {
		burst = bwMinBurst
	}
	return &bwBucket{bps: bps, burst: burst, avail: burst, last: time.Now()}
}

// reserve charges n bytes and returns how long the caller must wait before
// passing them; 0 means they fit right now.
func (b *bwBucket) reserve(n int64) time.Duration {
	if b == nil || b.bps <= 0 || n <= 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if el := now.Sub(b.last); el > 0 {
		// Duration counts nanoseconds: convert to seconds before multiplying,
		// or a few milliseconds of elapsed time "refills" a thousandfold.
		b.avail += el.Seconds() * b.bps
		if b.avail > b.burst {
			b.avail = b.burst
		}
		b.last = now
	}
	b.avail -= float64(n)
	if b.avail >= 0 {
		return 0
	}
	return time.Duration(-b.avail / b.bps * float64(time.Second))
}

// bwPair is one uuid's two buckets, tagged with the caps they were built from
// so a policy change is noticed without any signalling.
type bwPair struct {
	up, down *bwBucket
	upKbps   int
	downKbps int
}

// bwRegistry keeps the live buckets per uuid. Entries are cheap and stay after
// a cap is lifted (a nil pair stops limiting immediately); the map is only
// touched when a cap is first seen or changes.
type bwRegistry struct {
	mu sync.Mutex
	m  map[string]*bwPair
}

func newBWRegistry() *bwRegistry { return &bwRegistry{m: map[string]*bwPair{}} }

// pairFor returns uuid's buckets, rebuilt in place when the caps changed.
func (r *bwRegistry) pairFor(uuid string, upKbps, downKbps int) *bwPair {
	if upKbps <= 0 && downKbps <= 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.m[uuid]; ok && p.upKbps == upKbps && p.downKbps == downKbps {
		return p
	}
	p := &bwPair{upKbps: upKbps, downKbps: downKbps}
	if upKbps > 0 {
		p.up = newBWBucket(upKbps)
	}
	if downKbps > 0 {
		p.down = newBWBucket(downKbps)
	}
	r.m[uuid] = p
	return p
}

// drop removes uuid's buckets (used when a user disappears from every source).
func (r *bwRegistry) drop(uuid string) {
	r.mu.Lock()
	delete(r.m, uuid)
	r.mu.Unlock()
}

// waitBW parks the caller until n bytes fit in the direction's bucket.
//
// maxBound caps the total parking time: the pre-dial flush runs holding the
// stream lock and must not sit there, so it passes a small bound and lets the
// debt fall due in pumpUp, which waits outside the lock. dead, when given, is
// consulted between slices so a stream that died mid-wait stops waiting; ok=false
// means exactly that and the caller should abandon the write.
func waitBW(p *bwPair, up bool, n int64, maxBound time.Duration, dead func() bool) bool {
	if p == nil {
		return true
	}
	b := p.down
	if up {
		b = p.up
	}
	if b == nil {
		return true
	}
	// One charge, then park for exactly what it reported. reserve already folded
	// the refill in, so the remaining wait is just the clock, not more debt.
	wait := b.reserve(n)
	spent := time.Duration(0)
	for wait > 0 {
		if dead != nil && dead() {
			return false
		}
		slice := wait
		if slice > bwWaitSlice {
			slice = bwWaitSlice
		}
		if maxBound > 0 && spent+slice >= maxBound {
			return true // bytes are charged; the debt falls due on the next write
		}
		time.Sleep(slice)
		spent += slice
		wait -= slice
	}
	return true
}

// normUUID normalizes a uuid the way every user source and policy lookup does.
func normUUID(uuid string) string {
	return strings.ToLower(strings.TrimSpace(uuid))
}
