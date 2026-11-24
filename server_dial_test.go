package zhdata

import (
	"errors"
	"net"
	"testing"
)

// A domain that resolves to AAAA records only used to skip both dial loops, so
// openBackend called serveBackend with a nil conn and panicked (C455, 2026-11-24
// 13:27). dialPreferred has to try every family it was given and never hand back
// a nil conn with a nil error.
func TestDialPreferred(t *testing.T) {
	okConn := &net.TCPConn{}
	notNow := errors.New("refused")

	t.Run("AAAA only is still dialed", func(t *testing.T) {
		var tried []string
		dialOne := func(ip string) (net.Conn, error) {
			tried = append(tried, ip)
			if ip == "2001:db8::1" {
				return okConn, nil
			}
			return nil, notNow
		}
		bc, err := dialPreferred(dialOne, nil, []string{"2001:db8::1"})
		if err != nil {
			t.Fatalf("err = %v, want the AAAA dial to succeed", err)
		}
		if bc != net.Conn(okConn) {
			t.Fatalf("bc = %v, want the AAAA conn", bc)
		}
		if len(tried) != 1 || tried[0] != "2001:db8::1" {
			t.Fatalf("tried = %v, want the AAAA address", tried)
		}
	})

	t.Run("IPv4 first and IPv6 as fallback", func(t *testing.T) {
		var tried []string
		dialOne := func(ip string) (net.Conn, error) {
			tried = append(tried, ip)
			if ip == "2001:db8::2" {
				return okConn, nil
			}
			return nil, notNow
		}
		bc, err := dialPreferred(dialOne, []string{"192.0.2.1", "192.0.2.2"}, []string{"2001:db8::2"})
		if err != nil || bc != net.Conn(okConn) {
			t.Fatalf("bc, err = %v, %v; want the fallback conn", bc, err)
		}
		want := []string{"192.0.2.1", "192.0.2.2", "2001:db8::2"}
		if len(tried) != len(want) {
			t.Fatalf("tried = %v, want %v", tried, want)
		}
		for i := range want {
			if tried[i] != want[i] {
				t.Fatalf("tried = %v, want %v", tried, want)
			}
		}
	})

	t.Run("empty resolution is an error, not a nil conn", func(t *testing.T) {
		bc, err := dialPreferred(func(string) (net.Conn, error) { return okConn, nil }, nil, nil)
		if bc != nil {
			t.Fatalf("bc = %v, want nil", bc)
		}
		if err == nil {
			t.Fatal("err = nil, want an error so the caller closes the stream")
		}
	})

	t.Run("all attempts fail", func(t *testing.T) {
		bc, err := dialPreferred(func(string) (net.Conn, error) { return nil, notNow }, []string{"192.0.2.1"}, nil)
		if bc != nil {
			t.Fatalf("bc = %v, want nil", bc)
		}
		if !errors.Is(err, notNow) {
			t.Fatalf("err = %v, want the dial failure", err)
		}
	})
}
