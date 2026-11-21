package zhdata

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func usersFileWithDeviceLimit(t *testing.T, uuid string, maxDevices int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "users.json")
	body := fmt.Sprintf(`{"users":[{"uuid":%q,"max_devices":%d}]}`, uuid, maxDevices)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestZhV7DeviceIdSeparatesUUIDDevices(t *testing.T) {
	const uuid = "11111111-aaaa-bbbb-cccc-000000000001"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWith(t, uuid),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong-one")) }))
	defer s1.Close()
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong-two")) }))
	defer s2.Close()
	h1, p1 := splitHostPort(t, s1.URL)
	h2, p2 := splitHostPort(t, s2.URL)

	cli1, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		deviceID: "000000000000000000000000000000a1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli1.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "device 1 session established")

	cli2, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		deviceID: "000000000000000000000000000000a2",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 2 }, "device 2 session established")

	body1, err := httpThroughTunnel(t, cli1.Addr(), h1, p1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body1, "pong-one") {
		t.Fatalf("device 1 got %q", body1)
	}
	body2, err := httpThroughTunnel(t, cli2.Addr(), h2, p2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body2, "pong-two") {
		t.Fatalf("device 2 got %q", body2)
	}

	srv.mu.Lock()
	devices := len(srv.devices[uuid])
	srv.mu.Unlock()
	if devices != 2 {
		t.Fatalf("expected 2 active devices, got %d", devices)
	}
}

func TestZhV7DeviceLimitRejectsNewDevice(t *testing.T) {
	const uuid = "11111111-aaaa-bbbb-cccc-000000000002"
	srv, err := newServerApp(serverConfig{
		listen:      "127.0.0.1:0",
		usersPath:   usersFileWithDeviceLimit(t, uuid, 1),
		backend:     "direct",
		maxSessions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli1, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		deviceID: "000000000000000000000000000000b1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli1.Close()
	waitFor(t, func() bool { return srv.estab.Load() >= 1 }, "first device established")

	cli2, err := newClientApp(clientConfig{
		listen:   "127.0.0.1:0",
		server:   srv.Addr(),
		uuid:     uuid,
		deviceID: "000000000000000000000000000000b2",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()

	// Give the second client time to dial and be rejected before it can
	// establish. It retries forever with backoff, but never counts as a new
	// device.
	time.Sleep(1500 * time.Millisecond)

	srv.mu.Lock()
	active := srv.active
	devices := len(srv.devices[uuid])
	srv.mu.Unlock()
	if active != 1 {
		t.Fatalf("expected one active session, got %d", active)
	}
	if devices != 1 {
		t.Fatalf("expected one active device, got %d", devices)
	}
}
