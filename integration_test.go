//go:build integration
// +build integration

package unghost

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

// fastKeepAlive is used to drastically reduce test execution time
var fastKeepAlive = KeepAliveConfig{
	KeepAliveInterval: 100 * time.Millisecond,
	KeepAliveTimeout:  500 * time.Millisecond,
}

// Docker routing constants
const (
	toxiApiUrl         = "127.0.0.1:8474"
	serverListenAddr   = "127.0.0.1:8476" // Binds locally to bypass Windows Firewall prompt
	toxiUpstreamTarget = "host.docker.internal:8476"
	toxiListenAddr     = "0.0.0.0:8475"
	clientDialAddr     = "127.0.0.1:8475"
)

// setupHarness initializes the Toxiproxy client, creates the proxy, and sets up the upstream listener.
func setupHarness(t *testing.T) (*toxiproxy.Proxy, net.Listener) {
	t.Helper()
	t.Logf("Setting up Toxiproxy harness. Upstream Target: %s, Proxy Listen: %s", toxiUpstreamTarget, toxiListenAddr)

	listener, err := net.Listen("tcp", serverListenAddr)
	if err != nil {
		t.Fatalf("Failed to start upstream listener: %v", err)
	}

	toxiClient := toxiproxy.NewClient(toxiApiUrl)
	proxyName := fmt.Sprintf("unghost_test_proxy_%d", time.Now().UnixNano())

	if p, err := toxiClient.Proxy(proxyName); err == nil {
		t.Logf("Deleting old proxy: %s", proxyName)
		p.Delete()
	}

	proxy, err := toxiClient.CreateProxy(proxyName, toxiListenAddr, toxiUpstreamTarget)
	if err != nil {
		listener.Close()
		t.Fatalf("Failed to create Toxiproxy. Error: %v", err)
	}

	t.Cleanup(func() {
		t.Log("Cleaning up Toxiproxy harness...")
		proxy.Delete()
		listener.Close()
	})

	return proxy, listener
}

// assertGoroutineLeak verifies that all internal background goroutines are cleanly terminated.
func assertGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()

	var current int
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		current = runtime.NumGoroutine()
		if current <= baseline {
			t.Logf("Goroutine check: baseline=%d, current=%d", baseline, current)
			return
		}
	}

	t.Errorf("Goroutine leak detected: baseline=%d, current=%d", baseline, current)
}

// ----------------------------------------------------------------------
// Phase 1: Core Functionality Tests
// ----------------------------------------------------------------------

func TestSanity_SimpleMessage(t *testing.T) {
	t.Log("--- Starting TestSanity_SimpleMessage ---")

	// Initialize Toxiproxy harness FIRST so HTTP client pool goroutines are created
	_, listener := setupHarness(t)

	// Measure baseline AFTER setupHarness creates background HTTP connections
	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	var wg sync.WaitGroup
	wg.Add(1)

	// Server
	go func() {
		defer wg.Done()
		t.Log("Server: Waiting for connection...")
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}

		t.Log("Server: Connection accepted, wrapping with unghost...")
		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 1024)
		t.Log("Server: Waiting to read data...")
		n, err := serverConn.Read(buf)
		if err != nil {
			return
		}

		msg := string(buf[:n])
		t.Logf("Server: Read %d bytes: '%s'", n, msg)
		if msg != "Hello Unghost!" {
			t.Errorf("Expected 'Hello Unghost!', got '%s'", msg)
		}

		t.Log("Server: Sending acknowledgment...")
		serverConn.Write([]byte("Acknowledged"))
	}()

	// Client
	t.Log("Client: Dialing proxy...")
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}

	t.Log("Client: Wrapping connection...")
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	t.Log("Client: Sending 'Hello Unghost!'...")
	clientConn.Write([]byte("Hello Unghost!"))

	buf := make([]byte, 1024)
	t.Log("Client: Waiting for acknowledgment...")
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("Client Read failed: %v", err)
	}

	msg := string(buf[:n])
	t.Logf("Client: Received '%s'", msg)
	if msg != "Acknowledged" {
		t.Errorf("Expected 'Acknowledged', got '%s'", msg)
	}

	wg.Wait()
	t.Log("--- TestSanity_SimpleMessage Complete ---")
}

func TestDataIntegrity_100MB(t *testing.T) {
	t.Log("--- Starting TestDataIntegrity_100MB ---")

	_, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	payloadSize := 100 * 1024 * 1024 // 100 MB
	t.Logf("Generating %d bytes of random payload data...", payloadSize)
	payload := make([]byte, payloadSize)
	rand.Read(payload)

	expectedHash := sha256.Sum256(payload)

	var wg sync.WaitGroup
	wg.Add(1)

	// Server
	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}

		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		t.Log("Server: Ready to receive 100MB stream...")
		hasher := sha256.New()
		buf := make([]byte, ChunkSizeDefault)
		bytesRead := 0

		for bytesRead < payloadSize {
			n, err := serverConn.Read(buf)
			if err != nil && err != io.EOF {
				t.Errorf("Unexpected read error at byte %d: %v", bytesRead, err)
				break
			}
			hasher.Write(buf[:n])
			bytesRead += n
		}

		actualHash := hasher.Sum(nil)
		if !bytes.Equal(expectedHash[:], actualHash) {
			t.Errorf("Hash mismatch! Data corruption detected during 100MB transfer.")
		}
	}()

	// Client
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}

	t.Log("Client: Starting io.Copy of 100MB payload to server...")
	reader := bytes.NewReader(payload)
	io.Copy(clientConn, reader)

	// Wait for server to process all bytes before closing client connection
	wg.Wait()
	clientConn.Close()
	t.Log("--- TestDataIntegrity_100MB Complete ---")
}

func TestFlowControl_SlowReader(t *testing.T) {
	t.Log("--- Starting TestFlowControl_SlowReader ---")

	_, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	var wg sync.WaitGroup
	wg.Add(1)

	// Server (Slow Reader)
	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}

		serverConn, err := Wrap(rawConn, fastKeepAlive, 64*1024)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		time.Sleep(1 * time.Second)

		buf := make([]byte, 64*1024)
		serverConn.Read(buf)
	}()

	// Client (Fast Writer)
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, 64*1024)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	payload := make([]byte, 64*1024)
	writeCompletedCh := make(chan struct{})

	go func() {
		clientConn.Write(payload)
		clientConn.Write(payload)
		close(writeCompletedCh)
	}()

	select {
	case <-writeCompletedCh:
		t.Log("Test Passed: The writer eventually unblocked once the server read the data.")
	case <-time.After(3 * time.Second):
		t.Fatalf("Deadlock detected: Sender never received credit refund to unblock Write()")
	}

	wg.Wait()
}

// ----------------------------------------------------------------------
// Phase 2: Chaos & Resilience Tests (Toxiproxy)
// ----------------------------------------------------------------------

func TestChaos_BandwidthThrottlingAndFragmentation(t *testing.T) {
	t.Log("--- Starting TestChaos_BandwidthThrottlingAndFragmentation ---")

	proxy, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	proxy.AddToxic("throttle", "bandwidth", "downstream", 1.0, toxiproxy.Attributes{
		"rate": 100,
	})

	var wg sync.WaitGroup
	wg.Add(1)

	// Server
	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 1024*50)
		_, err = io.ReadFull(serverConn, buf)
		if err != nil {
			t.Errorf("Failed to read under heavy fragmentation: %v", err)
		}
	}()

	// Client
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	payload := make([]byte, 1024*50) // 50 KB
	clientConn.Write(payload)
	wg.Wait()
}

func TestChaos_HighLatencyAndJitter(t *testing.T) {
	t.Log("--- Starting TestChaos_HighLatencyAndJitter ---")

	proxy, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	proxy.AddToxic("lag", "latency", "downstream", 1.0, toxiproxy.Attributes{
		"latency": 200,
		"jitter":  50,
	})

	var wg sync.WaitGroup
	wg.Add(1)

	// Server
	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}

		relaxedKeepAlive := KeepAliveConfig{
			KeepAliveInterval: 500 * time.Millisecond,
			KeepAliveTimeout:  2000 * time.Millisecond,
		}
		serverConn, err := Wrap(rawConn, relaxedKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 10)
		serverConn.Read(buf)
	}()

	// Client
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	relaxedKeepAlive := KeepAliveConfig{
		KeepAliveInterval: 500 * time.Millisecond,
		KeepAliveTimeout:  2000 * time.Millisecond,
	}
	clientConn, err := Wrap(rawClientConn, relaxedKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	_, err = clientConn.Write([]byte("Late Data"))
	if err != nil {
		t.Fatalf("Latency toxic caused unexpected failure: %v", err)
	}
	wg.Wait()
}

func TestChaos_SilentDrop(t *testing.T) {
	t.Log("--- Starting TestChaos_SilentDrop ---")

	proxy, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 10)
		_, err = serverConn.Read(buf)
		if err == nil {
			t.Errorf("Server: Expected timeout error, got nil")
		}
	}()

	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	proxy.AddToxic("blackhole", "timeout", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": 0,
	})

	time.Sleep(fastKeepAlive.KeepAliveTimeout + 200*time.Millisecond)

	_, err = clientConn.Write([]byte("Lost in the void"))
	if err == nil {
		t.Errorf("Client: Expected Write to fail after silent drop, but it succeeded")
	}

	wg.Wait()
}

func TestChaos_HardReset(t *testing.T) {
	t.Log("--- Starting TestChaos_HardReset ---")

	proxy, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 1024)
		var readErr error
		// Continuously read until the TCP RST causes an error
		for {
			_, readErr = serverConn.Read(buf)
			if readErr != nil {
				break
			}
		}

		if readErr == nil {
			t.Errorf("Server: Expected connection reset error on Read(), got nil")
		}
	}()

	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	clientConn, err := Wrap(rawClientConn, fastKeepAlive, MaxWindowSize)
	if err != nil {
		t.Fatalf("Client Wrap failed: %v", err)
	}
	defer clientConn.Close()

	proxy.AddToxic("reset", "reset_peer", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": 0,
	})

	time.Sleep(100 * time.Millisecond)

	// Send data until a write fails from the reset packet
	var writeErr error
	for i := 0; i < 5; i++ {
		_, writeErr = clientConn.Write([]byte("Trigger Proxy"))
		if writeErr != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if writeErr == nil {
		t.Errorf("Client: Expected Write to fail on Hard Reset, but it succeeded")
	}

	wg.Wait()
}

// ----------------------------------------------------------------------
// Phase 3: Protocol Error Recovery
// ----------------------------------------------------------------------

func TestProtocol_MalformedDataInjection(t *testing.T) {
	t.Log("--- Starting TestProtocol_MalformedDataInjection ---")

	_, listener := setupHarness(t)

	baseline := runtime.NumGoroutine()
	defer assertGoroutineLeak(t, baseline)

	var wg sync.WaitGroup
	wg.Add(1)

	// Server
	go func() {
		defer wg.Done()
		rawConn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn, err := Wrap(rawConn, fastKeepAlive, MaxWindowSize)
		if err != nil {
			rawConn.Close()
			return
		}
		defer serverConn.Close()

		buf := make([]byte, 1024)
		_, err = serverConn.Read(buf)
		if err == nil {
			t.Errorf("Server: Expected protocol error from malformed data, but read succeeded")
		}
	}()

	// Malicious Client
	rawClientConn, err := net.Dial("tcp", clientDialAddr)
	if err != nil {
		t.Fatalf("Client dial failed: %v", err)
	}
	defer rawClientConn.Close()

	garbageFrame := []byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF}
	rawClientConn.Write(garbageFrame)

	wg.Wait()
}
