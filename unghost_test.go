package unghost

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestWrap_NoGoroutineLeak(t *testing.T) {
	// goleak belongs in unit tests, not benchmarks
	defer goleak.VerifyNone(t)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	htc, err := Wrap(client, KeepAliveConfig{}, 0)
	if err != nil {
		t.Fatalf("Wrap failed: %v", err)
	}

	// Close the wrapper and ensure background routines clean up
	if err := htc.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWrap(t *testing.T) {
	t.Run("Edge case: nil connection returns ErrConnectionNil", func(t *testing.T) {
		htc, err := Wrap(nil, KeepAliveConfig{}, 0)
		if !errors.Is(err, ErrConnectionNil) {
			t.Errorf("Wrap: got error %v, want %v", err, ErrConnectionNil)
		}
		if htc != nil {
			t.Errorf("Wrap: expected nil struct on error, got %+v", htc)
		}
	})

	t.Run("Base case: zero configs fall back to default values", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc, err := Wrap(localConn, KeepAliveConfig{}, 0)
		if err != nil {
			t.Fatalf("Wrap: unexpected error: %v", err)
		}
		defer htc.Close()

		if htc.config.KeepAliveInterval != defaultKeepAliveInterval {
			t.Errorf("Interval default mismatch: got %v, want %v", htc.config.KeepAliveInterval, defaultKeepAliveInterval)
		}
		if htc.config.KeepAliveTimeout != defaultKeepAliveTimeout {
			t.Errorf("Timeout default mismatch: got %v, want %v", htc.config.KeepAliveTimeout, defaultKeepAliveTimeout)
		}
		if htc.flowControlData.maxWindowSize != MaxWindowSize {
			t.Errorf("maxWindowSize default mismatch: got %d, want %d", htc.flowControlData.maxWindowSize, MaxWindowSize)
		}
	})

	t.Run("Intent case: custom non-zero configurations are preserved", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		customConfig := KeepAliveConfig{
			KeepAliveInterval: 10 * time.Second,
			KeepAliveTimeout:  30 * time.Second,
		}
		customMax := uint32(8192)

		htc, err := Wrap(localConn, customConfig, customMax)
		if err != nil {
			t.Fatalf("Wrap custom config error: %v", err)
		}
		defer htc.Close()

		if htc.config.KeepAliveInterval != 10*time.Second {
			t.Errorf("Custom interval got %v, want 10s", htc.config.KeepAliveInterval)
		}
		if htc.flowControlData.maxWindowSize != customMax {
			t.Errorf("Custom maxWindowSize got %d, want %d", htc.flowControlData.maxWindowSize, customMax)
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("Intent case: Close is idempotent and race-free under concurrent calls", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc, err := Wrap(localConn, KeepAliveConfig{}, 0)
		if err != nil {
			t.Fatalf("Wrap error: %v", err)
		}

		var wg sync.WaitGroup
		const concurrencyCount = 10

		for i := 0; i < concurrencyCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = htc.Close()
			}()
		}

		wg.Wait()

		htc.flowControlData.flowDataLock.Lock()
		isClosed := htc.flowControlData.isClosed
		htc.flowControlData.flowDataLock.Unlock()

		if !isClosed {
			t.Errorf("isClosed flag was not set to true after concurrent calls")
		}
	})
}

func TestDeadlinesDisabled(t *testing.T) {
	t.Run("Intent case: deadline overrides return standard ErrDeadlinesDisabled", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer localConn.Close()
		defer remoteConn.Close()

		htc := &HeartbeatTCP{Conn: localConn}
		dummyTime := time.Now()

		if err := htc.SetDeadline(dummyTime); !errors.Is(err, ErrDeadlinesDisabled) {
			t.Errorf("SetDeadline: got %v, want %v", err, ErrDeadlinesDisabled)
		}
		if err := htc.SetReadDeadline(dummyTime); !errors.Is(err, ErrDeadlinesDisabled) {
			t.Errorf("SetReadDeadline: got %v, want %v", err, ErrDeadlinesDisabled)
		}
		if err := htc.SetWriteDeadline(dummyTime); !errors.Is(err, ErrDeadlinesDisabled) {
			t.Errorf("SetWriteDeadline: got %v, want %v", err, ErrDeadlinesDisabled)
		}
	})
}

func TestRaceConditions(t *testing.T) {
	t.Run("Concurrent Read, Write, Ping, and Close under race detector", func(t *testing.T) {
		server, client := net.Pipe()

		htcServer, err := Wrap(server, KeepAliveConfig{KeepAliveInterval: 50 * time.Millisecond}, 0)
		if err != nil {
			t.Fatalf("Wrap server failed: %v", err)
		}
		htcClient, err := Wrap(client, KeepAliveConfig{KeepAliveInterval: 50 * time.Millisecond}, 0)
		if err != nil {
			t.Fatalf("Wrap client failed: %v", err)
		}

		var wg sync.WaitGroup
		payload := []byte("concurrent payload data for race detector test")

		// Goroutine 1: Client writes continuously
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, err := htcClient.Write(payload)
				if err != nil {
					return
				}
			}
		}()

		// Goroutine 2: Server reads continuously
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, len(payload))
			for i := 0; i < 50; i++ {
				_, err := io.ReadFull(htcServer, buf)
				if err != nil {
					return
				}
			}
		}()

		// Goroutine 3: Concurrent credit updates
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				htcServer.flowControlData.flowDataLock.Lock()
				htcServer.refundCredits(1024)
				htcServer.flowControlData.flowDataLock.Unlock()
				time.Sleep(1 * time.Millisecond)
			}
		}()

		wg.Wait()
		_ = htcServer.Close()
		_ = htcClient.Close()
	})
}
