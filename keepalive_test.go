package unghost

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func setupKeepAliveTestState(conn net.Conn) *HeartbeatTCP {
	maxWindowSize := uint32(4096)

	htc := &HeartbeatTCP{
		Conn: conn,
		config: KeepAliveConfig{
			KeepAliveTimeout: 5 * time.Minute,
		},
		flowControlData: flowControlData{
			sendCredits:              maxWindowSize,
			processedCredits:         100,
			processedCreditsNotifyCh: make(chan struct{}, 1),
			maxWindowSize:            maxWindowSize,
		},
		tcpDataCh:   make(chan tcpReadData, 5),
		heartbeatCh: make(chan byte, 5),
		closeCh:     make(chan struct{}, 5),
	}

	htc.flowControlData.sndNotifyCond = sync.NewCond(&htc.flowControlData.flowDataLock)
	return htc
}

func TestKeepAliveSender(t *testing.T) {
	t.Run("Base case: rejects non-heartbeat flags", func(t *testing.T) {
		htc := setupKeepAliveTestState(nil)

		err := htc.keepAliveSender(FlagUserData)
		if !errors.Is(err, ErrInvalidHeartbeat) {
			t.Errorf("got error %v, want %v", err, ErrInvalidHeartbeat)
		}
	})

	t.Run("Intent case: transmits Ping and resets processed credits", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc := setupKeepAliveTestState(localConn)

		var sendErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			sendErr = htc.keepAliveSender(FlagPing)
		}()

		buf := make([]byte, HEADERLENGTH)
		n, err := remoteConn.Read(buf)
		wg.Wait()

		if err != nil || sendErr != nil || n != HEADERLENGTH {
			t.Fatalf("Sender failed: readErr=%v, sendErr=%v, n=%d", err, sendErr, n)
		}

		flag, credits, length := parseHeader(buf)
		if flag != FlagPing || credits != 100 || length != 0 {
			t.Errorf("Header mismatch: flag=0x%02x, credits=%d, length=%d", flag, credits, length)
		}

		htc.flowControlData.flowDataLock.Lock()
		processed := htc.flowControlData.processedCredits
		htc.flowControlData.flowDataLock.Unlock()

		if processed != 0 {
			t.Errorf("processedCredits was not reset to 0, got %d", processed)
		}
	})

	t.Run("Intent case: restores processed credits on network write failure", func(t *testing.T) {
		mockConn := &mockErrConn{writeErr: errors.New("write failure")}
		htc := setupKeepAliveTestState(mockConn)

		htc.flowControlData.flowDataLock.Lock()
		initialCredits := htc.flowControlData.processedCredits
		htc.flowControlData.flowDataLock.Unlock()

		err := htc.keepAliveSender(FlagPing)

		if err == nil {
			t.Fatalf("Expected keepalive write error, got nil")
		}

		htc.flowControlData.flowDataLock.Lock()
		currentCredits := htc.flowControlData.processedCredits
		htc.flowControlData.flowDataLock.Unlock()

		if currentCredits != initialCredits {
			t.Errorf("processedCredits not restored on write error: got %d, want %d", currentCredits, initialCredits)
		}
	})

	t.Run("Edge case: keepalive write timeout triggers closeCh signal", func(t *testing.T) {
		mockConn := &mockErrConn{writeErr: &mockTimeoutErr{}}
		htc := setupKeepAliveTestState(mockConn)

		err := htc.keepAliveSender(FlagPing)

		if err == nil {
			t.Fatalf("Expected keepalive write timeout error, got nil")
		}

		select {
		case <-htc.closeCh:
			// Success
		default:
			t.Errorf("Expected closeCh signal on net.Error timeout in keepalive sender")
		}
	})
}

func TestKeepAliveReceiver(t *testing.T) {
	t.Run("Base case: receives Ping frame and routes to heartbeatCh", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		htc := setupKeepAliveTestState(localConn)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepAliveReciever()
		}()

		pingHeader := make([]byte, HEADERLENGTH)
		putHeader(FlagPing, 50, 0, pingHeader)
		remoteConn.Write(pingHeader)

		remoteConn.Close()
		wg.Wait()

		select {
		case flag := <-htc.heartbeatCh:
			if flag != FlagPing {
				t.Errorf("Routed flag mismatch: got 0x%02x, want 0x%02x", flag, FlagPing)
			}
		default:
			t.Errorf("heartbeatCh received no signal")
		}
	})

	t.Run("Edge/Intent case: packet size exceeding chunkSize causes ErrPacketTooLarge protocol violation", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		htc := setupKeepAliveTestState(localConn)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepAliveReciever()
		}()

		badHeader := make([]byte, HEADERLENGTH)
		putHeader(FlagUserData, 0, ChunkSizeDefault+10, badHeader)
		remoteConn.Write(badHeader)

		wg.Wait()

		select {
		case data := <-htc.tcpDataCh:
			if !errors.Is(data.err, ErrPacketTooLarge) {
				t.Errorf("Expected ErrPacketTooLarge, got %v", data.err)
			}
		default:
			t.Errorf("Expected error packet on tcpDataCh")
		}

		select {
		case <-htc.closeCh:
			// Success
		default:
			t.Errorf("Expected closeCh signal on protocol violation")
		}
	})

	t.Run("Edge case: unknown protocol flag causes ErrUnknownFlag", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		htc := setupKeepAliveTestState(localConn)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepAliveReciever()
		}()

		badHeader := make([]byte, HEADERLENGTH)
		putHeader(0xEE, 0, 0, badHeader)
		remoteConn.Write(badHeader)

		wg.Wait()

		select {
		case data := <-htc.tcpDataCh:
			if !errors.Is(data.err, ErrUnknownFlag) {
				t.Errorf("Expected ErrUnknownFlag wrap, got %v", data.err)
			}
		default:
			t.Errorf("Expected error on tcpDataCh")
		}
	})

	t.Run("Edge case: cleanly signals closeCh on connection EOF", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		htc := setupKeepAliveTestState(localConn)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepAliveReciever()
		}()

		remoteConn.Close()
		wg.Wait()

		select {
		case <-htc.closeCh:
			// Success
		default:
			t.Errorf("Expected closeCh signal on EOF")
		}
	})

	t.Run("Intent case: successfully routes valid FlagUserData to tcpDataCh", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		htc := setupKeepAliveTestState(localConn)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepAliveReciever()
		}()

		payload := []byte("standard user data")
		header := make([]byte, HEADERLENGTH)

		putHeader(FlagUserData, 0, uint32(len(payload)), header)
		remoteConn.Write(header)
		remoteConn.Write(payload)

		select {
		case data := <-htc.tcpDataCh:
			if data.err != nil {
				t.Errorf("Unexpected routing error: %v", data.err)
			}
			if string(data.msg[:data.len]) != "standard user data" {
				t.Errorf("Routed payload mismatch: got %s", string(data.msg[:data.len]))
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Timeout waiting for tcpDataCh to receive the valid payload")
		}

		remoteConn.Close()
		wg.Wait()
	})
}

func TestKeepaliveManager(t *testing.T) {
	t.Run("Intent case: responds to incoming FlagPing with FlagPong", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc := setupKeepAliveTestState(localConn)
		htc.config.KeepAliveInterval = 1 * time.Hour

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepaliveManager()
		}()

		htc.heartbeatCh <- FlagPing

		buf := make([]byte, HEADERLENGTH)
		n, err := remoteConn.Read(buf)

		if err != nil || n != HEADERLENGTH {
			t.Fatalf("Manager response error: n=%d, err=%v", n, err)
		}

		flag, _, _ := parseHeader(buf)
		if flag != FlagPong {
			t.Errorf("Manager ping response: got 0x%02x, want 0x%02x (FlagPong)", flag, FlagPong)
		}

		htc.closeCh <- struct{}{}
		wg.Wait()
	})

	t.Run("Intent case: flushes credits via FlagPing on processedCreditsNotifyCh signal", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc := setupKeepAliveTestState(localConn)
		htc.config.KeepAliveInterval = 1 * time.Hour

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepaliveManager()
		}()

		htc.flowControlData.processedCreditsNotifyCh <- struct{}{}

		buf := make([]byte, HEADERLENGTH)
		remoteConn.Read(buf)

		flag, _, _ := parseHeader(buf)
		if flag != FlagPing {
			t.Errorf("Credit flush ping: got 0x%02x, want 0x%02x (FlagPing)", flag, FlagPing)
		}

		htc.closeCh <- struct{}{}
		wg.Wait()
	})

	t.Run("Intent case: transmits FlagPing autonomously on ticker interval", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc := setupKeepAliveTestState(localConn)
		htc.config.KeepAliveInterval = 10 * time.Millisecond

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepaliveManager()
		}()

		buf := make([]byte, HEADERLENGTH)
		remoteConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := remoteConn.Read(buf)

		if err != nil || n != HEADERLENGTH {
			t.Fatalf("Ticker ping failed: n=%d, err=%v", n, err)
		}

		flag, _, _ := parseHeader(buf)
		if flag != FlagPing {
			t.Errorf("Manager ticker transmission: got 0x%02x, want 0x%02x (FlagPing)", flag, FlagPing)
		}

		htc.closeCh <- struct{}{}
		wg.Wait()
	})

	t.Run("Edge case: gracefully exits on closeCh signal", func(t *testing.T) {
		localConn, remoteConn := net.Pipe()
		defer remoteConn.Close()

		htc := setupKeepAliveTestState(localConn)
		htc.config.KeepAliveInterval = 1 * time.Hour

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			htc.keepaliveManager()
		}()

		htc.closeCh <- struct{}{}
		wg.Wait()

		htc.flowControlData.flowDataLock.Lock()
		isClosed := htc.flowControlData.isClosed
		htc.flowControlData.flowDataLock.Unlock()

		if !isClosed {
			t.Errorf("Expected manager to execute connection closure on closeCh signal")
		}
	})
}
