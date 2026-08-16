package unghost

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type mockErrConn struct {
	net.Conn
	writeErr error
}

func (m *mockErrConn) Write(b []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return len(b), nil
}

type mockTimeoutErr struct{}

func (e *mockTimeoutErr) Error() string   { return "timeout" }
func (e *mockTimeoutErr) Timeout() bool   { return true }
func (e *mockTimeoutErr) Temporary() bool { return true }

func setupIOTestState(conn net.Conn) *HeartbeatTCP {
	maxWindowSize := uint32(4096)

	htc := &HeartbeatTCP{
		Conn: conn,
		flowControlData: flowControlData{
			sendCredits:              maxWindowSize,
			processedCredits:         0,
			processedCreditsNotifyCh: make(chan struct{}, 1),
			maxWindowSize:            maxWindowSize,
		},
		tcpLeftOverData: make([]byte, 0, maxWindowSize),
		tcpDataCh:       make(chan tcpReadData, 10),
		closeCh:         make(chan struct{}, 1),
	}

	htc.flowControlData.sndNotifyCond = sync.NewCond(&htc.flowControlData.flowDataLock)

	return htc
}

func TestRead(t *testing.T) {
	t.Run("Base case: read entirely from leftover data", func(t *testing.T) {
		htc := setupIOTestState(nil)
		expectedData := []byte("leftover data")
		htc.tcpLeftOverData = append([]byte{}, expectedData...)

		buf := make([]byte, 32)
		n, err := htc.Read(buf)

		if err != nil || n != len(expectedData) || !bytes.Equal(buf[:n], expectedData) {
			t.Errorf("Read leftover mismatch: got %d bytes %s, err: %v", n, string(buf[:n]), err)
		}
		if len(htc.tcpLeftOverData) != 0 {
			t.Errorf("tcpLeftOverData not drained completely")
		}
	})

	t.Run("Edge case: leftover data larger than caller buffer (partial drain)", func(t *testing.T) {
		htc := setupIOTestState(nil)
		htc.tcpLeftOverData = []byte("1234567890")

		buf := make([]byte, 4)
		n, err := htc.Read(buf)

		if err != nil || n != 4 || !bytes.Equal(buf, []byte("1234")) {
			t.Errorf("Partial read from leftover failed: got %s, n=%d", string(buf[:n]), n)
		}
		if string(htc.tcpLeftOverData) != "567890" {
			t.Errorf("Leftover data remaining mismatch: got %s, want 567890", string(htc.tcpLeftOverData))
		}
	})

	t.Run("Edge case: channel closed returns net.ErrClosed", func(t *testing.T) {
		htc := setupIOTestState(nil)
		close(htc.tcpDataCh)

		buf := make([]byte, 10)
		n, err := htc.Read(buf)

		if !errors.Is(err, net.ErrClosed) || n != 0 {
			t.Errorf("Closed channel: got err %v, n=%d, want net.ErrClosed", err, n)
		}
	})

	t.Run("Intent case: notifies processedCreditsNotifyCh when credits threshold >= 25%", func(t *testing.T) {
		htc := setupIOTestState(nil)
		htc.flowControlData.processedCredits = 0

		msgData := []byte("payload")
		msgBuf := getBufData().([]byte)
		copy(msgBuf, msgData)

		htc.tcpDataCh <- tcpReadData{msg: msgBuf, len: len(msgData)}

		buf := make([]byte, 32)
		_, err := htc.Read(buf)

		if err != nil {
			t.Fatalf("Unexpected read error: %v", err)
		}

		select {
		case <-htc.flowControlData.processedCreditsNotifyCh:
			// Success
		default:
			t.Errorf("Expected notification on processedCreditsNotifyCh at >= 25%% threshold")
		}
	})
}

func TestWrite(t *testing.T) {
	t.Run("Base case: write success", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		htc := setupIOTestState(server)
		writeData := []byte("test payload")

		var writeErr error
		var writtenCount int
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			writtenCount, writeErr = htc.Write(writeData)
		}()

		expectedTotalLen := HEADERLENGTH + len(writeData)
		resultBuf := make([]byte, expectedTotalLen)
		n, err := io.ReadFull(client, resultBuf)
		wg.Wait()

		if err != nil || writeErr != nil || writtenCount != len(writeData) || n != expectedTotalLen {
			t.Fatalf("Write base case failed: written=%d, read=%d, err=%v, writeErr=%v", writtenCount, n, err, writeErr)
		}

		flag, _, dataLen := parseHeader(resultBuf[:HEADERLENGTH])
		if flag != FlagUserData || dataLen != uint32(len(writeData)) {
			t.Errorf("Header invalid: flag=0x%02x, dataLen=%d", flag, dataLen)
		}
	})

	t.Run("Edge case: write fails immediately when session is marked closed", func(t *testing.T) {
		htc := setupIOTestState(nil)
		htc.flowControlData.isClosed = true

		n, err := htc.Write([]byte("data"))

		if !errors.Is(err, net.ErrClosed) || n != 0 {
			t.Errorf("Closed session write: got n=%d, err=%v, want net.ErrClosed", n, err)
		}
	})

	t.Run("Logic/Intent case: multi-chunking payload exceeding maxPayloadSize", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		htc := setupIOTestState(server)
		htc.flowControlData.sendCredits = MaxWindowSize

		// 70,000 bytes payload exceeds maxPayloadSize (65,526)
		largePayload := make([]byte, 70000)

		var writtenCount int
		var writeErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			writtenCount, writeErr = htc.Write(largePayload)
		}()

		// Frame 1: 65,526 payload + 9 header = 65,535
		buf1 := make([]byte, 65535)
		io.ReadFull(client, buf1)

		// Frame 2: 4,474 payload + 9 header = 4,483
		buf2 := make([]byte, 4483)
		io.ReadFull(client, buf2)

		wg.Wait()

		if writeErr != nil || writtenCount != 70000 {
			t.Errorf("Chunking failure: written=%d, err=%v", writtenCount, writeErr)
		}
		_, _, len1 := parseHeader(buf1[:HEADERLENGTH])
		_, _, len2 := parseHeader(buf2[:HEADERLENGTH])

		if len1 != 65526 || len2 != 4474 {
			t.Errorf("Chunk lengths mismatch: got (%d, %d), want (65526, 4474)", len1, len2)
		}
	})

	t.Run("Intent case: error recovery restores credits and processed credits on network error", func(t *testing.T) {
		mockConn := &mockErrConn{writeErr: errors.New("network failure")}
		htc := setupIOTestState(mockConn)

		initialCredits := htc.flowControlData.sendCredits
		initialProcessed := htc.flowControlData.processedCredits

		_, err := htc.Write([]byte("fail payload"))

		if err == nil {
			t.Fatalf("Expected write error, got nil")
		}

		if htc.flowControlData.sendCredits != initialCredits {
			t.Errorf("Credits not restored on error: got %d, want %d", htc.flowControlData.sendCredits, initialCredits)
		}
		if htc.flowControlData.processedCredits < initialProcessed {
			t.Errorf("Processed credits not restored on error: got %d", htc.flowControlData.processedCredits)
		}
	})

	t.Run("Edge case: net.Error timeout triggers closeCh signal", func(t *testing.T) {
		mockConn := &mockErrConn{writeErr: &mockTimeoutErr{}}
		htc := setupIOTestState(mockConn)

		_, err := htc.Write([]byte("timeout data"))

		if err == nil {
			t.Fatalf("Expected timeout error, got nil")
		}

		select {
		case <-htc.closeCh:
			// Success
		default:
			t.Errorf("Expected closeCh signal on net.Error timeout")
		}
	})

	t.Run("Intent case: Write blocks when sendCredits is 0 and unblocks on refund", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		htc := setupIOTestState(server)

		htc.flowControlData.flowDataLock.Lock()
		htc.flowControlData.sendCredits = 0
		htc.flowControlData.flowDataLock.Unlock()

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			_, _ = htc.Write([]byte("delayed data"))
		}()

		time.Sleep(50 * time.Millisecond)

		htc.flowControlData.flowDataLock.Lock()
		htc.refundCredits(1024)
		htc.flowControlData.sndNotifyCond.Broadcast()
		htc.flowControlData.flowDataLock.Unlock()

		expectedTotalLen := HEADERLENGTH + 12
		buf := make([]byte, expectedTotalLen)
		n, err := io.ReadFull(client, buf)
		wg.Wait()

		if err != nil || n != expectedTotalLen {
			t.Errorf("Delayed write failed: n=%d, err=%v", n, err)
		}
	})
}
