package unghost

import (
	"io"
	"net"
	"sync"
	"testing"

	"go.uber.org/goleak"
)

func BenchmarkWrap(b *testing.B) {
	b.ReportAllocs()

	defer goleak.VerifyNone(b, goleak.IgnoreCurrent())

	b.ResetTimer()
	b.StopTimer()

	for i := 0; i < b.N; i++ {
		server, client := net.Pipe()

		b.StartTimer()
		htc, err := Wrap(client, KeepAliveConfig{}, 0)
		b.StopTimer()

		if err != nil {
			_ = server.Close()
			_ = client.Close()
			b.Fatalf("Wrap failed: %v", err)
		}
		_ = htc.Close()
		_ = server.Close()
	}
}

// Benchmark end-to-end Write and Read throughput (4KB payloads) over a real TCP loopback
func BenchmarkReadWrite4K(b *testing.B) {
	defer goleak.VerifyNone(b,goleak.IgnoreCurrent())
	benchmarkThroughput(b, 4096)
}

// Benchmark end-to-end Write and Read throughput (64KB payloads) over a real TCP loopback
func BenchmarkReadWrite64K(b *testing.B) {
	defer goleak.VerifyNone(b,goleak.IgnoreCurrent())
	benchmarkThroughput(b, 65536)
}

func BenchmarkReadWrite1MB(b *testing.B) {
	defer goleak.VerifyNone(b,goleak.IgnoreCurrent())
	benchmarkThroughput(b, 1048576)
}

func BenchmarkReadWrite2MB(b *testing.B) {
	defer goleak.VerifyNone(b,goleak.IgnoreCurrent())
	benchmarkThroughput(b, 2*1048576)
}


func benchmarkThroughput(b *testing.B, chunkSize int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var serverConn net.Conn
	var acceptErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, acceptErr = listener.Accept()
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatalf("failed to dial: %v", err)
	}
	wg.Wait()
	if acceptErr != nil {
		b.Fatalf("accept failed: %v", acceptErr)
	}

	htcServer, err := Wrap(serverConn, KeepAliveConfig{}, 0)
	if err != nil {
		b.Fatalf("wrap server failed: %v", err)
	}
	htcClient, err := Wrap(clientConn, KeepAliveConfig{}, 0)
	if err != nil {
		b.Fatalf("wrap client failed: %v", err)
	}

	payload := make([]byte, chunkSize)
	readBuf := make([]byte, chunkSize)

	// --- Measured region starts here ---
	b.SetBytes(int64(chunkSize))
	b.ReportAllocs()
	b.ResetTimer()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(htcServer, readBuf); err != nil {
				b.Logf("read %d failed: %v", i, err) // don't swallow it silently
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		if _, err := htcClient.Write(payload); err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}

	<-done

	// --- Measured region ends here ---
	b.StopTimer()

	_ = htcServer.Close()
	_ = htcClient.Close()
}

func BenchmarkRawTCP4K(b *testing.B) {
	benchmarkRawTCP(b, 4096)
}

func BenchmarkRawTCP64K(b *testing.B) {
	benchmarkRawTCP(b, 65536)
}

func BenchmarkRawTCP1MB(b *testing.B) {
	benchmarkRawTCP(b, 1048576)
}

func BenchmarkRawTCP2MB(b *testing.B) {
	benchmarkRawTCP(b, 2*1048576)
}


func benchmarkRawTCP(b *testing.B, chunkSize int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	var serverConn net.Conn
	var acceptErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, acceptErr = listener.Accept()
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatalf("failed to dial: %v", err)
	}
	wg.Wait()
	if acceptErr != nil {
		b.Fatalf("accept failed: %v", acceptErr)
	}

	payload := make([]byte, chunkSize)
	readBuf := make([]byte, chunkSize)

	// --- Measured region starts here ---
	b.SetBytes(int64(chunkSize)) // lets -benchmem report MB/s correctly
	b.ReportAllocs()
	b.ResetTimer()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(serverConn, readBuf); err != nil {
				b.Logf("read %d failed: %v", i, err) // don't swallow it silently
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		if _, err := clientConn.Write(payload); err != nil {
			b.Fatalf("write %d failed: %v", i, err)
		}
	}

	<-done

	// --- Measured region ends here ---
	// StopTimer BEFORE the deferred Close() calls fire, so their
	// teardown cost (which you already flagged, several messages back)
	// doesn't leak into ns/op or allocs/op.
	b.StopTimer()

	_ = serverConn.Close()
	_ = clientConn.Close()
}
