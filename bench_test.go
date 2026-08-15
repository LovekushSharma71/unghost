package unghost

import (
	"io"
	"net"
	"sync"
	"testing"
)

// Benchmark connection setup / wrapper initialization overhead
func BenchmarkWrap(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		server, client := net.Pipe()
		htc, err := Wrap(client, KeepAliveConfig{}, 0)
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
	benchmarkThroughput(b, 4096)
}

// Benchmark end-to-end Write and Read throughput (64KB payloads) over a real TCP loopback
func BenchmarkReadWrite64K(b *testing.B) {
	benchmarkThroughput(b, 65536)
}

func BenchmarkReadWrite1MB(b *testing.B) {
	benchmarkThroughput(b, 1048576)
}

func BenchmarkReadWrite2MB(b *testing.B) {
	benchmarkThroughput(b, 2*1048576)
}

// Helper harness to benchmark stream performance for a given buffer size
func benchmarkThroughput(b *testing.B, chunkSize int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to start listener: %v", err)
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
		b.Fatalf("Failed to dial server: %v", err)
	}
	wg.Wait()

	if acceptErr != nil {
		b.Fatalf("Accept failed: %v", acceptErr)
	}

	htcServer, err := Wrap(serverConn, KeepAliveConfig{}, 0)
	if err != nil {
		b.Fatalf("Failed to wrap server connection: %v", err)
	}
	htcClient, err := Wrap(clientConn, KeepAliveConfig{}, 0)
	if err != nil {
		b.Fatalf("Failed to wrap client connection: %v", err)
	}

	defer htcServer.Close()
	defer htcClient.Close()

	payload := make([]byte, chunkSize)
	readBuf := make([]byte, chunkSize)

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()
	b.ReportAllocs()

	done := make(chan struct{})

	// Continuous reader loop
	go func() {
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(htcServer, readBuf); err != nil {
				break
			}
		}
		close(done)
	}()

	// Continuous writer loop
	for i := 0; i < b.N; i++ {
		if _, err := htcClient.Write(payload); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	<-done
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
		b.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	var serverConn, clientConn net.Conn
	var acceptErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		serverConn, acceptErr = listener.Accept()
	}()

	clientConn, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatalf("Failed to dial: %v", err)
	}
	wg.Wait()

	if acceptErr != nil {
		b.Fatalf("Accept failed: %v", acceptErr)
	}

	defer serverConn.Close()
	defer clientConn.Close()

	payload := make([]byte, chunkSize)
	readBuf := make([]byte, chunkSize)

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()
	b.ReportAllocs()

	done := make(chan struct{})

	go func() {
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(serverConn, readBuf); err != nil {
				break
			}
		}
		close(done)
	}()

	for i := 0; i < b.N; i++ {
		if _, err := clientConn.Write(payload); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	<-done
}
