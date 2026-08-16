
# unghost

`unghost` is a high-performance Go package that wraps standard `net.Conn` interfaces to provide built-in application-level keepalives and credit-based flow control. It is designed to ensure TCP connection reliability, prevent buffer overflow from slow readers, and automatically detect dead or "ghost" connections.

---

## 🚀 Features

* **Credit-Based Flow Control:** Manages in-flight data using a configurable window size, automatically throttling fast writers when the remote reader is slow to prevent memory exhaustion.
* **Built-in Heartbeats (Keep-Alive):** A dedicated background manager automatically sends `Ping` frames and responds with `Pong` frames to guarantee connection vitality, replacing standard timeout deadlines.
* **Zero-Allocation Data Paths:** Utilizes global `sync.Pool` instances for 9-byte protocol headers and payload chunks, drastically reducing garbage collection overhead.
* **Custom Frame Protocol:** Encapsulates stream data into strict frames using a lightweight 9-byte header: `[1B Flag | 4B Credits | 4B DataLength]`.
* **Resilience & Chaos Tested:** Heavily hardened against network chaos (high latency, jitter, hard resets, silent drops, and fragmentation) using Toxiproxy.

---

## 📦 Installation

```bash
go get github.com/LovekushSharma71/unghost
```

---

## 🛠️ Usage

To utilize `unghost`, simply wrap your existing `net.Conn` on both the client and the server.

```go
package main

import (
	"log"
	"net"
	"time"
	"github.com/yourusername/unghost"
)

func main() {
	rawConn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		log.Fatal(err)
	}

	// Define keepalive configuration
	config := unghost.KeepAliveConfig{
		KeepAliveInterval: 4 * time.Second,
		KeepAliveTimeout:  5 * time.Minute,
	}

	// Wrap the connection (0 maxWindowSize defaults to ~1MB)
	wrappedConn, err := unghost.Wrap(rawConn, config, 0)
	if err != nil {
		log.Fatal("Failed to wrap connection:", err)
	}
	defer wrappedConn.Close()

	// Use wrappedConn exactly like a standard net.Conn
	wrappedConn.Write([]byte("Hello Unghost!"))
}
```

> **Note:** Calling `SetDeadline`, `SetReadDeadline`, or `SetWriteDeadline` on an `unghost` connection will return an `ErrDeadlinesDisabled` error, as deadlines are managed internally by the keep-alive framework to prevent conflicts.

---

## 📊 Benchmarks

`unghost` adds minimal overhead to standard TCP while providing robust session management.

**Environment Details:**

* **OS:** Windows
* **Architecture:** amd64
* **CPU:** AMD Ryzen 7 9700X 8-Core Processor

| Benchmark                          | Iterations | Time/op       | Throughput    | Bytes/op    | Allocs/op     |
| ---------------------------------- | ---------- | ------------- | ------------- | ----------- | ------------- |
| **BenchmarkWrap-16**         | 157,623    | 7,328 ns/op   | -             | 10,879 B/op | 69 allocs/op  |
| **BenchmarkReadWrite4K-16**  | 105,843    | 11,214 ns/op  | 365.24 MB/s   | 164 B/op    | 5 allocs/op   |
| **BenchmarkReadWrite64K-16** | 42,998     | 29,188 ns/op  | 2,245.30 MB/s | 118 B/op    | 5 allocs/op   |
| **BenchmarkReadWrite1MB-16** | 4,480      | 525,168 ns/op | 1,996.65 MB/s | 5,907 B/op  | 178 allocs/op |
| **BenchmarkReadWrite2MB-16** | 1,340      | 872,931 ns/op | 2,402.42 MB/s | 1,714 B/op  | 56 allocs/op  |
| **BenchmarkRawTCP4K-16**     | 173,121    | 6,987 ns/op   | 586.24 MB/s   | 100 B/op    | 2 allocs/op   |
| **BenchmarkRawTCP64K-16**    | 101,430    | 12,451 ns/op  | 5,263.39 MB/s | 0 B/op      | 0 allocs/op   |
| **BenchmarkRawTCP1MB-16**    | 9,578      | 209,334 ns/op | 5,009.12 MB/s | 2,110 B/op  | 62 allocs/op  |
| **BenchmarkRawTCP2MB-16**    | 5,206      | 588,004 ns/op | 3,566.56 MB/s | 4,353 B/op  | 128 allocs/op |

*Execution time: 22.294s*

---

## 🧪 Testing & Chaos Engineering

To run the full suite of integration tests and simulate adverse network conditions, `unghost` leverages [Toxiproxy](https://github.com/Shopify/toxiproxy).

Ensure you have the Toxiproxy Docker container running before executing the integration tests:

```bash
docker run -d --name toxiproxy -p 8474:8474 -p 8475:8475 ghcr.io/shopify/toxiproxy

```

Run tests with the integration build tag:

```bash
go test -v -tags=integration ./...

```
