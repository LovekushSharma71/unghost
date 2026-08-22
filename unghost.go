package unghost

import (
	"errors"
	"net"
	"sync"
	"time"
)

var (
	ErrConnectionNil     = errors.New("unghost: connection cannot be nil")
	ErrPacketTooLarge    = errors.New("unghost: protocol violation, packet size exceeds chunk size")
	ErrInvalidHeartbeat  = errors.New("unghost: keepalive error, cannot send non-heartbeat data")
	ErrUnknownFlag       = errors.New("unghost: unknown protocol flag received")
	ErrDeadlinesDisabled = errors.New("unghost: deadlines are managed internally by keep-alive")
)

type KeepAliveConfig struct {

	// KeepAliveInterval is how often to send a heartbeat to the remote
	KeepAliveInterval time.Duration

	// KeepAliveTimeout is how long the session will be closed if no data has arrived
	KeepAliveTimeout time.Duration
}

type HeartbeatTCP struct {
	net.Conn

	streamLock sync.Mutex

	config KeepAliveConfig

	flowControlData flowControlData

	tcpReadLock sync.Mutex

	tcpDataCh chan tcpReadData

	tcpLeftOverData []byte

	heartbeatCh chan byte

	// use to close channel safely to check if session is closed
	closeOnce sync.Once

	// send close signal
	closeCh chan struct{}

	isClosedCh chan struct{}

	writeLock sync.Mutex
}

// Wrap initializes a raw net.Conn with the HeartbeatTCP framework.
//
// [SPECIFICATION]
// - INTENT: Initialize all necessary channels, pools, locks, and background goroutines for the wrapper session.
// - PRECONDITION: c MUST NOT be nil.
// - POSTCONDITION: If config values are 0, they strictly fall back to defaultKeepAliveInterval and defaultKeepAliveTimeout.
// - POSTCONDITION: If window sizes are 0, they strictly fall back to MaxWindowSize and ChunkSizeDefault.
// - POSTCONDITION: Starts exactly two background routines: keepAliveReciever and keepaliveManager.
// - EXPECTED ERRORS: Returns non-nil error if input net.Conn is nil.
func Wrap(c net.Conn, config KeepAliveConfig, maxWindowSize uint32) (*HeartbeatTCP, error) {

	if c == nil {
		// fmt.Println("wrap error: connection cannot be nil")
		return nil, ErrConnectionNil
	}
	if config.KeepAliveInterval == 0 {
		config.KeepAliveInterval = defaultKeepAliveInterval
	}
	if config.KeepAliveTimeout == 0 {
		config.KeepAliveTimeout = defaultKeepAliveTimeout
	}

	if maxWindowSize == 0 {
		maxWindowSize = MaxWindowSize
	}

	htc := HeartbeatTCP{
		Conn: c,
		config: KeepAliveConfig{
			KeepAliveInterval: config.KeepAliveInterval,
			KeepAliveTimeout:  config.KeepAliveTimeout,
		},
		flowControlData: flowControlData{
			sendCredits:              maxWindowSize,
			processedCredits:         0,
			processedCreditsNotifyCh: make(chan struct{}, 1),
			flowDataLock:             sync.Mutex{},
			maxWindowSize:            maxWindowSize,
		},
		tcpReadLock:     sync.Mutex{},
		tcpLeftOverData: make([]byte, 0, 4096),
		tcpDataCh:       make(chan tcpReadData, 32),
		heartbeatCh:     make(chan byte, 1),
		closeCh:         make(chan struct{}, 1),
		writeLock:       sync.Mutex{},
		isClosedCh:      make(chan struct{}),
	}

	htc.flowControlData.sndNotifyCond = sync.NewCond(&htc.flowControlData.flowDataLock)
	htc.Conn.SetDeadline(time.Now().Add(config.KeepAliveTimeout))

	go htc.keepAliveReciever()
	go htc.keepaliveManager()

	return &htc, nil
}

// Close gracefully terminates the connection and unlocks all waiting routines.
//
// [SPECIFICATION]
// - INTENT: Ensure idempotent teardown of the connection state.
// - POSTCONDITION: Safely sets flowControlData.isClosed to true and triggers underlying net.Conn.Close().
// - POSTCONDITION: Broadcasts sndNotifyCond to prevent writer goroutine deadlocks.
// - INVARIANT: Can be called concurrently without data races due to sync.Once.
func (c *HeartbeatTCP) Close() error {

	c.closeOnce.Do(func() {

		c.Conn.Close()
		if c.isClosedCh != nil {
			close(c.isClosedCh)
		}
		c.flowControlData.flowDataLock.Lock()
		c.flowControlData.isClosed = true
		c.flowControlData.sndNotifyCond.Broadcast()
		c.flowControlData.flowDataLock.Unlock()

	})

	return nil
}

// SetDeadline overrides standard deadline behaviour as it conflicts with keepalive.
//
// [SPECIFICATION]
// - INTENT: Prevent caller from corrupting internal connection timeouts.
// - EXPECTED ERRORS: Always returns ErrDeadlinesDisabled.
func (c *HeartbeatTCP) SetDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}

// SetReadDeadline overrides standard deadline behaviour as it conflicts with keepalive.
//
// [SPECIFICATION]
// - INTENT: Prevent caller from corrupting internal connection timeouts.
// - EXPECTED ERRORS: Always returns ErrDeadlinesDisabled.
func (c *HeartbeatTCP) SetReadDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}

// SetWriteDeadline overrides standard deadline behaviour as it conflicts with keepalive.
//
// [SPECIFICATION]
// - INTENT: Prevent caller from corrupting internal connection timeouts.
// - EXPECTED ERRORS: Always returns ErrDeadlinesDisabled.
func (c *HeartbeatTCP) SetWriteDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}
