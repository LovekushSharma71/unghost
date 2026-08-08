package unghost

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

//TODO: add flow control in this code to add reduce infinite spong for this and reduce backpressure

var netErr net.Error

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

	//flow control
	flowControlData flowControlData

	//read lock
	tcpReadLock sync.Mutex

	// tcp data channel
	// should make it buffered
	tcpDataCh chan tcpReadData

	// leftover tcp read data
	tcpLeftOverData []byte

	// putting interface rn will put proper stuff later
	heartbeatCh chan byte

	// use to close channel safely to check if session is closed
	closeOnce sync.Once

	// send close signal to all go routines using this session
	closeCh chan struct{}

	// rn using general lock but for better readability will make is more specific in future like close lock or ping pong lock can also have sync atomic based on need
	writeLock sync.Mutex

	// sync pool for each connection
	datapool *sync.Pool
}

func Wrap(c net.Conn, config KeepAliveConfig, maxWindowSize uint32, chunkSize uint32) (*HeartbeatTCP, error) {

	if c == nil {
		fmt.Println("wrap error: connection cannot be nil")
		return nil, errors.New("connection not provided")
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
	if chunkSize == 0 {
		chunkSize = ChunkSizeDefault
	}

	var syncpool = sync.Pool{
		New: func() any {
			return make([]byte, chunkSize)
		},
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
			chunkSize:                chunkSize,
			maxWindowSize:            maxWindowSize,
		},
		tcpReadLock:     sync.Mutex{},
		tcpLeftOverData: make([]byte, 0, maxWindowSize),
		tcpDataCh:       make(chan tcpReadData, 1024),
		heartbeatCh:     make(chan byte),
		closeCh:         make(chan struct{}, 1),
		writeLock:       sync.Mutex{},
		datapool:        &syncpool,
	}

	htc.flowControlData.sndNotifyCond = sync.NewCond(&htc.flowControlData.flowDataLock)
	htc.Conn.SetDeadline(time.Now().Add(config.KeepAliveTimeout))

	go htc.keepAliveReciever()
	go htc.keepaliveManager()

	return &htc, nil
}

// add sync once for better logic
func (c *HeartbeatTCP) Close() error {

	c.closeOnce.Do(func() {

		c.Conn.Close()
		c.flowControlData.flowDataLock.Lock()
		c.flowControlData.isClosed = true
		c.flowControlData.sndNotifyCond.Broadcast()
		c.flowControlData.flowDataLock.Unlock()

	})

	return nil
}

var ErrDeadlinesDisabled = errors.New("unghost: deadlines are managed internally by keep-alive")

func (c *HeartbeatTCP) SetDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}

func (c *HeartbeatTCP) SetReadDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}

func (c *HeartbeatTCP) SetWriteDeadline(t time.Time) error {
	return ErrDeadlinesDisabled
}
