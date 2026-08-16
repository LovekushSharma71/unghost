package unghost

import (
	"errors"
	"net"
)

// Read reads data from the network connection into the provided buffer.
//
// [SPECIFICATION]
// - INTENT: Fulfill standard io.Reader contract by draining tcpLeftOverData first, then tcpDataCh.
// - PRECONDITION: Connection must be wrapped and initialized.
// - POSTCONDITION: If tcpLeftOverData > 0, reads entirely from leftover without blocking on channel.
// - POSTCONDITION: If read from channel is larger than len(b), excess bytes MUST be appended to tcpLeftOverData.
// - POSTCONDITION: processedCredits is incremented via addProcessedCredit().
// - NOTIFICATION: If processedCredits >= 25% of maxWindowSize, a non-blocking signal MUST be sent to processedCreditsNotifyCh.
// - EXPECTED ERRORS: Returns net.ErrClosed if tcpDataCh is closed.
// - EXPECTED ERRORS: Returns standard net.Conn errors.
func (c *HeartbeatTCP) Read(b []byte) (int, error) {

	// If we have leftovers from a previous read, use them first
	c.tcpReadLock.Lock()
	if len(c.tcpLeftOverData) > 0 {

		copiedCount := copy(b, c.tcpLeftOverData)
		// to reduce length and capacity
		remaining := copy(c.tcpLeftOverData, c.tcpLeftOverData[copiedCount:])
		c.tcpLeftOverData = c.tcpLeftOverData[:remaining]
		c.tcpReadLock.Unlock()
		return copiedCount, nil
	}
	c.tcpReadLock.Unlock()

	// get data from channel after previous data is read
	tcpMsg, ok := <-c.tcpDataCh
	if !ok {
		return 0, net.ErrClosed
	}

	// Copy what we can into the user's buffer
	copiedCount := copy(b, tcpMsg.msg[:tcpMsg.len])

	// If the channel gave us more data than the user asked for, stash the rest
	if copiedCount < tcpMsg.len {
		c.tcpReadLock.Lock()
		c.tcpLeftOverData = append(c.tcpLeftOverData, tcpMsg.msg[copiedCount:tcpMsg.len]...)
		c.tcpReadLock.Unlock()
	}

	c.flowControlData.flowDataLock.Lock()
	c.addProcessedCredit()
	isAboveThrushhold := float64(c.flowControlData.processedCredits) >= 0.25*float64(c.flowControlData.maxWindowSize)
	c.flowControlData.flowDataLock.Unlock()

	if isAboveThrushhold {
		select {
		case c.flowControlData.processedCreditsNotifyCh <- struct{}{}:
		default:
		}

	}

	if tcpMsg.msg != nil {
		putBufData(tcpMsg.msg)
	}
	return copiedCount, tcpMsg.err

}

// Write writes the provided buffer to the network connection over multiple chunked frames.
//
// [SPECIFICATION]
// - INTENT: Fulfill standard io.Writer contract while respecting maxPayloadSize and sendCredits flow control.
// - POSTCONDITION: Blocks via sndNotifyCond.Wait() if sendCredits == 0 until remote refunds credits.
// - POSTCONDITION: Slices data into chunks not exceeding maxPayloadSize or current sendCredits.
// - POSTCONDITION: Consumes credits prior to writing and packages processedCredits into the outgoing header.
// - ERROR RECOVERY: If network Write fails, consumed credits MUST be refunded and processedCredits MUST be restored.
// - EXPECTED ERRORS: Returns net.ErrClosed immediately if flowControlData.isClosed is true. Triggers closeCh on net.Error timeout.
// - EXPECTED ERRORS: Returns standard net.Conn.
func (c *HeartbeatTCP) Write(b []byte) (int, error) {

	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	ptr := 0
	frameBuf := getBufData().([]byte)
	defer putBufData(frameBuf)

	maxPayloadSize := int(ChunkSizeDefault) - HEADERLENGTH
	for ptr < len(b) {

		c.flowControlData.flowDataLock.Lock()
		for c.flowControlData.sendCredits == 0 && !c.flowControlData.isClosed {
			c.flowControlData.sndNotifyCond.Wait()
		}

		if c.flowControlData.isClosed {
			c.flowControlData.flowDataLock.Unlock()
			return ptr, net.ErrClosed
		}

		c.flowControlData.flowDataLock.Unlock()

		// header := syncpoolHeader.Get().([]byte)

		// prepare payload
		chunkSize := len(b) - ptr
		if chunkSize > maxPayloadSize {
			chunkSize = maxPayloadSize
		}
		c.flowControlData.flowDataLock.Lock()
		if chunkSize > int(c.flowControlData.sendCredits) {
			chunkSize = int(c.flowControlData.sendCredits)
		}
		creditsSent := c.flowControlData.processedCredits
		c.flowControlData.processedCredits = 0
		c.consumeCredits()

		putHeader(FlagUserData, creditsSent, uint32(chunkSize), frameBuf[:HEADERLENGTH])
		c.flowControlData.flowDataLock.Unlock()

		copy(frameBuf[HEADERLENGTH:], b[ptr:ptr+chunkSize])

		c.writeLock.Lock()
		// _, err := c.Conn.Write(header)
		// if err == nil {
		// 	_, err = c.Conn.Write(b[ptr : ptr+chunkSize])
		// }
		// bufs := net.Buffers{header, b[ptr : ptr+chunkSize]}
		// _, err := bufs.WriteTo(c.Conn)
		_, err := c.Conn.Write(frameBuf[:HEADERLENGTH+chunkSize])
		c.writeLock.Unlock()

		c.flowControlData.flowDataLock.Lock()
		if err != nil {

			c.refundCredits(ChunkSizeDefault)
			c.flowControlData.processedCredits += ChunkSizeDefault
		}
		c.flowControlData.flowDataLock.Unlock()
		// syncpoolHeader.Put(header)

		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// fmt.Println("timeout error:", err)
				select {
				case c.closeCh <- struct{}{}:
				default:
				}
			}
			return ptr, err
		}
		ptr += chunkSize

	}
	return len(b), nil
}
