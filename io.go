package unghost

import (
	"errors"
	"fmt"
	"net"
)

func (c *heartbeatTCP) Read(b []byte) (int, error) {

	// If we have leftovers from a previous read, use them first
	c.tcpReadLock.Lock()
	if len(c.tcpLeftOverData) > 0 {

		copiedCount := copy(b, c.tcpLeftOverData)
		c.tcpLeftOverData = c.tcpLeftOverData[copiedCount:]
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
	c.flowControlData.flowDataLock.Unlock()

	if float64(c.flowControlData.processedCredits) >= 0.25*float64(MaxWindowSize) {
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

func (c *heartbeatTCP) Write(b []byte) (int, error) {

	c.streamLock.Lock()
	defer c.streamLock.Unlock()

	ptr := 0

	maxPayloadSize := int(ChunkSizeDefault) - 9
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

		header := syncpoolHeader.Get().([]byte)
		var buff net.Buffers
		// prepare payload
		chunkSize := len(b) - ptr
		if chunkSize > maxPayloadSize {
			chunkSize = maxPayloadSize
		}
		if chunkSize > int(c.flowControlData.sendCredits) {
			chunkSize = int(c.flowControlData.sendCredits)
		}

		c.flowControlData.flowDataLock.Lock()
		creditsSent := c.flowControlData.processedCredits
		c.flowControlData.processedCredits = 0
		c.consumeCredits()

		putHeader(FlagUserData, creditsSent, uint32(chunkSize), header)
		buff = net.Buffers{header, b[ptr : ptr+chunkSize]}
		c.flowControlData.flowDataLock.Unlock()

		c.writeLock.Lock()
		_, err := buff.WriteTo(c.Conn)
		c.writeLock.Unlock()

		c.flowControlData.flowDataLock.Lock()
		if err != nil {

			c.refundCredits(ChunkSizeDefault)
			c.flowControlData.processedCredits += ChunkSizeDefault
		}
		c.flowControlData.flowDataLock.Unlock()
		syncpoolHeader.Put(header)

		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				fmt.Println("timeout error:", err)
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
