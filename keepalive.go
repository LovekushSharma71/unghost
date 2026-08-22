package unghost

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	defaultKeepAliveInterval time.Duration = 4 * time.Second
	defaultKeepAliveTimeout  time.Duration = 5 * time.Minute
)

// keepAliveReciever is an infinite loop that reads incoming frames from the network.
//
// [SPECIFICATION]
// - INTENT: Parse incoming headers, route data payloads, process credit refunds, and filter heartbeats.
// - POSTCONDITION: Header refunds are always processed via refundCredits() and sndNotifyCond is broadcasted if credits exceed chunkSize.
// - POSTCONDITION: FlagPing/FlagPong are non-blockingly routed to heartbeatCh.
// - POSTCONDITION: FlagUserData reads the payload and non-blockingly routes it to tcpDataCh.
// - POSTCONDITION: Read deadlines are continually extended by KeepAliveTimeout upon successful reads.
// - EXPECTED ERRORS: If FlagUserData datalength > chunkSize, raises ErrPacketTooLarge, signals closeCh, and exits loop.
// - EXPECTED ERRORS: If an unknown flag is received, raises ErrUnknownFlag, signals closeCh, and exits loop.
// - SIDE-EFFECTS: Triggers closeCh and terminates on io.EOF, net.ErrClosed, or timeout.

func (c *HeartbeatTCP) keepAliveReciever() {

	defer close(c.tcpDataCh)
	headerBuf := make([]byte, HEADERLENGTH)

	for {

		// read for flag
		_, err := io.ReadFull(c.Conn, headerBuf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				select {
				case c.closeCh <- struct{}{}:
				default:
				}
			}
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {

				select {
				case c.closeCh <- struct{}{}:
				default:
				}
				break
			}

			select {
			case c.tcpDataCh <- tcpReadData{err: err}:
			case <-c.isClosedCh:
				return
			}

			continue
		}

		flag, refundedCredits, datalength := parseHeader(headerBuf)

		c.flowControlData.flowDataLock.Lock()
		c.refundCredits(refundedCredits)
		c.flowControlData.flowDataLock.Unlock()

		// just for safety as checking using 0 will do the same
		if c.flowControlData.sendCredits >= ChunkSizeDefault {
			c.flowControlData.sndNotifyCond.Broadcast()
		}

		if flag == FlagPing || flag == FlagPong {
			select {
			case c.heartbeatCh <- flag:
			default:
				// Safely drop the heartbeat if the manager is dead or blocked, and no other goroutine will write on it so it should be safe
			}
		} else if flag == FlagUserData {
			if datalength > ChunkSizeDefault {
				// fmt.Printf("keepAliveReciever: protocol voilation Invalid packet size: 0x%02x should not exceede 0x%02x\n", datalength, ChunkSizeDefault)
				select {
				case c.tcpDataCh <- tcpReadData{msg: nil, len: 0, err: ErrPacketTooLarge}:
				case <-c.isClosedCh:
				}
				// c.tcpDataCh <- tcpReadData{msg: nil, len: 0, err: ErrPacketTooLarge}
				select {
				case c.closeCh <- struct{}{}:
				default:
				}
				return
			} else {
				// as one packet will always have data less than equal default chunk size
				b := getBufData().([]byte)
				n, err := io.ReadFull(c.Conn, b[:datalength])
				// not locking cause concurrent read is prevented by channel
				select {
				case c.tcpDataCh <- tcpReadData{msg: b, len: n, err: err}:
				case <-c.isClosedCh:
					// If we were allocated a buffer 'b', make sure to return it to the pool if we abort!
					if b != nil {
						putBufData(b)
					}
					return
				}
			}
		} else {
			//close connection due to safety
			select {
			case c.tcpDataCh <- tcpReadData{err: fmt.Errorf("%w: 0x%02x", ErrUnknownFlag, flag)}:
			case <-c.isClosedCh:
				return
			}

			// fmt.Printf("keepAliveReciever: Unknown flag: 0x%02x\n", flag)
			select {
			case c.closeCh <- struct{}{}:
			default:
			}
			return

		}
		c.Conn.SetDeadline(time.Now().Add(c.config.KeepAliveTimeout))

	}
}

// keepAliveSender transmits a control frame (Ping/Pong) over the network.
//
// [SPECIFICATION]
// - INTENT: Construct and write an empty payload header containing the heartbeat flag and current processedCredits.
// - PRECONDITION: heartbeatFlag MUST be either FlagPing or FlagPong.
// - POSTCONDITION: processedCredits is temporarily cleared during send.
// - ERROR RECOVERY: If network write fails, processedCredits MUST be fully restored.
// - EXPECTED ERRORS: Returns ErrInvalidHeartbeat if an invalid flag is provided. Triggers closeCh on timeout.
func (c *HeartbeatTCP) keepAliveSender(heartbeatFlag byte) error {

	buff := getBufHeader().([]byte)
	defer putBufHeader(buff)

	if !(heartbeatFlag == FlagPing || heartbeatFlag == FlagPong) {
		// fmt.Println("keepalive error: cannot send non heartbeat data")
		return ErrInvalidHeartbeat
	}

	// write for heartbeat
	c.flowControlData.flowDataLock.Lock()
	creditsSent := c.flowControlData.processedCredits
	c.flowControlData.processedCredits = 0
	putHeader(heartbeatFlag, creditsSent, 0, buff)
	c.flowControlData.flowDataLock.Unlock()

	c.writeLock.Lock()
	_, err := c.Conn.Write(buff)
	c.writeLock.Unlock()

	c.flowControlData.flowDataLock.Lock()
	if err != nil {

		//refund failed
		c.flowControlData.processedCredits += creditsSent
	}
	c.flowControlData.flowDataLock.Unlock()

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// fmt.Println("timeout error:", err)
		select {
		case c.closeCh <- struct{}{}:
		default:
		}

	}

	if err != nil {
		return fmt.Errorf("unghost keepalive write failed: %w", err)
	}
	return nil
}

// keepaliveManager maintains the connection lifecycle via regular pings and responding to events.
//
// [SPECIFICATION]
// - INTENT: Manage ticker-based pings, respond to pings with pongs, and push immediate pings when processed credits exceed thresholds.
// - POSTCONDITION: On receiving FlagPing on heartbeatCh, keepAliveSender(FlagPong) is immediately invoked.
// - POSTCONDITION: On ticker interval, keepAliveSender(FlagPing) is invoked.
// - POSTCONDITION: On processedCreditsNotifyCh signal, keepAliveSender(FlagPing) is immediately invoked to flush credits.
// - SIDE-EFFECTS: Terminates on closeCh signal or if any keepAliveSender network action returns EOF/Closed.
func (c *HeartbeatTCP) keepaliveManager() {
	ticker := time.NewTicker(c.config.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case flag, ok := <-c.heartbeatCh:
			if !ok {
				return
			}
			if flag == FlagPing {
				// fmt.Println(c.Conn.LocalAddr(), "PING")
				err := c.keepAliveSender(FlagPong)
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {

					select {
					case c.closeCh <- struct{}{}:
					default:
					}
					return
				}
			} else {
				// fmt.Println(c.Conn.LocalAddr(), "PONG")
			}
		case <-c.closeCh:
			c.Close()
			return
		case <-ticker.C:
			err := c.keepAliveSender(FlagPing)
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {

				select {
				case c.closeCh <- struct{}{}:
				default:
				}
				return
			}
		case <-c.flowControlData.processedCreditsNotifyCh:
			err := c.keepAliveSender(FlagPing)

			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {

				select {
				case c.closeCh <- struct{}{}:
				default:
				}
				return
			}
		case <-c.isClosedCh:
			return
		}
	}
}
