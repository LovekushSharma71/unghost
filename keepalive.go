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

func (c *HeartbeatTCP) keepAliveReciever() {

	defer close(c.tcpDataCh)
	headerBuf := make([]byte, HEADERLENGTH)

	for {

		// read for flag
		_, err := io.ReadFull(c.Conn, headerBuf)
		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				fmt.Println("timeout error:", err)
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

			c.tcpDataCh <- tcpReadData{err: err}
			continue
		}

		flag, refundedCredits, datalength := parseHeader(headerBuf)

		c.flowControlData.flowDataLock.Lock()
		c.refundCredits(refundedCredits)
		c.flowControlData.flowDataLock.Unlock()

		// just for safety as checking using 0 will do the same
		if c.flowControlData.sendCredits >= c.flowControlData.chunkSize {
			c.flowControlData.sndNotifyCond.Broadcast()
		}

		if flag == FlagPing || flag == FlagPong {
			select {
			case c.heartbeatCh <- flag:
			default:
				// Safely drop the heartbeat if the manager is dead or blocked, and no other goroutine will write on it so it should be safe
			}
		} else if flag == FlagUserData {
			if datalength > c.flowControlData.chunkSize {
				fmt.Printf("keepAliveReciever: protocol voilation Invalid packet size: 0x%02x should not exceede 0x%02x\n", datalength, c.flowControlData.chunkSize)
				c.tcpDataCh <- tcpReadData{msg: []byte{}, len: 0, err: errors.ErrUnsupported}
				select {
				case c.closeCh <- struct{}{}:
				default:
				}
				return
			} else {
				// as one packet will always have data less than equal default chunk size
				b := c.getBufData().([]byte)
				n, err := io.ReadFull(c.Conn, b[:datalength])
				// not locking cause concurrent read is prevented by channel
				c.tcpDataCh <- tcpReadData{msg: b, len: n, err: err}
			}
		} else {
			fmt.Printf("keepAliveReciever: Unknown flag: 0x%02x\n", flag)
		}
		c.Conn.SetDeadline(time.Now().Add(c.config.KeepAliveTimeout))

	}
}

func (c *HeartbeatTCP) keepAliveSender(heartbeatFlag byte) error {

	buff := getBufHeader().([]byte)
	defer putBufHeader(buff)

	if !(heartbeatFlag == FlagPing || heartbeatFlag == FlagPong) {
		fmt.Println("keepalive error: cannot send non heartbeat data")
		return errors.ErrUnsupported
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

	if errors.As(err, &netErr) && netErr.Timeout() {
		fmt.Println("timeout error:", err)
		select {
		case c.closeCh <- struct{}{}:
		default:
		}

	}

	return err
}

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
				fmt.Println(c.Conn.LocalAddr(), "PING")
				err := c.keepAliveSender(FlagPong)
				if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {

					select {
					case c.closeCh <- struct{}{}:
					default:
					}
					return
				}
			} else {
				fmt.Println(c.Conn.LocalAddr(), "PONG")
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
		}
	}
}
