package unghost

import (
	"sync"
)

// will change these constants based on experiments
const (

	// max window size 2^16-1 for now ie 64kb
	ChunkSizeDefault uint32 = 65535

	// Total window size 2^20-1 for now ie 1mb approx
	MaxWindowSize uint32 = 1048576
)

type flowControlData struct {

	//How much i can send
	sendCredits uint32

	//how much i have read
	processedCredits uint32

	//stream is closed
	isClosed bool

	//free send channel
	sndNotifyCond *sync.Cond

	//send update channel
	processedCreditsNotifyCh chan struct{}

	flowDataLock sync.Mutex

	chunkSize uint32

	maxWindowSize uint32
}

// sender and reciever will consume a discrete amount of credits no matter how small datalength sent
//locking and unlocking will be handled where used

// Contains assuming this is always in mutex with send or recive to safely deduct credits.
// cause i need to check credits before sending and recieving.
// sender will always consume or free credit
func (c *HeartbeatTCP) consumeCredits() {

	if c.flowControlData.sendCredits >= c.flowControlData.chunkSize {
		c.flowControlData.sendCredits = c.flowControlData.sendCredits - c.flowControlData.chunkSize
	} else {
		c.flowControlData.sendCredits = 0
	}

}

// atomically update window size on getting packet from reciever or when read operation is finished.
func (c *HeartbeatTCP) refundCredits(size uint32) {

	c.flowControlData.sendCredits = min(c.flowControlData.maxWindowSize, c.flowControlData.sendCredits+size)
}

// should not logically go above max size
// will only increase
func (c *HeartbeatTCP) addProcessedCredit() {

	c.flowControlData.processedCredits = min(c.flowControlData.maxWindowSize, c.flowControlData.processedCredits+c.flowControlData.chunkSize)
}
