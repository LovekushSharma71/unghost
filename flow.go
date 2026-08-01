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
}

// sender and reciever will consume a discrete amount of credits no matter how small datalength sent
//locking and unlocking will be handled where used

// Contains assuming this is always in mutex with send or recive to safely deduct credits.
// cause i need to check credits before sending and recieving.
// sender will always consume or free credit
func (c *heartbeatTCP) consumeCredits() {

	if c.flowControlData.sendCredits >= ChunkSizeDefault {
		c.flowControlData.sendCredits = c.flowControlData.sendCredits - ChunkSizeDefault
	} else {
		c.flowControlData.sendCredits = 0
	}

}

// atomically update window size on getting packet from reciever or when read operation is finished.
func (c *heartbeatTCP) refundCredits(size uint32) {

	c.flowControlData.sendCredits = min(MaxWindowSize, c.flowControlData.sendCredits+size)
}

// should not logically go above max size
// will only increase
func (c *heartbeatTCP) addProcessedCredit() {

	c.flowControlData.processedCredits = min(MaxWindowSize, c.flowControlData.processedCredits+ChunkSizeDefault)
}
