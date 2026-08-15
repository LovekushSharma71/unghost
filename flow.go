package unghost

import (
	"sync"
)

// Default constants for flow control windowing.
const (

	// ChunkSizeDefault is the default maximum size (64KB) for a single transmitted data chunk.
	ChunkSizeDefault uint32 = 65535

	// MaxWindowSize is the default total maximum window size (approx 1MB) for in-flight data.
	MaxWindowSize uint32 = 1048576
)

// flowControlData manages credit state and synchronization channels for a connection.
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

	maxWindowSize uint32
}

// consumeCredits deducts credits for an outgoing data chunk.
//
// [SPECIFICATION]
// - INTENT: Reduce the available sendCredits by chunkSize to reflect data being transmitted.
// - PRECONDITION: Caller MUST hold c.flowControlData.flowDataLock.
// - POSTCONDITION: If sendCredits >= chunkSize, sendCredits is decremented by chunkSize.
// - POSTCONDITION: If sendCredits < chunkSize, sendCredits MUST strictly clamp to 0.
// - INVARIANT: sendCredits MUST NEVER underflow below 0 (wrap around).
func (c *HeartbeatTCP) consumeCredits() {

	if c.flowControlData.sendCredits >= ChunkSizeDefault {
		c.flowControlData.sendCredits = c.flowControlData.sendCredits - ChunkSizeDefault
	} else {
		c.flowControlData.sendCredits = 0
	}

}

// refundCredits restores available credits after successful transmission or receipt of remote credits.
//
// [SPECIFICATION]
// - INTENT: Increase sendCredits by the provided size, up to the maximum window limit.
// - PRECONDITION: Caller MUST hold c.flowControlData.flowDataLock.
// - POSTCONDITION: sendCredits is increased by size.
// - POSTCONDITION: sendCredits MUST strictly clamp to maxWindowSize.
// - INVARIANT: sendCredits MUST NEVER exceed maxWindowSize.
func (c *HeartbeatTCP) refundCredits(size uint32) {

	c.flowControlData.sendCredits = min(c.flowControlData.maxWindowSize, c.flowControlData.sendCredits+size)
}

// addProcessedCredit increments the tracked read credits that need to be sent back to the remote.
//
// [SPECIFICATION]
// - INTENT: Increase processedCredits by chunkSize to reflect successfully read network data.
// - PRECONDITION: Caller MUST hold c.flowControlData.flowDataLock.
// - POSTCONDITION: processedCredits is increased by chunkSize.
// - POSTCONDITION: processedCredits MUST strictly clamp to maxWindowSize.
// - INVARIANT: processedCredits MUST NEVER exceed maxWindowSize.
func (c *HeartbeatTCP) addProcessedCredit() {

	c.flowControlData.processedCredits = min(c.flowControlData.maxWindowSize, c.flowControlData.processedCredits+ChunkSizeDefault)
}
