package unghost

import "testing"

func setupMockState(send uint32, processed uint32, maxSize uint32) *HeartbeatTCP {
	return &HeartbeatTCP{
		flowControlData: flowControlData{
			sendCredits:      send,
			processedCredits: processed,
			maxWindowSize:    maxSize,
		},
	}
}

func TestConsumeCredits(t *testing.T) {
	testCases := []struct {
		name       string
		givenState *HeartbeatTCP
		finalState *HeartbeatTCP
	}{
		{
			name:       "Base case: normal credit reduction",
			givenState: setupMockState(MaxWindowSize, 0, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize-ChunkSizeDefault, 0, MaxWindowSize),
		},
		{
			name:       "Boundary case: sendCredits equals chunkSize exactly",
			givenState: setupMockState(ChunkSizeDefault, 0, MaxWindowSize),
			finalState: setupMockState(0, 0, MaxWindowSize),
		},
		{
			name:       "Edge/Intent case: underflow protection clamps to 0",
			givenState: setupMockState(100, 0, MaxWindowSize),
			finalState: setupMockState(0, 0, MaxWindowSize),
		},
		{
			name:       "Edge case: sendCredits already 0 remains 0",
			givenState: setupMockState(0, 0, MaxWindowSize),
			finalState: setupMockState(0, 0, MaxWindowSize),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.givenState.flowControlData.flowDataLock.Lock()
			tc.givenState.consumeCredits()
			got := tc.givenState.flowControlData.sendCredits
			tc.givenState.flowControlData.flowDataLock.Unlock()

			want := tc.finalState.flowControlData.sendCredits

			if got != want {
				t.Errorf("got sendCredits: %d, want: %d", got, want)
			}
		})
	}
}

func TestRefundCredits(t *testing.T) {
	testCases := []struct {
		name       string
		givenState *HeartbeatTCP
		finalState *HeartbeatTCP
		size       uint32
	}{
		{
			name:       "Base case: normal credit refund",
			givenState: setupMockState(0, 0, MaxWindowSize),
			finalState: setupMockState(ChunkSizeDefault, 0, MaxWindowSize),
			size:       ChunkSizeDefault,
		},
		{
			name:       "Boundary case: refund reaches maxWindowSize exactly",
			givenState: setupMockState(MaxWindowSize-100, 0, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, 0, MaxWindowSize),
			size:       100,
		},
		{
			name:       "Edge/Intent case: overflow protection clamps to maxWindowSize",
			givenState: setupMockState(MaxWindowSize-100, 0, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, 0, MaxWindowSize),
			size:       500,
		},
		{
			name:       "Edge case: zero size refund changes nothing",
			givenState: setupMockState(500, 0, MaxWindowSize),
			finalState: setupMockState(500, 0, MaxWindowSize),
			size:       0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.givenState.flowControlData.flowDataLock.Lock()
			tc.givenState.refundCredits(tc.size)
			got := tc.givenState.flowControlData.sendCredits
			tc.givenState.flowControlData.flowDataLock.Unlock()

			want := tc.finalState.flowControlData.sendCredits

			if got != want {
				t.Errorf("got sendCredits: %d, want: %d", got, want)
			}
		})
	}
}

func TestAddProcessedCredit(t *testing.T) {
	testCases := []struct {
		name       string
		givenState *HeartbeatTCP
		finalState *HeartbeatTCP
	}{
		{
			name:       "Base case: normal processed credit accumulation",
			givenState: setupMockState(MaxWindowSize, 0, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, ChunkSizeDefault, MaxWindowSize),
		},
		{
			name:       "Boundary case: processed credits reach maxWindowSize exactly",
			givenState: setupMockState(MaxWindowSize, MaxWindowSize-ChunkSizeDefault, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, MaxWindowSize, MaxWindowSize),
		},
		{
			name:       "Edge/Intent case: overflow protection clamps processed credits to maxWindowSize",
			givenState: setupMockState(MaxWindowSize, MaxWindowSize-10, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, MaxWindowSize, MaxWindowSize),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.givenState.flowControlData.flowDataLock.Lock()
			tc.givenState.addProcessedCredit()
			got := tc.givenState.flowControlData.processedCredits
			tc.givenState.flowControlData.flowDataLock.Unlock()

			want := tc.finalState.flowControlData.processedCredits

			if got != want {
				t.Errorf("got processedCredits: %d, want: %d", got, want)
			}
		})
	}
}
