package unghost

import "testing"

// Helper to cleanly spin up a state without cluttering your test cases
func setupMockState(send uint32, processed uint32, chunkSize uint32, maxSize uint32) *HeartbeatTCP {
	return &HeartbeatTCP{
		flowControlData: flowControlData{
			sendCredits:      send,
			processedCredits: processed,
			maxWindowSize:    maxSize,
			chunkSize:        chunkSize,
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
			name:       "ConsumeCredits: base case",
			givenState: setupMockState(MaxWindowSize, 0, ChunkSizeDefault, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize-ChunkSizeDefault, 0, ChunkSizeDefault, MaxWindowSize),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.givenState.consumeCredits()

			// Extract targeted state
			got := testCase.givenState.flowControlData.sendCredits
			want := testCase.finalState.flowControlData.sendCredits

			if got != want {
				t.Errorf("%s: got sendCredits: %d, want: %d", testCase.name, got, want)
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
			name:       "RefundCredits: base case",
			givenState: setupMockState(0, ChunkSizeDefault, ChunkSizeDefault, MaxWindowSize),
			finalState: setupMockState(ChunkSizeDefault, 0, ChunkSizeDefault, MaxWindowSize),
			size:       ChunkSizeDefault,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.givenState.refundCredits(testCase.size)

			// Extract targeted state
			got := testCase.givenState.flowControlData.sendCredits
			want := testCase.finalState.flowControlData.sendCredits

			if got != want {
				t.Errorf("%s (refund size %d): got sendCredits: %d, want: %d", testCase.name, testCase.size, got, want)
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
			name:       "AddProcessedCredit: base case",
			givenState: setupMockState(MaxWindowSize, 0, ChunkSizeDefault, MaxWindowSize),
			finalState: setupMockState(MaxWindowSize, ChunkSizeDefault, ChunkSizeDefault, MaxWindowSize),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.givenState.addProcessedCredit()

			// Extract targeted state
			got := testCase.givenState.flowControlData.processedCredits
			want := testCase.finalState.flowControlData.processedCredits

			if got != want {
				t.Errorf("%s: got processedCredits: %d, want: %d", testCase.name, got, want)
			}
		})
	}
}
