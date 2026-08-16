package unghost

import (
	"bytes"
	"math"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	testCases := []struct {
		name    string
		flag    byte
		credits uint32
		length  uint32
	}{
		{
			name:    "Ping flag with zero data",
			flag:    FlagPing,
			credits: 1024,
			length:  0,
		},
		{
			name:    "Max uint32 values boundary case",
			flag:    FlagUserData,
			credits: math.MaxUint32,
			length:  math.MaxUint32,
		},
		{
			name:    "Zero values boundary case",
			flag:    0x00,
			credits: 0,
			length:  0,
		},
		{
			name:    "Pong flag with standard window",
			flag:    FlagPong,
			credits: 65535,
			length:  128,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, HEADERLENGTH)

			putHeader(tc.flag, tc.credits, tc.length, buf)

			gotFlag, gotCredits, gotLength := parseHeader(buf)

			if gotFlag != tc.flag {
				t.Errorf("Flag mismatch: got 0x%02x, want 0x%02x", gotFlag, tc.flag)
			}
			if gotCredits != tc.credits {
				t.Errorf("Credits mismatch: got %d, want %d", gotCredits, tc.credits)
			}
			if gotLength != tc.length {
				t.Errorf("Length mismatch: got %d, want %d", gotLength, tc.length)
			}
		})
	}
}

func TestPutHeaderExactBytes(t *testing.T) {
	t.Run("Intent case: validates exact BigEndian byte positioning", func(t *testing.T) {
		buf := make([]byte, HEADERLENGTH)

		// BigEndian test pattern: 0x11223344 for credits, 0x55667788 for length
		putHeader(FlagUserData, 0x11223344, 0x55667788, buf)

		expectedBytes := []byte{
			FlagUserData,
			0x11, 0x22, 0x33, 0x44, // Credits (Bytes 1-4)
			0x55, 0x66, 0x77, 0x88, // Length  (Bytes 5-8)
		}

		if !bytes.Equal(buf, expectedBytes) {
			t.Errorf("Byte layout mismatch.\nGot:  %x\nWant: %x", buf, expectedBytes)
		}
	})
}
