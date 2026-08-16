package unghost

import (
	"testing"
)

func TestHeaderPool(t *testing.T) {
	t.Run("Get and Put Header Buffer", func(t *testing.T) {
		// 1. Get a buffer from the global pool
		bufAny := getBufHeader()

		// 2. Verify it is exactly a byte slice (type assertion check)
		buf, ok := bufAny.([]byte)
		if !ok {
			t.Errorf("getBufHeader: got type %T, want []byte", bufAny)
			return // stop test if type is wrong to prevent panics below
		}

		// 3. Verify the length perfectly matches HEADERLENGTH
		gotLen := len(buf)
		wantLen := HEADERLENGTH
		if gotLen != wantLen {
			t.Errorf("getBufHeader length: got %d, want %d", gotLen, wantLen)
		}

		// 4. Put it back (verifies it accepts the type without panicking)
		putBufHeader(buf)
	})
}

func TestDataPool(t *testing.T) {
	t.Run("Get and Put Data Buffer", func(t *testing.T) {

		bufAny := getBufData()

		// 3. Verify type
		buf, ok := bufAny.([]byte)
		if !ok {
			t.Errorf("getBufData: got type %T, want []byte", bufAny)
			return
		}

		// 4. Verify length matches the chunk size configured in the mock
		gotLen := uint32(len(buf))
		wantLen := ChunkSizeDefault
		if gotLen != wantLen {
			t.Errorf("getBufData length: got %d, want %d", gotLen, wantLen)
		}

		// 5. Put it back
		putBufData(buf)
	})
}
