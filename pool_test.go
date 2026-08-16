package unghost

import (
	"testing"
)

func TestHeaderPool(t *testing.T) {
	t.Run("Get and Put Header Buffer", func(t *testing.T) {
		
		bufAny := getBufHeader()

		
		buf, ok := bufAny.([]byte)
		if !ok {
			t.Errorf("getBufHeader: got type %T, want []byte", bufAny)
			return
		}


		gotLen := len(buf)
		wantLen := HEADERLENGTH
		if gotLen != wantLen {
			t.Errorf("getBufHeader length: got %d, want %d", gotLen, wantLen)
		}

	
		putBufHeader(buf)
	})
}

func TestDataPool(t *testing.T) {
	t.Run("Get and Put Data Buffer", func(t *testing.T) {

		bufAny := getBufData()

		buf, ok := bufAny.([]byte)
		if !ok {
			t.Errorf("getBufData: got type %T, want []byte", bufAny)
			return
		}

		gotLen := uint32(len(buf))
		wantLen := ChunkSizeDefault
		if gotLen != wantLen {
			t.Errorf("getBufData length: got %d, want %d", gotLen, wantLen)
		}


		putBufData(buf)
	})
}
