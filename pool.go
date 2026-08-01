package unghost

import "sync"

// create a buffer of size chunkSizeDefault
// should i keep length 0?
var syncpoolData = sync.Pool{
	New: func() any {
		return make([]byte, ChunkSizeDefault)
	},
}

func getBufData() any {
	return syncpoolData.Get()
}

func putBufData(buf any) {
	syncpoolData.Put(buf)
}

var syncpoolHeader = sync.Pool{
	New: func() any {
		return make([]byte, HEADERLENGTH)
	},
}

func getBufHeader() any {
	return syncpoolHeader.Get()
}

func putBufHeader(buf any) {
	syncpoolHeader.Put(buf)
}
