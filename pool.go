package unghost

import "sync"

var syncpool = sync.Pool{
	New: func() any {
		return make([]byte, ChunkSizeDefault)
	},
}

// getBufData retrieves a pre-allocated chunk buffer from the instance pool.
//
// [SPECIFICATION]
// - INTENT: Provide zero-allocation access to data buffers.
// - POSTCONDITION: Returns a buffer interface whose underlying type is []byte with length == chunkSize.
func getBufData() any {
	return syncpool.Get()
}

// putBufData returns a chunk buffer back to the instance pool.
//
// [SPECIFICATION]
// - INTENT: Return used data buffers to prevent garbage collection overhead.
// - PRECONDITION: buf MUST be of type []byte.
func putBufData(buf any) {
	syncpool.Put(buf)
}

// syncpoolHeader is a global pool strictly for allocating 9-byte headers.
var syncpoolHeader = sync.Pool{
	New: func() any {
		return make([]byte, HEADERLENGTH)
	},
}

// getBufHeader retrieves a pre-allocated header buffer from the global pool.
//
// [SPECIFICATION]
// - INTENT: Provide zero-allocation access to header buffers.
// - POSTCONDITION: Returns a buffer interface whose underlying type is []byte with length == HEADERLENGTH.
func getBufHeader() any {
	return syncpoolHeader.Get()
}

// putBufHeader returns a header buffer back to the global pool.
//
// [SPECIFICATION]
// - INTENT: Return used header buffers to prevent garbage collection overhead.
// - PRECONDITION: buf MUST be of type []byte.
func putBufHeader(buf any) {
	syncpoolHeader.Put(buf)
}
