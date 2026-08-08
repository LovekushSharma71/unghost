package unghost

import "sync"

// // create a buffer of size chunkSizeDefault
// var syncpoolData = sync.Pool{
// 	New: func() any {
// 		return make([]byte, ChunkSizeDefault)
// 	},
// }

func (c *HeartbeatTCP) getBufData() any {
	return c.datapool.Get()
}

func (c *HeartbeatTCP) putBufData(buf any) {
	c.datapool.Put(buf)
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
