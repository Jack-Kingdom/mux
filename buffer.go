package mux

import "sync"

const (
	defaultBufferSize = 64 * 1024
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, defaultBufferSize)
	},
}

func getBuffer() []byte {
	return bufferPool.Get().([]byte)
}

func putBuffer(buffer []byte) {
	bufferPool.Put(buffer)
}
