package mux

import (
	"sync"
)

// todo 不要写死，后面将其作为一个接口暴露出来

const (
	BufferLength = 64 * 1024
	BufferLimit  = 63 * 1024	// 保留一部分用作 Frame 的 header, 目前最大为 1k
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, BufferLength)
	},
}

func GetBuffer() []byte {
	return bufferPool.Get().([]byte)
}

func PutBuffer(buffer []byte) {
	bufferPool.Put(buffer)
}
