package mux

import (
	"context"
	"errors"
)

var (
	StreamClosedErr = errors.New("stream has been closed")
)

type Stream struct {
	id            uint32
	syn           bool // if sync false, write sync cmd to server create new stream
	session       *Session
	readyReadChan chan *Frame
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewStream todo 修改为 session 的成员方法
func NewStream(streamId uint32, session *Session) *Stream {
	ctx, cancel := context.WithCancel(session.ctx)
	return &Stream{
		id:            streamId,
		syn:           false,
		session:       session,
		readyReadChan: make(chan *Frame),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (stream *Stream) writeFrame(cmd cmdType, buffer []byte) (int, error) {
	frame := NewFrame(cmd, stream.id, buffer)

	select {
	case <-stream.session.ctx.Done():
		return 0, stream.session.err
	case stream.session.readyWriteChan <- frame:
		// 直接返回的话可能会导致 frame 中 data 的 buffer 被回收覆盖
		select {
		case <-stream.ctx.Done():
			return 0, StreamClosedErr
		case <-frame.ctx.Done():
			return len(buffer), nil
		}
	}
}

func (stream *Stream) Done() <-chan struct{} {
	return stream.ctx.Done()
}

func (stream *Stream) Write(buffer []byte) (int, error) {
	var cmd cmdType
	if stream.syn {
		cmd = cmdPSH
	}else {
		cmd = cmdSYN
		stream.syn = true
	}

	return stream.writeFrame(cmd, buffer)
}

func (stream *Stream) Read(buffer []byte) (int, error) {
	select {
	case <-stream.ctx.Done():
		return 0, StreamClosedErr
	case frame := <-stream.readyReadChan:
		copy(buffer, frame.data) // todo 对性能的影响待评估
		_ = frame.Close()
		return len(frame.data), nil
	}
}

func (stream *Stream) IsClose() bool {
	select {
	case <-stream.ctx.Done():
		return true
	default:
		return false
	}
}

func (stream *Stream) Close() error {
	_, _ = stream.writeFrame(cmdFIN, nil)
	stream.cancel()
	_ = stream.session.unregisterStream(stream)
	return nil
}
