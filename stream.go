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
	session       *Session
	readyReadChan chan *Frame
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewStream(streamId uint32, session *Session) *Stream {
	ctx, cancel := context.WithCancel(context.TODO())
	return &Stream{
		id:            streamId,
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
		case <-stream.session.ctx.Done():
			return 0, stream.session.err
		case <-stream.ctx.Done():
			return 0, StreamClosedErr
		case <-frame.ctx.Done():
			return len(buffer), nil
		}
	}
}

func (stream *Stream) Write(buffer []byte) (int, error) {
	return stream.writeFrame(cmdPSH, buffer)
}

func (stream *Stream) Read(buffer []byte) (int, error) {
	select {
	case <-stream.session.ctx.Done():
		return 0, stream.session.err
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
	stream.cancel()
	return nil
}
