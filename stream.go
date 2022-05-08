package mux

import (
	"context"
	"errors"
	"net"
	"time"
)

var (
	ErrStreamClosed      = errors.New("stream has been closed")
	ErrReadBufferLimited = errors.New("read buffer limited")
	ErrReadTimeout       = errors.New("read timeout")
	ErrWriteTimeout      = errors.New("write timeout")

	defaultTimeout = 24 * time.Hour
)

type Stream struct {
	id            uint32
	syn           bool // if sync false, write sync cmd to server create new stream with data
	session       *Session
	readyReadChan chan *Frame
	readDeadline  time.Time
	writeDeadline time.Time
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewStream create a new stream
func NewStream(streamId uint32, syn bool, session *Session) *Stream {
	ctx, cancel := context.WithCancel(session.ctx)
	return &Stream{
		id:            streamId,
		syn:           syn,
		session:       session,
		readyReadChan: make(chan *Frame),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (stream *Stream) writeFrame(cmd cmdType, buffer []byte) (int, error) {
	frame := NewFrame(cmd, stream.id, buffer)

	var timeout *time.Timer
	if stream.readDeadline != (time.Time{}) {
		timeout = time.NewTimer(time.Until(stream.readDeadline))
	} else {
		timeout = time.NewTimer(defaultTimeout)
	}
	defer timeout.Stop()

	select {
	case <-stream.session.ctx.Done():
		return 0, stream.session.err
	case <-timeout.C:
		return 0, ErrWriteTimeout
	case stream.session.readyWriteChan <- frame:
		// 直接返回的话可能会导致 frame 中 data 的 buffer 被回收覆盖
		select {
		case <-stream.ctx.Done():
			return 0, ErrStreamClosed
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
	} else {
		cmd = cmdSYN
		stream.syn = true
	}

	return stream.writeFrame(cmd, buffer)
}

func (stream *Stream) Read(buffer []byte) (int, error) {
	var timeout *time.Timer
	if stream.readDeadline != (time.Time{}) {
		timeout = time.NewTimer(time.Until(stream.readDeadline))
	} else {
		timeout = time.NewTimer(defaultTimeout)
	}
	defer timeout.Stop()

	select {
	case <-stream.ctx.Done():
		return 0, ErrStreamClosed
	case <-timeout.C:
		return 0, ErrReadTimeout
	case frame := <-stream.readyReadChan:
		if len(buffer) < len(frame.data) {
			return 0, ErrReadBufferLimited
		}
		copy(buffer, frame.data)
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

func (stream *Stream) SetReadDeadline(t time.Time) error {
	stream.readDeadline = t
	return nil
}

func (stream *Stream) SetWriteDeadline(t time.Time) error {
	stream.writeDeadline = t
	return nil
}

func (stream *Stream) SetDeadline(t time.Time) error {
	stream.readDeadline = t
	stream.writeDeadline = t
	return nil
}

func (stream *Stream) LocalAddr() net.Addr {
	return &Addr{}
}

func (stream *Stream) RemoteAddr() net.Addr {
	return &Addr{}
}