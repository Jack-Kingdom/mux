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
	synced        bool // 判断当前 stream 是否已经与 remote 同步
	session       *Session
	readyReadChan chan *Frame
	readDeadline  time.Time
	writeDeadline time.Time
	ctx           context.Context
	cancel        context.CancelFunc
}

func (stream *Stream) Done() <-chan struct{} {
	return stream.ctx.Done()
}

func (stream *Stream) Write(buffer []byte) (int, error) {
	frame := NewFrameContext(stream.ctx, cmdPSH, stream.id, buffer)

	if !stream.synced {
		frame.cmd = cmdSYN
		stream.synced = true
	}

	var timeout *time.Timer
	if stream.readDeadline != (time.Time{}) {
		timeout = time.NewTimer(time.Until(stream.readDeadline))
	} else {
		timeout = time.NewTimer(defaultTimeout)
	}
	defer timeout.Stop()

	select {
	case <- stream.session.Ctx().Done():
		return 0, ErrSessionClosed
	case <-stream.ctx.Done():
		return 0, ErrStreamClosed
	case <-timeout.C:
		return 0, ErrWriteTimeout
	case stream.session.readyWriteChan <- frame:
		// 直接返回的话可能会导致 frame 中 payload 的 buffer 被回收覆盖
		select {
		case <-stream.ctx.Done():
			return 0, ErrStreamClosed
		case <-frame.ctx.Done():
			return len(buffer), nil
		}
	}
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
	case <- stream.session.Ctx().Done():
		return 0, ErrSessionClosed
	case <-stream.ctx.Done():
		return 0, ErrStreamClosed
	case <-timeout.C:
		return 0, ErrReadTimeout
	case frame := <-stream.readyReadChan:
		if len(buffer) < int(frame.dataLength) {
			frame.Close()
			return 0, ErrReadBufferLimited
		}
		copy(buffer, frame.payload)
		frame.Close()
		return len(frame.payload), nil
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

func (stream *Stream) Close() error { // 主动关闭，需要通知 remote
	err := stream.session.unregisterStream(stream)
	if err != nil {
		if errors.Is(err, ErrStreamIdNotFound) { // 此处由 session 关闭了，跳过即可
			return nil
		}
		return err
	}

	select {
	case <- stream.session.Ctx().Done():
		return ErrSessionClosed
	case <-stream.ctx.Done():
		return ErrStreamClosed
	case stream.session.readyWriteChan <- NewFrameContext(stream.ctx, cmdFIN, stream.id, nil):
		stream.cancel()
		return nil
	}
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
