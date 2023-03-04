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
)

type Stream struct {
	id            uint32
	session       *Session
	readyReadChan chan *Frame
	ctx           context.Context
	cancel        context.CancelFunc
	readDeadline  time.Time
	writeDeadline time.Time
}

func (stream *Stream) Done() <-chan struct{} {
	return stream.ctx.Done()
}

func (stream *Stream) WriteContext(ctx context.Context, buffer []byte) (int, error) {
	start := time.Now()
	defer func() {
		sendFrameDuration.Observe(time.Since(start).Seconds())
		stream.session.detectBusyFlag(time.Since(start))
	}()

	frame := NewFrameContext(stream.ctx, cmdPSH, stream.id, buffer)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-stream.session.Ctx().Done():
		return 0, ErrSessionClosed
	case <-stream.ctx.Done():
		return 0, ErrStreamClosed
	case stream.session.readyWriteChan <- frame:
		// 直接返回的话可能会导致 frame 中 payload 的 buffer 被回收覆盖
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-stream.session.Ctx().Done():
			return 0, ErrSessionClosed
		case <-stream.ctx.Done():
			return 0, ErrStreamClosed
		case <-frame.ctx.Done():
			return len(buffer), nil
		}
	}
}

func (stream *Stream) Write(buffer []byte) (int, error) {
	if stream.writeDeadline.After(time.Now()) {
		ctx , cancel := context.WithDeadline(context.TODO(), stream.writeDeadline)
		defer cancel()
		return stream.WriteContext(ctx, buffer)
	}
	return stream.WriteContext(context.TODO(), buffer)
}

func (stream *Stream) ReadContext(ctx context.Context, buffer []byte) (int, error) {
	frame := NewFrameContext(stream.ctx, cmdPSH, stream.id, buffer)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-stream.session.Ctx().Done():
		return 0, ErrSessionClosed
	case <-stream.ctx.Done():
		return 0, ErrStreamClosed
	case stream.readyReadChan <- frame:
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-stream.session.Ctx().Done():
			return 0, ErrSessionClosed
		case <-stream.ctx.Done():
			return 0, ErrStreamClosed
		case <-frame.ctx.Done():
			return int(frame.dataLength), nil
		}
	}
}

func (stream *Stream) Read(buffer []byte) (int, error) {
	if stream.readDeadline.After(time.Now()) {
		ctx , cancel := context.WithDeadline(context.TODO(), stream.readDeadline)
		defer cancel()
		return stream.ReadContext(ctx, buffer)
	}

	return stream.ReadContext(context.TODO(), buffer)
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
		return err
	}

	select {
	case <-stream.session.Ctx().Done():
		return ErrSessionClosed
	case <-stream.ctx.Done():
		return ErrStreamClosed
	case stream.session.readyWriteChan <- NewFrameContext(stream.ctx, cmdFIN, stream.id, nil):
		stream.cancel()
		return nil
	}
}

func (stream *Stream) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "mux-stream", Net: "unix"}
}

func (stream *Stream) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "mux-stream", Net: "unix"}
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
