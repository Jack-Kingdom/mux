package mux

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

var (
	ConnReadWriteOpsErr = errors.New("conn read write ops err")
	SessionClosedErr    = errors.New("session closed")
	SessionTTLExceed    = errors.New("session ttl exceed")
	StreamIdDupErr      = errors.New("stream id duplicated err")
	StreamIdNotFoundErr = errors.New("stream id not found")
)

type roleType uint8

const (
	RoleClient roleType = iota + 1 // 客户端为奇数，服务器端为偶数
	RoleServer
)

type Session struct {
	conn io.ReadWriteCloser

	streams     map[uint32]*Stream
	streamMutex *sync.Mutex

	streamIdCounter uint32
	streamIdMutex   *sync.Mutex

	readyWriteChan  chan *Frame // chan Frame send to remote
	readyAcceptChan chan *Frame // chan Frame ready accept

	err    error // current err
	ctx    context.Context
	cancel context.CancelFunc

	// session config
	role              roleType
	keepAliveSwitch   bool
	keepAliveInterval uint8
	keepAliveTTL      uint8
	bufferAlloc       BufferAllocFunc
	bufferRecycle     BufferRecycleFunc
	createdAt         time.Time
}

type Option func(*Session)
type BufferAllocFunc func() []byte
type BufferRecycleFunc func([]byte)

func WithRole(role roleType) Option {
	return func(session *Session) {
		session.role = role
	}
}

func WithKeepAliveSwitch(choose bool) Option {
	return func(session *Session) {
		session.keepAliveSwitch = choose
	}
}

func WithKeepAliveInterval(interval uint8) Option {
	return func(session *Session) {
		session.keepAliveInterval = interval
	}
}

func WithKeepAliveTTL(ttl uint8) Option {
	return func(session *Session) {
		session.keepAliveTTL = ttl
	}
}

func WithBufferAllocFunc(f BufferAllocFunc) Option {
	return func(session *Session) {
		session.bufferAlloc = f
	}
}

func WithBufferRecycleFunc(f BufferRecycleFunc) Option {
	return func(session *Session) {
		session.bufferRecycle = f
	}
}

func NewSessionContext(ctx context.Context, conn io.ReadWriteCloser, options ...Option) *Session {
	ctx, cancel := context.WithCancel(ctx)
	session := &Session{
		conn:            conn,
		streams:         make(map[uint32]*Stream, 64),
		streamMutex:     new(sync.Mutex),
		streamIdCounter: 0,
		streamIdMutex:   new(sync.Mutex),
		readyWriteChan:  make(chan *Frame),
		readyAcceptChan: make(chan *Frame),
		err:             nil,
		ctx:             ctx,
		cancel:          cancel,

		role:              RoleClient,
		keepAliveSwitch:   false,
		keepAliveInterval: 30,
		keepAliveTTL:      90,
		bufferAlloc:       nil,
		bufferRecycle:     nil,
		createdAt:         time.Now(),
	}

	for _, option := range options {
		option(session)
	}

	session.streamIdCounter = uint32(session.role) // 初始化 streamId 计数器

	// 维护任务
	go session.recvLoop()
	go session.sendLoop()
	go session.heartBeatLoop()

	return session
}

func NewSession(conn io.ReadWriteCloser, options ...Option) *Session {
	return NewSessionContext(context.TODO(), conn, options...)
}

/*
get usable streamId
*/
func (session *Session) genStreamId() uint32 {

	session.streamIdMutex.Lock()
	defer session.streamIdMutex.Unlock()

	current := session.streamIdCounter
	session.streamIdCounter += 2

	return current
}

func (session *Session) CreatedAt() time.Time {
	return session.createdAt
}

func (session *Session) getBuffer() []byte {
	if session.bufferAlloc == nil {
		return getBuffer()
	}
	return session.bufferAlloc()
}

func (session *Session) putBuffer(buffer []byte) {
	if session.bufferRecycle == nil {
		putBuffer(buffer)
		return
	}
	session.bufferRecycle(buffer)
}

func (session *Session) Done() <-chan struct{} {
	return session.ctx.Done()
}

func (session *Session) IsClose() bool {
	select {
	case <-session.ctx.Done():
		return true
	default:
		return false
	}
}

func (session *Session) Close() error {
	session.cancel()
	return session.err
}

func (session *Session) CloseWithErr(err error) {
	session.err = err
	_ = session.Close()
}

func (session *Session) recvLoop() {
	buffer := session.getBuffer()
	defer session.putBuffer(buffer)

	ttl := 30 * 24 * time.Hour // 这里先假设一个很长的时间
	if session.keepAliveSwitch {
		ttl = time.Duration(session.keepAliveTTL) * time.Second
	}

	ttlTicker := time.NewTicker(ttl)
	defer ttlTicker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ttlTicker.C:
			session.CloseWithErr(SessionTTLExceed)
			return
		default:
			n, err := session.conn.Read(buffer)
			if err != nil {
				session.CloseWithErr(errors.Wrap(ConnReadWriteOpsErr, err.Error()))
				return
			}

			frame := NewFrame(cmdNOP, 0, nil)
			_, err = frame.UnMarshal(buffer[:n])
			if err != nil {
				session.CloseWithErr(errors.Wrap(err, "err on unmarshal buffer to frame"))
				return
			}

			switch frame.cmd {
			case cmdSYN, cmdPSH:
				if frame.cmd == cmdSYN {	// syn frame create stream first
					synFrame := NewFrame(cmdSYN, frame.streamId, nil)
					select {
					case <-session.ctx.Done():
						return
					case session.readyAcceptChan <- synFrame:
						select {
						case <-session.ctx.Done():
						case <-synFrame.ctx.Done():
						}
					}
				}

				stream, err := session.getStream(frame.streamId)
				if err != nil && errors.Is(err, StreamIdNotFoundErr) {
					// 此处没有拿到 stream，可能被关闭了，此时通知远程进行关闭
					// 注意这个地方可能会重复发多个包
					select {
					case <-session.ctx.Done():
					case session.readyWriteChan <- NewFrame(cmdFIN, frame.streamId, nil):
					}
					continue
				}

				select {
				case <-session.ctx.Done():
				case <-stream.ctx.Done():
				case stream.readyReadChan <- frame:
					// 这里需要等待 frame 被消耗掉
					select {
					case <-session.ctx.Done():
					case <-stream.ctx.Done():
					case <-frame.ctx.Done():
					}
				}

			case cmdFIN:
				stream, err := session.getStream(frame.streamId)
				if err != nil && errors.Is(err, StreamIdNotFoundErr) {
					// 这个 stream 可能已经被关闭了,直接返回就可以了
					continue
				}
				_ = stream.Close()
			case cmdNOP:
				zap.L().Debug("ttl pkg received")
				ttlTicker.Reset(ttl)
			}
		}
	}
}

func (session *Session) sendLoop() {
	buffer := session.getBuffer()
	defer session.putBuffer(buffer)

	for {
		select {
		case <-session.ctx.Done():
			return
		case frame := <-session.readyWriteChan:
			n, err := frame.Marshal(buffer)
			if err != nil {
				session.CloseWithErr(err)
				return
			}

			_, err = session.conn.Write(buffer[:n])
			if err != nil {
				session.CloseWithErr(err)
				return
			}

			_ = frame.Close() // 标记当前 frame 不再使用了
		}
	}
}

// 持续地发送心跳包
func (session *Session) heartBeatLoop() {
	if session.keepAliveSwitch {
		ticker := time.NewTicker(time.Duration(session.keepAliveInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-session.ctx.Done():
				return
			case <-ticker.C:
				frame := NewFrame(cmdNOP, 0, nil)
				session.readyWriteChan <- frame
				zap.L().Debug("ttl pkg sent")
			}
		}
	}

}

func (session *Session) getStream(streamId uint32) (*Stream, error) {
	session.streamMutex.Lock()
	defer session.streamMutex.Unlock()

	if stream, ok := session.streams[streamId]; ok {
		return stream, nil
	} else {
		return nil, StreamIdNotFoundErr
	}
}

func (session *Session) registerStream(stream *Stream) error {
	session.streamMutex.Lock()
	defer session.streamMutex.Unlock()

	if _, ok := session.streams[stream.id]; ok {
		return StreamIdDupErr
	}

	session.streams[stream.id] = stream
	return nil
}

func (session *Session) unregisterStream(stream *Stream) error {
	session.streamMutex.Lock()
	defer session.streamMutex.Unlock()

	if _, ok := session.streams[stream.id]; ok {
		delete(session.streams, stream.id)
	}

	return nil
}

func (session *Session) OpenStream() (*Stream, error) {
	streamId := session.genStreamId()
	stream := NewStream(streamId, false, session)
	err := session.registerStream(stream)
	if err != nil {
		return nil, err
	}
	return stream, err
}

func (session *Session) Open() (io.ReadWriteCloser, error) {
	return session.OpenStream()
}

func (session *Session) AcceptStream() (*Stream, error) {
	select {
	case <-session.ctx.Done():
		return nil, errors.Wrap(SessionClosedErr, session.err.Error())
	case frame := <-session.readyAcceptChan:
		stream := NewStream(frame.streamId, true, session)
		err := session.registerStream(stream)
		if err != nil {
			return nil, err
		}
		return stream, frame.Close()
	}

}

func (session *Session) Accept() (io.ReadWriteCloser, error) {
	return session.AcceptStream()
}
