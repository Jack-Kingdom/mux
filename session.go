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
	SessionNilErr       = errors.New("nil session ops err")
	StreamIdDupErr      = errors.New("stream id duplicated err")
	StreamIdNotFoundErr = errors.New("stream id not found")
)

type roleType uint8

const (
	RoleClient roleType = iota + 1
	RoleServer
)

type SessionConfig struct {
	keepAliveSwitch   bool
	keepAliveInterval uint8
	keepAliveTTL      uint8
	bufferSize        uint
}

func getDefaultSessionConfig() *SessionConfig {
	return &SessionConfig{
		keepAliveSwitch:   true,
		keepAliveInterval: 30,
		keepAliveTTL:      90,
		bufferSize:        64 * 1024,
	}
}

type Session struct {
	conn io.ReadWriteCloser

	streams     map[uint32]*Stream
	streamMutex *sync.Mutex

	streamIdCounter uint32
	streamIdMutex   *sync.Mutex

	readyWriteChan  chan *Frame // chan Frame send to remote
	readyAcceptChan chan *Frame // chan Frame ready accept
	bufferPool      *sync.Pool  // session buffer pool

	err    error // current err
	ctx    context.Context
	cancel context.CancelFunc

	role   roleType
	config *SessionConfig
}

func NewSession(conn io.ReadWriteCloser, role roleType, configPtr *SessionConfig) *Session {

	ctx, cancel := context.WithCancel(context.TODO())

	if configPtr == nil {
		configPtr = getDefaultSessionConfig()
	}

	session := &Session{
		conn:            conn,
		streams:         make(map[uint32]*Stream, 256),
		streamMutex:     new(sync.Mutex),
		streamIdCounter: uint32(role),
		streamIdMutex:   new(sync.Mutex),
		readyWriteChan:  make(chan *Frame),
		readyAcceptChan: make(chan *Frame),
		bufferPool:      &sync.Pool{New: func() interface{} { return make([]byte, configPtr.bufferSize) }},
		err:             nil,
		ctx:             ctx,
		cancel:          cancel,
		role:            role,
		config:          configPtr,
	}

	// 维护任务
	go session.recvLoop()
	go session.sendLoop()
	go session.heartBeatLoop()

	return session
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

func (session *Session) getBuffer() []byte {
	return session.bufferPool.Get().([]byte)
}

func (session *Session) PutBuffer(buffer []byte) {
	session.bufferPool.Put(buffer)
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
	defer session.PutBuffer(buffer)

	ttl := 30 * 24 * time.Hour // 这里先假设一个很长的时间
	if session.config != nil && session.config.keepAliveSwitch {
		ttl = time.Duration(session.config.keepAliveTTL) * time.Second
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
			case cmdSYN:
				select {
				case <-session.ctx.Done():
					return
				case session.readyAcceptChan <- frame:
					select {
					case <-session.ctx.Done():
					case <-frame.ctx.Done():
					}
				}
			case cmdPSH:
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
	defer session.PutBuffer(buffer)

	defer func() {
		zap.L().Debug("mux sendLoop closed")
	}()

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
	if session.config != nil && session.config.keepAliveSwitch {
		ticker := time.NewTicker(time.Duration(session.config.keepAliveInterval) * time.Second)
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
	if session == nil {
		return nil, SessionNilErr
	}
	streamId := session.genStreamId()
	stream := NewStream(streamId, session)
	err := session.registerStream(stream)
	if err != nil {
		return nil, err
	}

	_, err = stream.writeFrame(cmdSYN, nil)
	return stream, err
}

func (session *Session) Open() (io.ReadWriteCloser, error) {
	return session.OpenStream()
}

func (session *Session) AcceptStream() (*Stream, error) {
	select {
	case <-session.ctx.Done():
		return nil, SessionClosedErr
	case frame := <-session.readyAcceptChan:
		stream := NewStream(frame.streamId, session)
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
