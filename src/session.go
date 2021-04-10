package mux

import (
	"context"
	"github.com/pkg/errors"
	"io"
	"sync"
)

var (
	ConnReadWriteOpsErr = errors.New("conn read write ops err")
	StreamNotFoundErr   = errors.New("stream not found")
	SessionClosedErr    = errors.New("session closed")
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
}

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

	role   roleType
	config *SessionConfig
}

func NewSession(conn io.ReadWriteCloser, role roleType, configPtr *SessionConfig) *Session {

	ctx, cancel := context.WithCancel(context.TODO())
	session := &Session{
		conn:            conn,
		streams:         make(map[uint32]*Stream, 256),
		streamMutex:     new(sync.Mutex),
		streamIdCounter: uint32(role),
		streamIdMutex:   new(sync.Mutex),
		readyWriteChan:  make(chan *Frame),
		readyAcceptChan: make(chan *Frame),
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
	buffer := GetBuffer()
	defer PutBuffer(buffer)

	for {
		select {
		case <-session.ctx.Done():
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
				if stream, err := session.getStream(frame.streamId); err == nil {
					select {
					case <-session.ctx.Done():
					case <-stream.ctx.Done():
					case stream.readyReadChan <- frame:
					}
				} else {
					session.CloseWithErr(StreamNotFoundErr)
					return
				}
			case cmdFIN:
				stream, ok := session.streams[frame.streamId]
				if !ok {
					session.CloseWithErr(StreamNotFoundErr)
					return
				}
				_ = stream.Close()
			case cmdNOP:
				// todo 心跳包检测
			}
		}
	}
}

func (session *Session) sendLoop() {
	buffer := GetBuffer()
	defer PutBuffer(buffer)

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

func (session *Session) heartBeatLoop() {
	// todo
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
