package mux

import (
	"context"
	dsaBuffer "github.com/Jack-Kingdom/go-dsa/buffer"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"net"
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
	RoleClient roleType = 1 + iota // 客户端为奇数，服务器端为偶数，这里区分 client 与 server 主要是为了防止 streamId 冲突，在传输过程中二者是对等的
	RoleServer
)

func (rule roleType) String() string {
	switch rule {
	case RoleClient:
		return "client"
	case RoleServer:
		return "server"
	default:
		return "unknown"
	}
}

type Session struct {
	conn             net.Conn
	readTimeout      time.Duration
	writeTimeout     time.Duration
	readWriteTimeout time.Duration

	streams     map[uint32]*Stream
	streamMutex sync.Mutex

	streamIdCounter uint32
	streamIdMutex   sync.Mutex

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
	bufferSize        int
	bufferAlloc       BufferAllocFunc
	bufferRecycle     BufferRecycleFunc

	// private variable
	createdAt time.Time
}

type Option func(*Session)
type BufferAllocFunc func() []byte
type BufferRecycleFunc func([]byte)

func WithReadTimeout(readTimeout time.Duration) Option {
	return func(s *Session) {
		s.readTimeout = readTimeout
	}
}

func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(s *Session) {
		s.writeTimeout = writeTimeout
	}
}

func WithReadWriteTimeout(readWriteTimeout time.Duration) Option {
	return func(s *Session) {
		s.readWriteTimeout = readWriteTimeout
	}
}

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

func WithBufferSize(sizeLimit int) Option {
	return func(session *Session) {
		session.bufferSize = sizeLimit
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

func NewSessionContext(ctx context.Context, conn net.Conn, options ...Option) *Session {
	currentCtx, cancel := context.WithCancel(ctx)
	session := &Session{
		conn:            conn,
		streams:         make(map[uint32]*Stream, 64),
		readyWriteChan:  make(chan *Frame),
		readyAcceptChan: make(chan *Frame),
		err:             nil,
		ctx:             currentCtx,
		cancel:          cancel,

		role:              RoleClient,
		keepAliveSwitch:   false,
		keepAliveInterval: 30,
		keepAliveTTL:      90,
		bufferSize:        1024,
		createdAt:         time.Now(),
	}

	for _, option := range options {
		option(session)
	}

	session.streamIdCounter = uint32(session.role) // 初始化 streamId 计数器

	// 维护任务
	go session.recvLoop()
	go session.sendLoop()
	if session.keepAliveSwitch {
		go session.heartBeatLoop()
	}

	return session
}

func NewSession(conn net.Conn, options ...Option) *Session {
	return NewSessionContext(context.TODO(), conn, options...)
}

func (session *Session) StreamCount() int {
	return len(session.streams)
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
	if session.bufferAlloc != nil {
		return session.bufferAlloc()
	}

	return dsaBuffer.Get(session.bufferSize)
}

func (session *Session) putBuffer(buffer []byte) {
	if session.bufferRecycle != nil {
		session.bufferRecycle(buffer)
	} else {
		dsaBuffer.Put(buffer)
	}
}

func (session *Session) Ctx() context.Context {
	return session.ctx
}

// Lifetime 用以获取当前 session 的存在时间
func (session *Session) Lifetime() time.Duration {
	return time.Now().Sub(session.createdAt)
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
	_ = session.conn.Close()
	return session.err
}

func (session *Session) CloseWithErr(err error) {
	session.err = err
	_ = session.Close()
}

func (session *Session) configureReadDeadline() {
	if session.readTimeout > 0 {
		_ = session.conn.SetReadDeadline(time.Now().Add(session.readTimeout))
	}else if session.readWriteTimeout > 0 {
		_ = session.conn.SetReadDeadline(time.Now().Add(session.readWriteTimeout))
	}else {
		_ = session.conn.SetReadDeadline(time.Time{})
	}
}

func (session *Session) configureWriteDeadline() {
	if session.writeTimeout > 0 {
		_ = session.conn.SetWriteDeadline(time.Now().Add(session.writeTimeout))
	}else if session.readWriteTimeout > 0 {
		_ = session.conn.SetWriteDeadline(time.Now().Add(session.readWriteTimeout))
	}else {
		_ = session.conn.SetWriteDeadline(time.Time{})
	}
}

func (session *Session) recvLoop() {
	buffer := session.getBuffer()
	defer session.putBuffer(buffer)

	if len(buffer) < headerSize {
		session.CloseWithErr(BufferSizeLimitErr)
		return
	}

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
			// 首先处理 header
			session.configureReadDeadline()
			n, err := session.conn.Read(buffer[:headerSize])
			if err != nil {
				session.CloseWithErr(errors.Wrap(ConnReadWriteOpsErr, err.Error()))
				return
			}

			var header Frame

			_, err = header.UnMarshalHeader(buffer[:n])
			if err != nil {
				session.CloseWithErr(errors.Wrap(err, "err on unmarshal header to frame"))
				return
			}

			switch header.cmd {
			case cmdSYN:
				synFrame := NewFrameContext(session.ctx, cmdSYN, header.streamId, nil)
				select {
				case <-session.ctx.Done():
					return
				case session.readyAcceptChan <- synFrame:
					select {
					case <-session.ctx.Done():
					case <-synFrame.ctx.Done():
					}
				}
			case cmdPSH:
				// 注意这个地方需要处理拆包和粘包的问题
				if len(buffer) < int(header.dataLength) {
					session.CloseWithErr(BufferSizeLimitErr)
					return
				}

				hasRead := 0
				for hasRead < int(header.dataLength) {
					session.configureReadDeadline()
					n, err := session.conn.Read(buffer[hasRead:header.dataLength])
					if err != nil {
						session.CloseWithErr(errors.Wrap(ConnReadWriteOpsErr, err.Error()))
						return
					}

					hasRead += n
				}

				dataFrame := NewFrameContext(session.ctx, cmdPSH, header.streamId, buffer[:header.dataLength])
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, StreamIdNotFoundErr) {
					// 此处没有拿到 stream，可能被关闭了，主动关闭连接时需通知远程进行关闭，此处丢弃
					dataFrame.Close()
					continue
				}

				select {
				case <-session.ctx.Done():
				case <-stream.ctx.Done():
				case stream.readyReadChan <- dataFrame:
					// 这里需要等待 frame 被消耗掉
					select {
					case <-session.ctx.Done():
					case <-stream.ctx.Done():
					case <-dataFrame.ctx.Done():
					}
				}

			case cmdFIN: // 收到远程的关闭通知，被动关闭
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, StreamIdNotFoundErr) {
					// 这个 stream 可能已经被关闭了,直接返回就可以了
					continue
				}
				select {
				case <-stream.Done():
					return
				default:
					// 被动关闭，不需要通知 remote
					stream.cancel()
					_ = session.unregisterStream(stream)
				}

			case cmdNOP:
				zap.L().Debug("ttl pkg received", zap.String("rule", session.role.String()))
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
			// write header
			n, err := frame.MarshalHeader(buffer)
			if err != nil {
				session.CloseWithErr(err)
				return
			}

			session.configureWriteDeadline()
			_, err = session.conn.Write(buffer[:n])
			if err != nil {
				session.CloseWithErr(errors.Wrap(err, "err on write frame header"))
				return
			}

			if frame.cmd == cmdPSH {
				session.configureWriteDeadline()
				n, err = session.conn.Write(frame.payload[:frame.dataLength])
				if err != nil {
					session.CloseWithErr(errors.Wrap(err, "err on write frame payload"))
					return
				}
			}
			frame.Close() // 标记当前 frame 处理完毕
		}
	}
}

// 持续地发送心跳包
func (session *Session) heartBeatLoop() {
	ticker := time.NewTicker(time.Duration(session.keepAliveInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
			frame := NewFrameContext(session.ctx, cmdNOP, 0, nil)
			session.readyWriteChan <- frame
			zap.L().Debug("ttl pkg sent", zap.String("role", session.role.String()))
		}
	}
}

func (session *Session) newStream(streamId uint32) *Stream {
	ctx, cancel := context.WithCancel(session.ctx)
	return &Stream{
		id:            streamId,
		session:       session,
		readyReadChan: make(chan *Frame),
		ctx:           ctx,
		cancel:        cancel,
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
		return nil
	} else {
		return StreamIdNotFoundErr
	}
}

// OpenStream 创建一个新的 stream
func (session *Session) OpenStream() (*Stream, error) {
	if session.IsClose() {
		return nil, SessionClosedErr
	}

	streamId := session.genStreamId()

	select {
	case <-session.ctx.Done():
		return nil, SessionClosedErr
	case session.readyWriteChan <- NewFrameContext(session.ctx, cmdSYN, streamId, nil):
		stream := session.newStream(streamId)
		err := session.registerStream(stream)
		if err != nil {
			return nil, err
		}
		return stream, nil
	}
}

func (session *Session) Open() (io.ReadWriteCloser, error) {
	return session.OpenStream()
}

func (session *Session) AcceptStream() (*Stream, error) {
	select {
	case <-session.ctx.Done():
		return nil, errors.Wrap(SessionClosedErr, session.err.Error())
	case frame := <-session.readyAcceptChan:
		stream := session.newStream(frame.streamId)
		err := session.registerStream(stream)
		if err != nil {
			return nil, err
		}
		frame.Close()
		return stream, nil
	}
}

func (session *Session) Accept() (io.ReadWriteCloser, error) {
	return session.AcceptStream()
}
