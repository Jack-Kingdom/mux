package mux

import (
	"context"
	"errors"
	"fmt"
	dsaBuffer "github.com/Jack-Kingdom/go-dsa/buffer"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSessionClosed    = errors.New("session closed")
	ErrSessionTTLExceed = errors.New("session ttl exceed")
	ErrStreamIdDup      = errors.New("stream id duplicated err")
	ErrStreamIdNotFound = errors.New("stream id not found")
)

type roleType uint8

const (
	RoleClient roleType = 1 + iota // 客户端为奇数，服务器端为偶数，这里区分 client 与 server 主要是为了防止 streamId 冲突，在传输过程中二者是对等的
	RoleServer
)

func (role roleType) String() string {
	switch role {
	case RoleClient:
		return "client"
	case RoleServer:
		return "server"
	default:
		return "unknown"
	}
}

type Session struct {
	conn            io.ReadWriteCloser
	streams         map[uint32]*Stream
	streamMutex     sync.Mutex
	streamIdCounter uint32
	isBusy          bool

	readyWriteChan  chan *Frame // chan Frame send to remote
	readyAcceptChan chan *Frame // chan Frame ready accept

	err    error // current err
	ctx    context.Context
	cancel context.CancelFunc

	// session config
	role         roleType
	transportTTL time.Duration

	// heartbeat config
	heartBeatSwitch        bool
	heartBeatInterval      time.Duration
	heartBeatSentTimestamp time.Time     // 发送心跳包的时间戳
	transportRtt           time.Duration // 根据心跳包计算出的 rtt

	// session buffer config
	bufferSize    int
	bufferAlloc   BufferAllocFunc
	bufferRecycle BufferRecycleFunc

	// private variable
	createdAt time.Time
}

type Option func(*Session)
type BufferAllocFunc func(size int) []byte
type BufferRecycleFunc func([]byte)

func WithRole(role roleType) Option {
	return func(session *Session) {
		session.role = role
	}
}

func WithHeartBeatSwitch(choose bool) Option {
	return func(session *Session) {
		session.heartBeatSwitch = choose
	}
}

func WithHeartBeatInterval(interval time.Duration) Option {
	return func(session *Session) {
		session.heartBeatInterval = interval
	}
}

func WithTTL(ttl time.Duration) Option {
	return func(session *Session) {
		session.transportTTL = ttl
	}
}

func WithBufferSize(sizeLimit int) Option {
	return func(session *Session) {
		session.bufferSize = sizeLimit
	}
}

func WithBufferManager(allocFunc BufferAllocFunc, recycleFunc BufferRecycleFunc) Option {
	return func(session *Session) {
		session.bufferAlloc = allocFunc
		session.bufferRecycle = recycleFunc
	}
}

func NewSessionContext(ctx context.Context, conn io.ReadWriteCloser, options ...Option) *Session {
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
		transportTTL:      60 * time.Second,
		heartBeatSwitch:   false,
		heartBeatInterval: 30 * time.Second,
		bufferSize:        1024,
		createdAt:         time.Now(),
	}

	for _, option := range options {
		option(session)
	}

	if session.bufferAlloc == nil && session.bufferRecycle == nil {
		session.bufferAlloc = dsaBuffer.Get
		session.bufferRecycle = dsaBuffer.Put
	}

	session.streamIdCounter = uint32(session.role) // 初始化 streamId 计数器

	// 维护 goroutine
	go session.recvLoop()
	go session.sendLoop()
	if session.heartBeatSwitch {
		go session.heartBeatLoop()
	}

	return session
}

func NewSession(conn io.ReadWriteCloser, options ...Option) *Session {
	return NewSessionContext(context.TODO(), conn, options...)
}

func (session *Session) StreamCount() int {
	return len(session.streams)
}

/*
get usable streamId
*/
func (session *Session) genStreamId() uint32 {
	return atomic.AddUint32(&session.streamIdCounter, 2)
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

func (session *Session) IsBusy() bool {
	return session.isBusy
}

func (session *Session) detectBusyFlag(duration time.Duration) {
	if duration > 100*time.Millisecond {
		session.isBusy = true
	} else {
		session.isBusy = false
	}
}

func (session *Session) recvLoop() {
	buffer := session.bufferAlloc(session.bufferSize)
	defer session.bufferRecycle(buffer)

	if len(buffer) < headerSize {
		session.CloseWithErr(BufferSizeLimitErr)
		return
	}

	ttlTicker := time.NewTicker(session.transportTTL)
	defer ttlTicker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ttlTicker.C:
			session.CloseWithErr(ErrSessionTTLExceed)
			return
		default:
			// 首先处理 header
			n, err := session.conn.Read(buffer[:headerSize])
			if err != nil {
				session.CloseWithErr(fmt.Errorf("session.recvLoop read header error: %w", err))
				return
			}

			if n < headerSize {
				session.CloseWithErr(fmt.Errorf("session.recvLoop read header error: %s", "read header size less than headerSize"))
				return
			}

			var header Frame

			_, err = header.UnMarshalHeader(buffer[:n])
			if err != nil {
				session.CloseWithErr(fmt.Errorf("session.recvLoop unmarshal header error: %w", err))
				return
			}

			ttlTicker.Reset(session.transportTTL) // 收到数据包，重置 ttl

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
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, ErrStreamIdNotFound) {
					// 此处没有拿到 stream，可能被关闭了，主动关闭连接时需通知远程进行关闭，此处读取剩余的数据包并丢弃
					hasRead := 0
					for hasRead < int(header.dataLength) {
						n, err := session.conn.Read(buffer[:header.dataLength])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}

						hasRead += n
					}
					continue
				}
				if err != nil {
					session.CloseWithErr(err)
					return
				}

				// 这个地方可能存在队头阻塞的问题，先加入 metrics 进行监控
				start := time.Now()
				select {
				case <-session.ctx.Done():
					return
				case <-stream.ctx.Done():
					// 当前 stream 被关闭了，读取剩下的数据包并丢弃
					hasRead := 0
					for hasRead < int(header.dataLength) {
						n, err := session.conn.Read(buffer[:header.dataLength])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}

						hasRead += n
					}
					continue
				case dataFrame := <-stream.readyReadChan:
					dispatchFrameDuration.Observe(time.Since(start).Seconds())
					session.detectBusyFlag(time.Since(start))

					// 注意这个地方需要处理拆包和粘包的问题
					if len(dataFrame.payload) < int(header.dataLength) {
						session.CloseWithErr(BufferSizeLimitErr)
						return
					}

					dataFrame.dataLength = header.dataLength
					hasRead := 0
					for hasRead < int(header.dataLength) {
						n, err := session.conn.Read(dataFrame.payload[hasRead:header.dataLength])
						if err != nil {
							dataFrame.Close()
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}

						hasRead += n
					}

					dataFrame.Close() //读取完成后关闭此 frame
				}
			case cmdFIN: // 收到远程的关闭通知，被动关闭
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, ErrStreamIdNotFound) {
					// 这个 stream 可能已经被关闭了,直接返回就可以了
					continue
				}
				// 被动关闭，不需要通知 remote
				stream.cancel()
				_ = session.unregisterStream(stream)
			case cmdPING:
				frame := NewFrameContext(session.ctx, cmdPONG, 0, nil)
				session.readyWriteChan <- frame
			case cmdPONG:
				session.transportRtt = time.Now().Sub(session.heartBeatSentTimestamp)
			}
		}
	}
}

func (session *Session) sendLoop() {
	buffer := session.bufferAlloc(session.bufferSize)
	defer session.bufferRecycle(buffer)

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

			_, err = session.conn.Write(buffer[:n])
			if err != nil {
				session.CloseWithErr(fmt.Errorf("session.sendLoop write header error: %w", err))
				return
			}

			if frame.cmd == cmdPSH {
				n, err = session.conn.Write(frame.payload[:frame.dataLength])
				if err != nil {
					session.CloseWithErr(fmt.Errorf("session.sendLoop write payload error: %w", err))
					return
				}
			}
			frame.Close() // 标记当前 frame 处理完毕
		}
	}
}

// 持续地发送心跳包
func (session *Session) heartBeatLoop() {
	ticker := time.NewTicker(session.heartBeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
			frame := NewFrameContext(session.ctx, cmdPING, 0, nil)
			session.readyWriteChan <- frame
			session.heartBeatSentTimestamp = time.Now()
		}
	}
}

func (session *Session) RTT() time.Duration {
	return session.transportRtt
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
		return nil, ErrStreamIdNotFound
	}
}

func (session *Session) registerStream(stream *Stream) error {
	session.streamMutex.Lock()
	defer session.streamMutex.Unlock()

	if _, ok := session.streams[stream.id]; ok {
		return ErrStreamIdDup
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
		return ErrStreamIdNotFound
	}
}

// OpenStream 创建一个新的 stream
func (session *Session) OpenStream(ctx context.Context) (*Stream, error) {
	if session.IsClose() {
		return nil, ErrSessionClosed
	}

	streamId := session.genStreamId()
	stream := session.newStream(streamId)
	frame := NewFrameContext(ctx, cmdSYN, streamId, nil)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-session.ctx.Done():
		return nil, session.ctx.Err()
	case session.readyWriteChan <- frame:
		select {
		case <-ctx.Done(): // 注意：存在 syn 发送，server 创建 stream 但 client 却没有创建的情况
			return nil, ctx.Err()
		case <-session.ctx.Done():
			return nil, session.ctx.Err()
		case <-frame.ctx.Done():
			err := session.registerStream(stream)
			if err != nil {
				session.CloseWithErr(err)
				return nil, err
			}
			return stream, nil
		}
	}
}

func (session *Session) AcceptStream(ctx context.Context) (*Stream, error) {
	select {
	case <-session.ctx.Done():
		return nil, fmt.Errorf("%w, %s", ErrSessionClosed, session.err.Error())
	case <-ctx.Done():
		return nil, ctx.Err()
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
