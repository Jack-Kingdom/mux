package mux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	dsaBuffer "github.com/Jack-Kingdom/go-dsa/buffer"
	"go.uber.org/zap"
	"io"
	"math"
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
	RoleClient roleType = 1 + iota // client start with odd number, server start with even number
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

type NoneType struct{}

type Session struct {
	conn                   io.ReadWriteCloser
	openStreams            map[uint32]*Stream
	openStreamsMutex       sync.Mutex
	establishedStreams     map[uint32]*Stream
	establishedStreamMutex sync.Mutex
	streamIdCounter        uint32
	busyFlag               int32 // 0: false, 1: true
	busyTriggerChan        chan NoneType
	idleTriggerChan        chan NoneType
	busyTimestamp          int64

	readyWriteChan  chan *Frame // chan Frame send to remote
	readyAcceptChan chan *Frame // chan Frame ready accept

	err    error // current err
	ctx    context.Context
	cancel context.CancelFunc

	// session config
	role         roleType

	// heartbeat config
	heartBeatSwitch        bool
	heartBeatInterval      time.Duration
	heartBeatTTL time.Duration

	// transport relevant
	transportRtt         time.Duration
	transportSRtt        time.Duration
	transportRttVar      time.Duration
	transportRto         time.Duration
	transportMinRtoBound time.Duration
	transportMaxRtoBound time.Duration

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

func WithHeartBeatTTL(ttl time.Duration) Option {
	return func(session *Session) {
		session.heartBeatTTL = ttl
	}
}

func WithTransportRtoBound(min, max time.Duration) Option {
	return func(session *Session) {
		session.transportMinRtoBound = min
		session.transportMaxRtoBound = max
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
		conn:               conn,
		establishedStreams: make(map[uint32]*Stream, 64),

		busyTriggerChan: make(chan NoneType),
		idleTriggerChan: make(chan NoneType),
		readyWriteChan:  make(chan *Frame),
		readyAcceptChan: make(chan *Frame),
		err:             nil,
		ctx:             currentCtx,
		cancel:          cancel,

		role:              RoleClient,
		heartBeatSwitch:   false,
		heartBeatInterval: 30 * time.Second,
		heartBeatTTL:      365 * 24 * time.Hour,
		bufferSize:        1024,
		createdAt:         time.Now(),
	}

	for _, option := range options {
		option(session)
	}

	if session.transportMinRtoBound == 0 || session.transportMaxRtoBound == 0 {
		session.transportMinRtoBound = 100 * time.Millisecond
		session.transportMaxRtoBound = 500 * time.Millisecond
	}

	if session.bufferAlloc == nil && session.bufferRecycle == nil {
		session.bufferAlloc = dsaBuffer.Get
		session.bufferRecycle = dsaBuffer.Put
	}

	session.streamIdCounter = uint32(session.role) // 初始化 streamId 计数器

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
	return len(session.establishedStreams)
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
	return time.Since(session.createdAt)
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

	sessionLifetimeDurationSummary.Observe(session.Lifetime().Seconds())

	return session.err
}

func (session *Session) CloseWithErr(err error) {
	session.err = err
	_ = session.Close()
}

func (session *Session) IsBusy() bool {
	return atomic.LoadInt32(&session.busyFlag) == 0
}

func (session *Session) BusyTrigger() <-chan NoneType {
	return session.busyTriggerChan
}

func (session *Session) IdleTrigger() <-chan NoneType {
	return session.idleTriggerChan
}

func (session *Session) acquireBusyFlag() {
	select {
	case session.busyTriggerChan <- NoneType{}:
	default:
		// do nothing
	}

	atomic.StoreInt64(&session.busyTimestamp, time.Now().UnixMilli())
	atomic.AddInt32(&session.busyFlag, 1)
}

func (session *Session) releaseBusyFlag() {
	busyTimestamp := atomic.LoadInt64(&session.busyTimestamp)
	idleInterval := time.Now().UnixMilli() - busyTimestamp

	// if idle interval is greater than 2 * RTO, trigger idle event
	if atomic.LoadInt32(&session.busyFlag) == 0 && idleInterval > 2*session.Rto().Milliseconds() {
		select {
		case session.idleTriggerChan <- NoneType{}:
		default:
			// do nothing
		}
		atomic.StoreInt32(&session.busyFlag, 0)
	}
}

func (session *Session) recvLoop() {
	buffer := session.bufferAlloc(session.bufferSize)
	defer session.bufferRecycle(buffer)

	if len(buffer) < headerSize {
		session.CloseWithErr(BufferSizeLimitErr)
		return
	}

	ttlTicker := time.NewTicker(session.heartBeatTTL)
	defer ttlTicker.Stop()

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ttlTicker.C:
			session.CloseWithErr(ErrSessionTTLExceed)
			return
		default:
			session.acquireBusyFlag()
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

			switch header.cmd {
			case cmdSyn:
				synFrame := NewFrameContext(session.ctx, cmdSyn, header.streamId, nil)
				select {
				case <-session.ctx.Done():
					return
				case session.readyAcceptChan <- synFrame:
					select {
					case <-session.ctx.Done():
					case <-synFrame.ctx.Done():
					}
				}
			case cmdPsh:
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, ErrStreamIdNotFound) {
					// 此处没有拿到 stream，可能被关闭了，主动关闭连接时需通知远程进行关闭，此处读取剩余的数据包并丢弃

					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(buffer[:header.dataLength])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}
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

					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(buffer[:header.dataLength])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}
					}
					continue
				case dataFrame := <-stream.readyReadChan:
					dispatchFrameDuration.Observe(time.Since(start).Seconds())

					// 注意这个地方需要处理拆包和粘包的问题
					if len(dataFrame.payload) < int(header.dataLength) {
						session.CloseWithErr(BufferSizeLimitErr)
						return
					}

					dataFrame.dataLength = header.dataLength
					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(dataFrame.payload[hasRead:header.dataLength])
						if err != nil {
							dataFrame.Close()
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}
					}
					dataFrame.Close() //读取完成后关闭此 frame
				}
			case cmdFin: // 收到远程的关闭通知，被动关闭
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, ErrStreamIdNotFound) {
					// 这个 stream 可能已经被关闭了,直接返回就可以了
					continue
				}
				// 被动关闭，不需要通知 remote
				stream.cancel()
				_ = session.unregisterStream(stream)
			case cmdPing:
				ttlTicker.Reset(session.heartBeatTTL) // receive heartbeat, reset ttlTicker

				for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
					n, err = session.conn.Read(buffer[hasRead:header.dataLength])
					if err != nil {
						session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
						return
					}
				}
				frame := NewFrameContext(session.ctx, cmdPong, 0, buffer[:header.dataLength])
				session.readyWriteChan <- frame
			case cmdPong:
				if header.dataLength != heartBeatPayloadSize {
					session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %s", "read pong data length error"))
					return
				}

				for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
					n, err = session.conn.Read(buffer[hasRead:header.dataLength])
					if err != nil {
						session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
						return
					}
				}

				unixMilli := binary.BigEndian.Uint64(buffer[:heartBeatPayloadSize])
				start := time.UnixMilli(int64(unixMilli))

				// ref: https://www.rfc-editor.org/rfc/rfc6298
				session.transportRtt = time.Since(start)
				session.transportSRtt = time.Duration((1-sRttSmoothingFactor)*float64(session.SRtt()) + sRttSmoothingFactor*float64(session.Rtt()))
				session.transportRttVar = time.Duration((1-rttVarBeta)*float64(session.RttVar()) + rttVarBeta*math.Abs(float64(session.Rtt())-float64(session.SRtt())))
				session.transportRto = session.transportSRtt + rtoK*session.transportRttVar

				rttDuration.Observe(session.Rto().Seconds()) // todo remove this
			}

			session.releaseBusyFlag()
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
			startTimestamp := time.Now()
			session.acquireBusyFlag()

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

			if frame.dataLength > 0 {
				n, err = session.conn.Write(frame.payload[:frame.dataLength])
				if err != nil {
					session.CloseWithErr(fmt.Errorf("session.sendLoop write payload error: %w", err))
					return
				}
			}
			frame.Close() // flag current frame as sent

			sendFrameDuration.Observe(time.Since(startTimestamp).Seconds())
			session.releaseBusyFlag()
		}
	}
}

const (
	heartBeatPayloadSize = 8
	sRttSmoothingFactor  = 0.125
	rttVarBeta           = 0.25
	rtoK                 = 4
)

func (session *Session) heartBeatLoop() {
	ticker := time.NewTicker(session.heartBeatInterval)
	defer ticker.Stop()

	heartBeatPayload := session.bufferAlloc(heartBeatPayloadSize)
	defer session.bufferRecycle(heartBeatPayload)

	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:

			unixMilli := uint64(time.Now().UnixMilli())
			binary.BigEndian.PutUint64(heartBeatPayload, unixMilli)

			frame := NewFrameContext(session.ctx, cmdPing, 0, heartBeatPayload[:heartBeatPayloadSize])
			session.readyWriteChan <- frame
		}
	}
}

func (session *Session) Rtt() time.Duration {
	// assume 500ms if rtt is 0
	if session.transportRtt == 0 {
		return 500 * time.Millisecond
	}

	return session.transportRtt
}

func (session *Session) SRtt() time.Duration {
	if session.transportSRtt == 0 {
		return session.Rtt()
	}

	return session.transportSRtt
}

func (session *Session) RttVar() time.Duration {
	if session.transportRttVar == 0 {
		return session.Rtt() / 2
	}

	return session.transportRttVar
}

func (session *Session) Rto() time.Duration {
	if session.transportRto == 0 {
		return session.SRtt() + rtoK*session.RttVar()
	}

	if session.transportRto < session.transportMinRtoBound {
		zap.L().Warn("transportRto is too small, use transportMinRtoBound instead", zap.Duration("transportRto", session.transportRto), zap.Duration("transportMinRtoBound", session.transportMinRtoBound))
		return session.transportMinRtoBound
	}
	if session.transportRto > session.transportMaxRtoBound {
		zap.L().Warn("transportRto is too large, use transportMaxRtoBound instead", zap.Duration("transportRto", session.transportRto), zap.Duration("transportMaxRtoBound", session.transportMaxRtoBound))
		return session.transportMaxRtoBound
	}
	return session.transportRto
}

func (session *Session) newStream(streamId uint32) *Stream {
	ctx, cancel := context.WithCancel(session.ctx)
	return &Stream{
		id:            streamId,
		session:       session,
		readyReadChan: make(chan *Frame),
		ctx:           ctx,
		cancel:        cancel,
		createdAt:     time.Now(),
	}
}

func (session *Session) getStream(streamId uint32) (*Stream, error) {
	session.establishedStreamMutex.Lock()
	defer session.establishedStreamMutex.Unlock()

	if stream, ok := session.establishedStreams[streamId]; ok {
		return stream, nil
	} else {
		return nil, ErrStreamIdNotFound
	}
}

func (session *Session) registerStream(stream *Stream) error {
	session.establishedStreamMutex.Lock()
	defer session.establishedStreamMutex.Unlock()

	if _, ok := session.establishedStreams[stream.id]; ok {
		return ErrStreamIdDup
	}

	session.establishedStreams[stream.id] = stream
	return nil
}

func (session *Session) unregisterStream(stream *Stream) error {
	session.establishedStreamMutex.Lock()
	defer session.establishedStreamMutex.Unlock()

	if _, ok := session.establishedStreams[stream.id]; ok {
		delete(session.establishedStreams, stream.id)
		return nil
	} else {
		return ErrStreamIdNotFound
	}
}

// OpenStreamNoDelay todo
// OpenStreamNoDelay create a stream immediately, not confirm another side stream established
func (session *Session) OpenStreamNoDelay(ctx context.Context) (*Stream, error) {
	return nil, nil
}

// OpenStream create a new established stream connection
func (session *Session) OpenStream(ctx context.Context) (*Stream, error) {
	if session.IsClose() {
		return nil, ErrSessionClosed
	}

	streamId := session.genStreamId()
	stream := session.newStream(streamId)
	frame := NewFrameContext(ctx, cmdSyn, streamId, nil)
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
