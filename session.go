package mux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	dsaBuffer "github.com/Jack-Kingdom/go-dsa/buffer"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	ErrBufferLimited    = errors.New("buffer length limited")
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

type Session struct {
	conn                   io.ReadWriteCloser
	openStreams            map[uint32]*Stream
	openStreamsMutex       sync.Mutex
	establishedStreams     map[uint32]*Stream
	establishedStreamMutex sync.Mutex
	streamIdCounter        uint32

	finalizers      []func() // todo it's too complicated, simplify it
	finalizersMutex sync.Mutex

	readyWriteChan  chan *Frame // chan Frame send to remote
	readyAcceptChan chan *Frame // chan Frame ready accept

	err    error // current err
	ctx    context.Context
	cancel context.CancelFunc

	// session config
	role roleType

	// heartbeat config
	heartBeatSwitch   bool
	heartBeatInterval time.Duration
	heartBeatTTL      time.Duration
	heartBeatRtt      time.Duration // todo this variable not use at now

	// buffer config
	bufferSize    int
	bufferAlloc   BufferAllocFunc
	bufferRecycle BufferRecycleFunc

	// metrics
	writeFrameDurations   prometheus.Histogram
	recvFrameDurations    prometheus.Histogram
	acceptStreamDurations prometheus.Histogram

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

func WithFinalizer(finalizer func()) Option {
	return func(session *Session) {
		session.finalizers = append(session.finalizers, finalizer)
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

func WithMetrics(writeFrameDurations, recvFrameDurations, acceptStreamDurations prometheus.Histogram) Option {
	return func(session *Session) {
		session.writeFrameDurations = writeFrameDurations
		session.recvFrameDurations = recvFrameDurations
		session.acceptStreamDurations = acceptStreamDurations
	}
}

func observe(metrics prometheus.Histogram, start time.Time) {
	if metrics == nil {
		return
	}
	metrics.Observe(time.Since(start).Seconds())
}

func NewSessionContext(ctx context.Context, conn io.ReadWriteCloser, options ...Option) *Session {
	currentCtx, cancel := context.WithCancel(ctx)
	session := &Session{
		conn:               conn,
		establishedStreams: make(map[uint32]*Stream, 64),
		readyWriteChan:     make(chan *Frame),
		readyAcceptChan:    make(chan *Frame),
		err:                nil,
		ctx:                currentCtx,
		cancel:             cancel,

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

func (session *Session) finalize() {
	session.finalizersMutex.Lock()
	defer session.finalizersMutex.Unlock()

	for _, finalizer := range session.finalizers {
		finalizer()
	}

	if len(session.finalizers) > 0 {
		session.finalizers = session.finalizers[:0] // clear executed finalizers
	}
}

func (session *Session) Close() error {
	session.cancel()
	_ = session.conn.Close()

	session.finalize()
	return session.err
}

func (session *Session) Err() error {
	return session.err
}

func (session *Session) CloseWithErr(err error) { // todo remove this method
	session.err = err
	_ = session.Close()
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
					// stream not found, perhaps this stream has been closed. just read and discard it
					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(buffer[:session.bufferSize])
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

				startReceiveDataTimestamp := time.Now()
			startReceiveData:

				select {
				case <-session.ctx.Done():
					return
				case <-stream.ctx.Done():
					// this stream has been closed, just read and discard it
					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(buffer[:session.bufferSize])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}
					}
					continue
				case dataFrame := <-stream.readyReadChan:
					dispatchFrameDuration.Observe(time.Since(startReceiveDataTimestamp).Seconds())

					// receive buffer less than dataLength, just fill this buffer first and read again
					if len(dataFrame.payload) < int(header.dataLength) {
						zap.L().Warn("receive frame payload larger than stream buffer", zap.Uint16("frame-length", header.dataLength), zap.Uint16("buffer-length", dataFrame.dataLength))
						dataFrame.dataLength = uint16(len(dataFrame.payload))
						for hasRead := 0; hasRead < int(dataFrame.dataLength); hasRead += n {
							n, err = session.conn.Read(dataFrame.payload[hasRead:dataFrame.dataLength])
							if err != nil {
								session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
								return
							}
						}
						dataFrame.Close()

						header.dataLength -= dataFrame.dataLength
						goto startReceiveData // read again
					}

					dataFrame.dataLength = header.dataLength
					for hasRead := 0; hasRead < int(header.dataLength); hasRead += n {
						n, err = session.conn.Read(dataFrame.payload[hasRead:header.dataLength])
						if err != nil {
							session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
							return
						}
					}
					dataFrame.Close()
				}
			case cmdFin:
				stream, err := session.getStream(header.streamId)
				if err != nil && errors.Is(err, ErrStreamIdNotFound) {
					// stream not found, perhaps this stream has been closed. ignore it
					continue
				}
				stream.silenceClose()
			case cmdPing:
				ttlTicker.Reset(session.heartBeatTTL) // receive heartbeat, reset ttlTicker

				err = readUtilLength(session.conn, buffer, int(header.dataLength))
				if err != nil {
					session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
					return
				}

				frame := NewFrameContext(session.ctx, cmdPong, 0, buffer[:header.dataLength])
				session.readyWriteChan <- frame
			case cmdPong:
				if header.dataLength != heartBeatPayloadSize {
					session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %s", "read pong data length error"))
					return
				}

				err = readUtilLength(session.conn, buffer, int(header.dataLength))
				if err != nil {
					session.CloseWithErr(fmt.Errorf("session.recvLoop read data error: %w", err))
					return
				}

				unixMilli := binary.BigEndian.Uint64(buffer[:heartBeatPayloadSize])
				start := time.UnixMilli(int64(unixMilli))
				session.heartBeatRtt = time.Since(start)
			default:
				session.CloseWithErr(UnknownCmdErr)
				return
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
			n, err := frame.MarshalHeader(buffer[:session.bufferSize])
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
		}
	}
}

const (
	heartBeatPayloadSize = 8
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
		case <-ctx.Done(): // todo: bug? Syn sent to remote, but ctx is done, server created stream, but client not
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

func readUtilLength(conn io.ReadWriteCloser, buf []byte, length int) error {
	if len(buf) < length {
		return ErrBufferLimited
	}

	var n int
	var err error
	for hasRead := 0; hasRead < length; hasRead += n {
		n, err = conn.Read(buf[hasRead:length])
		if err != nil {
			return err
		}
	}
	return nil
}
