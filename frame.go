package mux

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
)

type cmdType byte

const (
	cmdSYN cmdType = iota // stream open
	cmdFIN                // stream close, a.k.a EOF mark
	cmdPSH                // data push
	cmdNOP                // no operation
)

const (
	sizeOfCmd      = 1
	sizeOfStreamId = 4
	sizeOfLength   = 2
	headerSize     = sizeOfCmd + sizeOfStreamId + sizeOfLength
)

var (
	BufferSizeLimitErr = errors.New("buffer size limit err")
	UnknownCmdErr      = errors.New("cmd unknown err")
)

type Frame struct {
	cmd      cmdType
	streamId uint32
	data     []byte
	ctx      context.Context
	cancel   context.CancelFunc
}

func (frame *Frame) Cmd() byte {
	return byte(frame.cmd)
}

func (frame *Frame) StreamId() uint32 {
	return frame.streamId
}

func (frame *Frame) checkCmd() error {
	if frame.cmd != cmdSYN && frame.cmd != cmdFIN && frame.cmd != cmdPSH && frame.cmd != cmdNOP {
		return UnknownCmdErr
	}
	return nil
}

func (frame *Frame) Close() error {
	frame.cancel()
	return nil
}

func NewFrame(cmd cmdType, streamId uint32, data []byte) *Frame {
	return NewFrameContext(context.TODO(), cmd, streamId, data)
}

func NewFrameContext(ctx context.Context, cmd cmdType, streamId uint32, data []byte) *Frame {
	currentCtx, cancel := context.WithCancel(ctx)
	return &Frame{
		cmd:      cmd,
		streamId: streamId,
		data:     data,
		ctx:      currentCtx,
		cancel:   cancel,
	}
}

func (frame *Frame) Marshal(buffer []byte) (int, error) {
	if err := frame.checkCmd(); err != nil {
		return 0, err
	}

	totalSize := headerSize + len(frame.data)
	if len(buffer) < totalSize {
		return 0, BufferSizeLimitErr
	}
	buffer[0] = frame.Cmd()
	binary.BigEndian.PutUint32(buffer[sizeOfCmd:], frame.streamId)
	binary.BigEndian.PutUint16(buffer[sizeOfCmd+sizeOfStreamId:], uint16(len(frame.data)))
	copy(buffer[headerSize:], frame.data)
	return totalSize, nil
}

func (frame *Frame) UnMarshal(buffer []byte) (int, error) {
	if len(buffer) < headerSize {
		return 0, errors.Wrap(BufferSizeLimitErr, "buffer length less than headerSize")
	}

	frame.cmd = cmdType(buffer[0])
	if err := frame.checkCmd(); err != nil {
		return 0, err
	}

	frame.streamId = binary.BigEndian.Uint32(buffer[sizeOfCmd:])

	dataLength := int(binary.BigEndian.Uint16(buffer[sizeOfCmd+sizeOfStreamId:]))
	if len(buffer) < headerSize+dataLength {
		return 0, errors.Wrap(BufferSizeLimitErr, fmt.Sprintf("buffer length %d less than protocol showed %d", len(buffer), headerSize+dataLength))
	}
	frame.data = buffer[headerSize : headerSize+dataLength]
	used := headerSize + dataLength

	return used, nil
}
