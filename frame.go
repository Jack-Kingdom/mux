package mux

import (
	"context"
	"encoding/binary"
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

	ctx, cancel := context.WithCancel(context.TODO())

	return &Frame{
		cmd:      cmd,
		streamId: streamId,
		data:     data,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (frame *Frame) Marshal(buffer []byte) (int, error) {
	switch frame.cmd {
	case cmdSYN, cmdFIN, cmdNOP:
		headerSize := sizeOfCmd + sizeOfStreamId
		if len(buffer) < headerSize {
			return 0, BufferSizeLimitErr
		}

		buffer[0] = frame.Cmd()
		binary.BigEndian.PutUint32(buffer[sizeOfCmd:], frame.streamId)
		return headerSize, nil
	case cmdPSH:
		headerSize := sizeOfCmd + sizeOfStreamId + sizeOfLength
		totalSize := headerSize + len(frame.data)
		if len(buffer) < totalSize {
			return 0, BufferSizeLimitErr
		}
		buffer[0] = frame.Cmd()
		binary.BigEndian.PutUint32(buffer[sizeOfCmd:], frame.streamId)
		binary.BigEndian.PutUint16(buffer[sizeOfCmd+sizeOfStreamId:], uint16(len(frame.data)))
		copy(buffer[headerSize:], frame.data)
		return totalSize, nil
	default:
		return 0, UnknownCmdErr
	}
}

func (frame *Frame) UnMarshal(buffer []byte) (int, error) {
	headerSize := sizeOfCmd + sizeOfStreamId
	if len(buffer) < headerSize {
		return 0, BufferSizeLimitErr
	}

	frame.cmd = cmdType(buffer[0])
	if err := frame.checkCmd(); err != nil {
		return 0, err
	}

	frame.streamId = binary.BigEndian.Uint32(buffer[sizeOfCmd:])

	used := headerSize

	if frame.cmd == cmdPSH {
		dataLength := int(binary.BigEndian.Uint16(buffer[headerSize:]))
		if len(buffer) < headerSize+sizeOfLength+dataLength {
			return 0, BufferSizeLimitErr
		}
		frame.data = buffer[headerSize+sizeOfLength : headerSize+sizeOfLength+dataLength]
		used += sizeOfLength + dataLength
	}
	return used, nil
}
