package mux

import (
	"context"
	"encoding/binary"
	"errors"
)

type cmdType byte

const (
	cmdSyn cmdType = iota // stream open
	cmdSynAck
	cmdFin // stream close, a.k.a EOF mark
	cmdFinAck
	cmdPsh // payload push
	cmdPshAck
	cmdPing // ping
	cmdPong // pong
)

const (
	sizeOfCmd      = 1
	sizeOfStreamId = 4
	sizeOfLength   = 2
	headerSize     = sizeOfCmd + sizeOfStreamId + sizeOfLength
)

const (
	maxPayloadSize = 0xFFFF
)

var (
	BufferSizeLimitErr = errors.New("buffer size limit err")
	UnknownCmdErr      = errors.New("cmd unknown err")
)

type Frame struct {
	cmd        cmdType
	streamId   uint32
	dataLength uint16 // Limit: 2^16 payload size less than 64KB
	payload    []byte
	ctx        context.Context
	cancel     context.CancelFunc
}

func (frame *Frame) Cmd() byte {
	return byte(frame.cmd)
}

func (frame *Frame) StreamId() uint32 {
	return frame.streamId
}

func (frame *Frame) checkCmd() error {
	switch frame.cmd {
	case cmdSyn, cmdFin, cmdPsh, cmdPing, cmdPong:
		return nil
	default:
		return UnknownCmdErr
	}
}

func (frame *Frame) Close() {
	frame.cancel()
}

func NewFrameContext(ctx context.Context, cmd cmdType, streamId uint32, data []byte) *Frame {
	currentCtx, cancel := context.WithCancel(ctx)
	return &Frame{
		cmd:        cmd,
		streamId:   streamId,
		dataLength: uint16(len(data)),
		payload:    data,
		ctx:        currentCtx,
		cancel:     cancel,
	}
}

func (frame *Frame) MarshalHeader(buffer []byte) (int, error) {
	if err := frame.checkCmd(); err != nil {
		return 0, err
	}

	if len(buffer) < headerSize {
		return 0, BufferSizeLimitErr
	}

	buffer[0] = frame.Cmd()
	binary.BigEndian.PutUint32(buffer[sizeOfCmd:], frame.streamId)
	binary.BigEndian.PutUint16(buffer[sizeOfCmd+sizeOfStreamId:], frame.dataLength)
	return headerSize, nil
}

func (frame *Frame) UnMarshalHeader(buffer []byte) (int, error) {
	if len(buffer) < headerSize {
		return 0, BufferSizeLimitErr
	}

	frame.cmd = cmdType(buffer[0])
	if err := frame.checkCmd(); err != nil {
		return 0, err
	}

	frame.streamId = binary.BigEndian.Uint32(buffer[sizeOfCmd:])
	frame.dataLength = binary.BigEndian.Uint16(buffer[sizeOfCmd+sizeOfStreamId:])
	return headerSize, nil
}
