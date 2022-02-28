package mux

import (
	"go.uber.org/zap"
	"net"
	"testing"
)

const (
	bufferLength = 8 * 1024
	bufferLimit  = 6 * 1024
)

var (
	testPayload = "helloworld"
)

func TestSession(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	go func() {
		listener, err := net.Listen("tcp", "localhost:8843")
		if err != nil {
			t.Error(err)
			return
		}

		// 只测试一个主连接就可以了
		conn, err := listener.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("conn accept.")

		session := NewSession(conn,
			WithRole(RoleServer),
			WithBufferSizeLimit(bufferLimit),
			WithBufferAllocFunc(func() []byte {
				return make([]byte, bufferLength)
			}),
			WithBufferRecycleFunc(func(buffer []byte) {
			}),
		)

		for {
			stream, err := session.AcceptStream()
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("stream %d accept.", stream.id)

			buffer := session.getBuffer()
			n, err := stream.Read(buffer)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream %d read: %s", stream.id, buffer[:n])

			_, err = stream.Write(buffer[:n])
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream %d write: %s", stream.id, buffer[:n])

			err = stream.Close()
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", "localhost:8843")
	if err != nil {
		t.Error(err)
		return
	}

	session := NewSession(conn,
		WithRole(RoleClient),
		WithBufferSizeLimit(bufferLimit),
		WithBufferAllocFunc(func() []byte {
			return make([]byte, bufferLength)
		}),
		WithBufferRecycleFunc(func(buffer []byte) {
		}),
	)

	for i := 0; i < 4; i++ {
		stream, err := session.OpenStream()
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d opened.", stream.id)

		_, err = stream.Write([]byte(testPayload))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d write: %s", stream.id, testPayload)

		buffer := make([]byte, bufferLength)
		n, err := stream.Read(buffer)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d read: %s", stream.id, buffer[:n])

		err = stream.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}
}
