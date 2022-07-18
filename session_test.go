package mux

import (
	"context"
	"github.com/rabbit-proxy/transport"
	"go.uber.org/zap"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

const (
	bufferLength = 8 * 1024
)

var (
	testPayload = "helloworld"
)

func TestSession(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 5 * time.Second)
	defer cancel()

	go func() {
		listener, err := net.Listen("tcp", "localhost:8843")
		defer listener.Close()

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

		serverSession := NewSession(transport.NewConnSocket(conn), WithRole(RoleServer), WithBufferSize(bufferLength))

		for {
			stream, err := serverSession.AcceptStream(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("stream %d accept.", stream.id)

			buffer := serverSession.getBuffer()
			n, err := stream.Read(ctx, buffer)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream %d read: %s", stream.id, buffer[:n])

			_, err = stream.Write(ctx, buffer[:n])
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream %d write: %s", stream.id, buffer[:n])

			_ = stream.Close()
		}
	}()

	conn, err := net.Dial("tcp", "localhost:8843")
	defer conn.Close()

	if err != nil {
		t.Error(err)
		return
	}

	clientSession := NewSession(transport.NewConnSocket(conn), WithRole(RoleClient), WithHeartBeatInterval(10*time.Second), WithBufferSize(bufferLength))

	for i := 0; i < 4; i++ {
		stream, err := clientSession.OpenStream(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d opened.", stream.id)

		_, err = stream.Write(ctx, []byte(testPayload))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d write: %s", stream.id, testPayload)

		buffer := make([]byte, bufferLength)
		n, err := stream.Read(ctx, buffer)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream %d read: %s", stream.id, buffer[:n])

		_ = stream.Close()
	}
}
