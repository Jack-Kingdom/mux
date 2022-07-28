package mux

import (
	"context"
	dsaIo "github.com/Jack-Kingdom/go-dsa/io"
	"go.uber.org/zap"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"testing"
	"time"
)

const (
	bufferLength = 8 * 1024
	Mb           = 1024 * 1024
)

var (
	testPayload = "helloworld"
	mutex sync.Mutex
)

func TestSession(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 5 * time.Second)
	defer cancel()

	client, server := dsaIo.NewMemoryConnPeer()
	defer client.Close()
	defer server.Close()

	go func() {
		serverSession := NewSession(server, WithRole(RoleServer), WithBufferSize(bufferLength))

		for {
			stream, err := serverSession.AcceptStream(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("stream %d accept.", stream.id)

			buffer := serverSession.getBuffer()
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

			//_ = stream.Close()
		}
	}()

	clientSession := NewSession(client, WithRole(RoleClient), WithBufferSize(bufferLength))

	for i := 0; i < 4; i++ {
		stream, err := clientSession.OpenStream(ctx)
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

		_ = stream.Close()
	}
}

func BenchmarkSession(b *testing.B) {
	testReader := func(ctx context.Context, conn io.ReadWriteCloser) {
		buffer := make([]byte, bufferLength)
		hasRead := 0
		for {
			select {
			case <-ctx.Done():
				mutex.Lock()
				b.ReportMetric(float64(hasRead)/Mb, "MB")
				mutex.Unlock()
				return
			default:
				n, err := conn.Read(buffer[:bufferLength-1024])
				if err != nil {
					b.Errorf("read error: %s", err)
				}
				hasRead += n
			}
		}
	}

	testWriter := func(ctx context.Context, conn io.ReadWriteCloser) {
		buffer := make([]byte, bufferLength)
		hasWrite := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n, err := conn.Write(buffer[:bufferLength-1024])
				if err != nil {
					b.Errorf("read error: %s", err)
				}
				hasWrite += n
			}
		}
	}

	for i := 0; i < b.N; i++ {
		b.ReportAllocs()

		client, server := dsaIo.NewMemoryConnPeer()
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)

		clientSession := NewSession(client, WithRole(RoleClient), WithBufferSize(bufferLength))
		serverSession := NewSession(server, WithRole(RoleServer), WithBufferSize(bufferLength))

		clientStream, err := clientSession.OpenStream(ctx)
		if err!=nil {
			b.Errorf("open stream error: %s", err)
		}

		serverStream, err := serverSession.AcceptStream(ctx)
		if err!=nil {
			b.Errorf("accept stream error: %s", err)
		}

		go testReader(ctx, clientStream)
		go testWriter(ctx, serverStream)
		go testReader(ctx, serverStream)
		go testWriter(ctx, clientStream)

		time.Sleep(1 * time.Second)
		cancel()
	}
}