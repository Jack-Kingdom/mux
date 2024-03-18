package mux_test

import (
	"context"
	"github.com/Jack-Kingdom/mux"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// MemoryConnType In-memory connection, used for testing
type MemoryConnType struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

func (conn *MemoryConnType) Read(b []byte) (int, error) {
	return conn.reader.Read(b)
}

func (conn *MemoryConnType) Write(b []byte) (int, error) {
	return conn.writer.Write(b)
}

func (conn *MemoryConnType) Close() error {
	return conn.writer.Close()
}

func (conn *MemoryConnType) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "memory", Net: "memory"}
}

func (conn *MemoryConnType) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "memory", Net: "memory"}
}

func (conn *MemoryConnType) SetDeadline(t time.Time) error {
	return nil
}

func (conn *MemoryConnType) SetReadDeadline(t time.Time) error {
	return nil
}

func (conn *MemoryConnType) SetWriteDeadline(t time.Time) error {
	return nil
}

func NewMemoryConnPeer() (*MemoryConnType, *MemoryConnType) {
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()
	return &MemoryConnType{clientReader, clientWriter}, &MemoryConnType{serverReader, serverWriter}
}

const (
	bufferLength = 8 * 1024
	Mb           = 1024 * 1024
)

var (
	testPayload = "helloworld"
	mutex       sync.Mutex
)

func TestSession(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	client, server := NewMemoryConnPeer()
	defer client.Close()
	defer server.Close()

	go func() {
		serverSession := mux.NewSession(server, mux.WithRole(mux.RoleServer), mux.WithBufferSize(bufferLength), mux.WithHeartBeatTTL(5*time.Second))

		for {
			stream, err := serverSession.AcceptStream(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("stream accept.")

			buffer := make([]byte, bufferLength)
			n, err := stream.Read(buffer)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream read: %s", buffer[:n])

			_, err = stream.Write(buffer[:n])
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("server stream write: %s", buffer[:n])

			//_ = stream.Close()
		}
	}()

	clientSession := mux.NewSession(client, mux.WithRole(mux.RoleClient), mux.WithBufferSize(bufferLength), mux.WithHeartBeatSwitch(true), mux.WithHeartBeatInterval(2*time.Second))

	for i := 0; i < 4; i++ {
		stream, err := clientSession.OpenStream(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream opened.")

		_, err = stream.Write([]byte(testPayload))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream write: %s", testPayload)

		buffer := make([]byte, bufferLength)
		n, err := stream.Read(buffer)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("client stream read: %s", buffer[:n])

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

		client, server := NewMemoryConnPeer()
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)

		clientSession := mux.NewSession(client, mux.WithRole(mux.RoleClient), mux.WithBufferSize(bufferLength))
		serverSession := mux.NewSession(server, mux.WithRole(mux.RoleServer), mux.WithBufferSize(bufferLength))

		clientStream, err := clientSession.OpenStream(ctx)
		if err != nil {
			b.Errorf("open stream error: %s", err)
		}

		serverStream, err := serverSession.AcceptStream(ctx)
		if err != nil {
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
