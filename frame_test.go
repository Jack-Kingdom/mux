package mux

import (
	"context"
	"testing"
)

func TestFrame_MarshalHeader(t *testing.T) {
	frame := NewFrameContext(context.TODO(), cmdSYN, 1, []byte{1, 2, 3})

	buffer := make([]byte, 100)
	n, err := frame.MarshalHeader(buffer)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(buffer[:n], n)
}

func TestFrame_UnMarshalHeader(t *testing.T) {
	buffer := []byte{0, 0, 0, 0, 1, 0, 3, 1, 2, 3}

	var frame Frame
	n, err := frame.UnMarshalHeader(buffer)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(frame, n)
}
