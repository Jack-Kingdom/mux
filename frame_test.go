package mux

import "testing"

func TestFrame_Marshal(t *testing.T) {
	frame := NewFrame(cmdSYN, 1, []byte{1, 2, 3})

	buffer := make([]byte, 100)
	n, err := frame.Marshal(buffer)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(buffer[:n], n)
}

func TestFrame_UnMarshal(t *testing.T) {
	buffer := []byte{0, 0, 0, 0, 1, 0, 3, 1, 2, 3}

	var frame Frame
	n, err := frame.UnMarshal(buffer)
	if err !=nil {
		t.Fatal(err)
	}
	t.Log(frame, n)
}
