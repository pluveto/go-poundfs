package main

import (
	"testing"
)

func TestNewFileBlockDevice(t *testing.T) {
	// 50MB
	blockcount := uint64(50 * 1024 * 1024 / 512)
	dev, err := NewFileBlockDevice("./device.bin", blockcount)
	if err != nil {
		t.Error(err)
	}
	if dev == nil {
		t.Error("dev is nil")
	}
	fill1 := make([]byte, BlockSize)
	for i := 0; i < BlockSize; i++ {
		fill1[i] = 0xff
	}
	err = dev.WriteBlock(0, fill1)
	if err != nil {
		t.Error(err)
	}
	err = dev.Write(3, []byte("hello"))
	if err != nil {
		t.Error(err)
	}
	s := &struct {
		Name string
		Grad int64
	}{"Zhang Zijing", 0x32323232}
	data, err := BytesOf(s)
	if err != nil {
		t.Error(err)
	}
	err = dev.Write(32, data)
	if err != nil {
		t.Error(err)
	}
}
