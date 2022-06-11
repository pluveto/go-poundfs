package main

import (
	"testing"
)

type Record struct {
	Message string `struct:"[128]byte"`
}

type Container struct {
	Version   int `struct:"int32"`
	NumRecord int `struct:"int32,sizeof=Records"`
	Records   []Record
}

func TestBytesOf(t *testing.T) {
	c := &Container{
		Version:   1,
		NumRecord: 1,
		Records: []Record{
			{
				Message: "Hello, World!",
			},
			{
				Message: "Good bye, World!",
			},
		},
	}
	b, err := BytesOf(c)
	if err != nil {
		t.Error(err)
	}
	WriteBytesToFile(b, "test.bin")
}
