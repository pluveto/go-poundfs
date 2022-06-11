package main

import (
	"testing"
)

func TestGetTimestampNsec(t *testing.T) {
	ts := GetTimestampNsec()
	println(ts / 1000000000)
	println(ts & 0xFFFFFFFF)
}

func TestDecodeFlags(t *testing.T) {
	println(Join(DecodeFlags(33152), "|"))
}
