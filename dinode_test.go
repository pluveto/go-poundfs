package main

import "testing"

func TestInodeModeToStr(t *testing.T) {
	println("00777\t" + StrMode(00777))
	println("33188\t" + StrMode(33188))
	println("33152\t" + StrMode(33152))
	println("0x42f3\t" + StrMode(0x42f3))
}
