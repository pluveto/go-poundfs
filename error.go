package main

type PdErr struct {
	Code int
	Msg  string
}

func (e PdErr) Error() string {
	return e.Msg
}
func (e PdErr) GetCode() int {
	return e.Code
}

var ErrAgToSmall = NewPdErr(1, "AG is too small")
var ErrUnreachable = NewPdErr(2, "unreachable")
var ErrNotDirectory = NewPdErr(3, "not a directory")
var ErrEntryExists = NewPdErr(4, "entry already exists")
var ErrNoEntry = NewPdErr(4, "no entry")
var ErrInvalidStructBytes = NewPdErr(5, "invalid struct bytes")
var ErrNotImplemented = NewPdErr(6, "not implemented")
var ErrNoSpace = NewPdErr(7, "no space")
var ErrOutOfRange = NewPdErr(8, "out of range")

func NewPdErr(code int, msg string) PdErr {
	return PdErr{
		Code: code,
		Msg:  msg,
	}
}
