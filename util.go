package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// import (
// 	"github.com/vmihailenco/msgpack"
// )

// // StructToBytes converts a struct to a byte array.
// // The struct must be a pointer.
// func StructToBytes(s interface{}) ([]byte, error) {
// 	return msgpack.Marshal(s)
// }

// // BytesToStruct converts a byte array to a struct.
// func BytesToStruct(b []byte, s interface{}) error {
// 	return msgpack.Unmarshal(b, s)
// }

func WriteBytesToFile(b []byte, filename string) error {
	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(b)
	return err
}

func GetTimestampNsec() uint64 {
	return uint64(time.Now().UnixNano())
}

func JsonStringify(data interface{}) string {
	b, _ := json.MarshalIndent(data, "", "    ")
	return string(b)
}

type magicType interface{ uint32 | uint16 }

func CheckMagic[T magicType](data []byte, magic T) bool {
	magicKind := reflect.TypeOf(magic).Kind()
	if magicKind == reflect.Uint32 {
		if len(data) < 4 {
			panic(ErrUnreachable)
		}
		val := binary.LittleEndian.Uint32(data)
		return val == uint32(magic)
	}
	if magicKind == reflect.Uint16 {
		if len(data) < 2 {
			panic(ErrUnreachable)
		}
		val := binary.LittleEndian.Uint16(data)
		return val == uint16(magic)
	}
	panic(ErrUnreachable)
}

func GetFileSize(filename string) (int64, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type Integer interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64
}

type Float interface {
	float32 | float64
}

type Ordered interface {
	Integer | Float | ~string
}

func Min[T Ordered](nums ...T) T {
	if len(nums) == 0 {
		panic(ErrUnreachable)
	}
	min := nums[0]
	for _, v := range nums[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func TimestampSecPart(ts uint64) uint64 {
	return ts / 1000000000
}

func TimestampNsecPart(ts uint64) uint32 {
	return uint32(ts % 1000000000)
}

func TimestampCombine(sec uint64, nsec uint32) uint64 {
	return sec*1000000000 + uint64(nsec)
}

const O_RDONLY = 00
const O_WRONLY = 01
const O_RDWR = 02
const O_CREAT = 0100  /* not fcntl */
const O_EXCL = 0200   /* not fcntl */
const O_NOCTTY = 0400 /* not fcntl */
const O_TRUNC = 01000 /* not fcntl */
const O_APPEND = 02000
const O_NONBLOCK = 04000
const O_NDELAY = O_NONBLOCK
const O_SYNC = 010000
const O_FSYNC = O_SYNC
const O_ASYNC = 020000

func DecodeFlags(flags uint32) []string {
	var ret []string
	map_ := map[uint32]string{
		O_RDONLY:   "O_RDONLY",
		O_WRONLY:   "O_WRONLY",
		O_RDWR:     "O_RDWR",
		O_CREAT:    "O_CREAT",
		O_EXCL:     "O_EXCL",
		O_TRUNC:    "O_TRUNC",
		O_APPEND:   "O_APPEND",
		O_NONBLOCK: "O_NONBLOCK",
		O_SYNC:     "O_SYNC",
	}
	for k, v := range map_ {
		if flags&k != 0 {
			ret = append(ret, v)
		}
	}
	return ret
}

func Join[T any](a []T, sep string) string {
	var ret string
	for i, v := range a {
		if i != 0 {
			ret += sep
		}
		ret += fmt.Sprintf("%v", v)
	}
	return ret
}

func accessMaskToStr(mask uint32) string {
	var str string
	if mask&fuse.R_OK != 0 {
		str += "R"
	}
	if mask&fuse.W_OK != 0 {
		str += "W"
	}
	if mask&fuse.X_OK != 0 {
		str += "X"
	}
	return str
}

func PreviewBuffer(buf []byte, length int) string {
	if len(buf) < length {
		length = len(buf)
	}
	str := string(buf[:length])
	strHex := hex.EncodeToString(buf[:length])
	return fmt.Sprintf("%s(%s)", str, strHex)
}
