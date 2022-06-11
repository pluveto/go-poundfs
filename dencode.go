package main

import (
	"encoding/binary"
	"errors"
	"reflect"

	"github.com/go-restruct/restruct"
)

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"errors"
// 	"os"
// 	"reflect"
// )

func BytesOf(data interface{}) ([]byte, error) {
	// 确保 data 是指针结构
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("data must be a pointer")
	}
	return restruct.Pack(binary.LittleEndian, data)
}

func StructOf(data []byte, v interface{}) error {
	return restruct.Unpack(data, binary.LittleEndian, v)
}

func SizeOf(data interface{}) (int, error) {
	return restruct.SizeOf(data)
}

func Pad(data []byte, size int) []byte {
	if len(data) == size {
		return data
	}
	if len(data) > size {
		panic("data is too long")
	}
	return append(data, make([]byte, size-len(data))...)
}

// func BytesOf(data interface{}) ([]byte, error) {
// 	ty := reflect.TypeOf(data)
// 	tykind := ty.Kind()
// 	// check if data is int
// 	switch tykind {
// 	case reflect.Bool:
// 		bs := make([]byte, 1)
// 		binary.LittleEndian.PutUint8(bs, uint8(data.(bool)))
// 	case reflect.Int:
// 		bs := make([]byte, 8)
// 		binary.LittleEndian.PutUint64(bs, uint64(data.(ty)))
// 		return bs, nil
// 	case reflect.Int8:
// 		bs := make([]byte, 1)
// 		binary.LittleEndian.PutInt8(bs, int8(data.(ty)))
// 		return bs, nil
// 	case reflect.Int16:
// 		bs := make([]byte, 2)
// 		binary.LittleEndian.PutInt16(bs, int16(data.(ty)))
// 		return bs, nil
// 	case reflect.Int32:
// 		bs := make([]byte, 4)
// 		binary.LittleEndian.PutInt32(bs, int32(data.(ty)))
// 		return bs, nil
// 	case reflect.Int64:
// 		bs := make([]byte, 8)
// 		binary.LittleEndian.PutInt64(bs, int64(data.(ty)))
// 		return bs, nil
// 	case reflect.Uint:
// 		bs := make([]byte, 8)
// 		binary.LittleEndian.PutUint64(bs, uint64(data.(ty)))
// 		return bs, nil
// 	case reflect.Uint8:
// 		bs := make([]byte, 1)
// 		binary.LittleEndian.PutUint8(bs, uint8(data.(ty)))
// 		return bs, nil
// 	case reflect.Uint16:
// 		bs := make([]byte, 2)
// 		binary.LittleEndian.PutUint16(bs, uint16(data.(ty)))
// 		return bs, nil
// 	case reflect.Uint32:
// 		bs := make([]byte, 4)
// 		binary.LittleEndian.PutUint32(bs, uint32(data.(ty)))
// 		return bs, nil
// 	case reflect.Uint64:
// 		bs := make([]byte, 8)
// 		binary.LittleEndian.PutUint64(bs, uint64(data.(ty)))
// 		return bs, nil
// 	case reflect.Array:
// 		baseType := ty.Elem()
// 		elemCount := ty.Len()
// 		slice := make([]byte, 0)
// 		for i := 0; i < elemCount; i++ {
// 			elemData = data.([]interface{})[i]
// 			elemBytes, err := BytesOf(elemData)
// 			if err != nil {
// 				return nil, err
// 			}
// 			slice = append(slice, elemBytes...)
// 		}
// 		return slice, nil

// 	case reflect.Struct:
// 		bs := make([]byte, 0)
// 		for i := 0; i < ty.NumField(); i++ {
// 			field := ty.Field(i)
// 			fieldData := data.(interface{})[i]
// 			fieldBytes, err := BytesOf(fieldData)
// 			if err != nil {
// 				return nil, err
// 			}
// 			bs = append(bs, fieldBytes...)
// 		}
// 		return bs, nil
// 	default:
// 		return nil, errors.New("unsupported type")
// 	}
// 	return nil, nil
// }

// func BytesOfStruct(data interface{}) ([]byte, error) {
// 	buf := new(bytes.Buffer)
// }

// func SaveBytesToFile(data []byte, filename string) error {
// 	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = fd.Write(data)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
