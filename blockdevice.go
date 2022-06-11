package main

import (
	"errors"
	"os"
)

const BlockSize = 512

type BlockDevice interface {
	ReadBlock(blockno uint64) ([]byte, error)
	WriteBlock(blockno uint64, data []byte) error
	Read(offset uint64, data []byte) error
	Write(offset uint64, data []byte) error
	GetTotalBlockCount() uint32
}

type FileBlockDevice struct {
	file       *os.File
	blockcount uint64
}

func NewFileBlockDevice(path string, blockcount uint64) (*FileBlockDevice, error) {
	// create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// truncate to blockcount * blocksize
	err = file.Truncate(int64(blockcount * BlockSize))
	if err != nil {
		return nil, err
	}

	return &FileBlockDevice{
		file:       file,
		blockcount: blockcount,
	}, nil
}

func (f *FileBlockDevice) ReadBlock(blockno uint64) ([]byte, error) {
	data := make([]byte, BlockSize)
	nbytes, err := f.file.ReadAt(data, int64(blockno*BlockSize))
	if err != nil {
		return nil, err
	}
	if nbytes != BlockSize {
		return nil, errors.New("short read")
	}
	return data, nil
}

func (f *FileBlockDevice) WriteBlock(blockno uint64, data []byte) error {
	nbytes, err := f.file.WriteAt(data, int64(blockno*BlockSize))
	if err != nil {
		return err
	}
	if nbytes != BlockSize {
		return errors.New("short write")
	}
	return nil
}

func (f *FileBlockDevice) Read(offset uint64, data []byte) error {
	nbytes, err := f.file.ReadAt(data, int64(offset))
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		// return errors.New("short read")
	}
	return nil
}


func (f *FileBlockDevice) Write(offset uint64, data []byte) error {
	nbytes, err := f.file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		// return errors.New("short write")
	}
	return nil
}

func (f *FileBlockDevice) GetTotalBlockCount() uint32 {
	return uint32(f.blockcount)
}
