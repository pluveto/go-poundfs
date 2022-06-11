package main

import (
	"os"
	"testing"
)

func TestMakefs(t *testing.T) {
	// 10MB
	blockcount := uint64(10 * 1024 * 1024 / BlockSize)
	// remove
	os.Remove("./device.bin")
	dev, err := NewFileBlockDevice("./device.bin", blockcount)
	if err != nil {
		t.Error(err)
	}
	if dev == nil {
		t.Error("dev is nil")
	}
	// 格式化文件系统
	err = Makefs(dev)
	if err != nil {
		t.Error(err)
	}

	// 获取空闲块
	mp, err := NewMountPoint(dev)
	if err != nil {
		t.Error(err)
	}
	cntroot := mp.AgCtx[0].Agf.Meta.CntRoot
	btCtx := NewBtreeContext[uint64, DFreeBlockBtRec](dev, uint64(cntroot))
	needBlk := uint64(2*1024) / BlockSize // 2KB
	rec, index, err := btCtx.GetFirstMeet(needBlk)
	if err != nil {
		t.Error(err)
	}
	if rec == nil {
		t.Error("rec is nil")
		return
	}
	ndatablock := uint64(4096 / BlockSize)
	// 创建 inode
	inoblk := rec.StartBlock
	defer func() {
		rec.BlockCount -= ndatablock + 1
		rec.StartBlock += ndatablock + 1
		btCtx.SetByIndex(*rec, index)
	}()
	// dirInoCtx := NewInoContext(dev, inoblk)
	// 创建目录
	// dirInoCtx.InitInode(S_IFDIR | 0755)

	// 创建文件
	inofile := inoblk
	fileInoCtx := NewInoContext(dev, inofile)
	err = fileInoCtx.InitInode(S_IFREG | 0644)
	if err != nil {
		t.Error(err)
	}
	fileInoCtx.coreCache.NLocBlk = ndatablock
	err = fileInoCtx.SyncInode()
	if err != nil {
		t.Error(err)
	}
	err = fileInoCtx.InitDataBlock()
	if err != nil {
		t.Error(err)
	}

	// bytes is loop 0x01 to 0xfe to fill the block
	bytes := make([]byte, BlockSize*4)
	for i := 0; i < len(bytes); i++ {
		bytes[i] = byte(i%0xfe + 1)
	}
	n, err := fileInoCtx.Write(33, bytes)
	if err != nil {
		t.Error(err)
	}
	if n != uint64(len(bytes)) {
		t.Errorf("n is %d not %d", n, len(bytes))
	}
	// 将文件放到根目录
	inoRoot := mp.AgCtx[0].Agi.Meta.Root
	rootInodeCtx := NewInoContext(dev, uint64(inoRoot))
	err = rootInodeCtx.LoadInode()
	if err != nil {
		t.Error(err)
	}
	err = rootInodeCtx.AddEntry("myfile.txt", fileInoCtx.ino)
	if err != nil {
		t.Error(err)
	}
	rootInodeCtx.SyncInode()

	// 将文件放到目录
	// dirInoCtx.AddEntry("myfile.txt", fileInoCtx.ino)
	// dirInoCtx.Sync()
}
