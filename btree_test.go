package main

import (
	"testing"
)

func TestNewBtreeContext(t *testing.T) {
	// 1KB
	blockcount := uint64(1 * 1024 / BlockSize)
	dev, err := NewFileBlockDevice("./btree.bin", blockcount)
	if err != nil {
		t.Error(err)
	}
	ctx := NewBtreeContext[uint64, DFreeBlockBtRec](dev, 1)
	err = ctx.InitBlock()
	if err != nil {
		t.Error(err)
	}
	err = ctx.Set(DFreeBlockBtRec{StartBlock: 123, BlockCount: 999})
	if err != nil {
		t.Error(err)
	}
	err = ctx.Set(DFreeBlockBtRec{StartBlock: 456, BlockCount: 888})
	if err != nil {
		t.Error(err)
	}
	err = ctx.Set(DFreeBlockBtRec{StartBlock: 789, BlockCount: 777})
	if err != nil {
		t.Error(err)
	}
	rec, err, exact := ctx.Get(123)
	if err != nil {
		t.Error(err)
	}
	if rec.StartBlock != 123 || rec.BlockCount != 999 || !exact {
		t.Error("rec is not correct")
	}
	ctx.Del(123)
	_, _, exact = ctx.Get(123)
	if exact == true {
		t.Error("rec is not deleted")
	}
}
