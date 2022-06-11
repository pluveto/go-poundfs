package main

import (
	"errors"
	"reflect"

	"github.com/sirupsen/logrus"
)

type any = interface{}

const BtreeBlockMagicNum = uint32(0x42465442) // "BTFB" btree free block
type DBtreeBlock[TRec any] struct {
	MagicNum uint32 `struct:"uint32"`
	NumRecs  uint32 `struct:"uint32,sizeof=Recs"`
	BlkNo    uint64 `struct:"uint64"`
	Recs     []TRec
	// followed by BtreeRecord
}

type DFreeBlockBtRec struct {
	BlockCount uint64 `struct:"uint64"`
	StartBlock uint64 `struct:"uint64"`
}

func (f DFreeBlockBtRec) GetKey() uint64 {
	return f.BlockCount
}

func (f DFreeBlockBtRec) Less(other RecInterface[uint64]) bool {
	return f.BlockCount < other.GetKey()
}

type RecInterface[TKey uint64] interface {
	GetKey() TKey
	// Comparable
	Less(other RecInterface[TKey]) bool
}

// BtreeContext 用于管理基于磁盘的 Btree 结构
type BtreeContext[TKey uint64, TRec RecInterface[TKey]] struct {
	dev  BlockDevice
	root uint64 // root blockno
}

func NewBtreeContext[TKey uint64, TRec RecInterface[TKey]](dev BlockDevice, root uint64) *BtreeContext[TKey, TRec] {
	if root == 0 {
		panic("root blockno cannot be 0")
	}
	return &BtreeContext[TKey, TRec]{
		dev:  dev,
		root: root,
	}
}

// initBlock 初始化一个 BtreeBlock，并将其写入磁盘
func (ctx *BtreeContext[TKey, TRec]) InitBlock() error {
	blk := DBtreeBlock[TRec]{
		MagicNum: BtreeBlockMagicNum,
		NumRecs:  0,
		BlkNo:    ctx.root,
		Recs:     make([]TRec, 0),
	}
	blkBytes, err := BytesOf(&blk)
	if err != nil {
		return err
	}
	return ctx.dev.WriteBlock(ctx.root, Pad(blkBytes, BlockSize))
}

// loadBlock 从指定块号加载根结点
func (ctx *BtreeContext[TKey, TRec]) loadBlock(blkno uint64) (*DBtreeBlock[TRec], error) {
	if blkno == 0 {
		return nil, ErrUnreachable
	}
	data, err := ctx.dev.ReadBlock(blkno)
	if err != nil {
		return nil, err
	}
	// 检查 MagicNum
	if !CheckMagic(data[0:4], BtreeBlockMagicNum) {
		return nil, errors.New("invalid block magic num")
	}
	var btreeBlock DBtreeBlock[TRec]
	err = StructOf(data, &btreeBlock)
	if err != nil {
		return nil, err
	}
	return &btreeBlock, nil
}
func (ctx *BtreeContext[TKey, TRec]) Get(key TKey) (retRec *TRec, err error, exactEqual bool) {
	rootBlk, err := ctx.loadBlock(ctx.root)
	if err != nil {
		return nil, err, false
	}
	if rootBlk.NumRecs == 0 {
		return nil, nil, false
	}
	// search in recs
	for _, rec := range rootBlk.Recs {
		if rec.GetKey() == key {
			return &rec, nil, true
		}
		if rec.GetKey() < key {
			retRec = &rec
		}
	}
	// todo: search in children
	return
}

type Cond = int

const (
	CondEq = iota
	CondLt
	CondGt
	CondGe
	CondLe
)

// 获取第一个大于等于给定 key 的记录
func (ctx *BtreeContext[TKey, TRec]) GetFirstMeet(key TKey) (retRec *TRec, index int, err error) {
	retRec, _, index, err = ctx.get(key, CondGe)
	return
}

func (ctx *BtreeContext[TKey, TRec]) get(key TKey, cond Cond) (retRec *TRec, exactEqual bool, index int, err error) {
	rootBlk, err := ctx.loadBlock(ctx.root)
	if err != nil {
		return nil, false, -1, err
	}
	if rootBlk.NumRecs == 0 {
		return nil, false, -1, nil
	}
	// search in recs
	for i, rec := range rootBlk.Recs {
		// 如果完全匹配，立即返回
		if cond == CondEq || cond == CondLe || cond == CondGe {
			if rec.GetKey() == key {
				retRec = &rec
				exactEqual = true
				index = i
				return
			}
		}
		if cond == CondLt || cond == CondLe {
			// 返回最后一个满足小于条件的
			if rec.GetKey() < key {
				retRec = &rec
				index = i
			}
		}
		if cond == CondGt || cond == CondGe {
			// 返回第一个满足大于条件的
			if rec.GetKey() > key {
				retRec = &rec
				index = i
			}
		}

	}
	// todo: search in children
	return
}
func (ctx *BtreeContext[TKey, TRec]) SetByIndex(value TRec, index int) error {
	rootBlk, err := ctx.loadBlock(ctx.root)
	if err != nil {
		return err
	}
	if index < 0 || index >= int(rootBlk.NumRecs) {
		return errors.New("index out of range")
	}
	rootBlk.Recs[index] = value
	blkBytes, err := BytesOf(rootBlk)
	if err != nil {
		return err
	}
	return ctx.dev.WriteBlock(ctx.root, Pad(blkBytes, BlockSize))
}
func (ctx *BtreeContext[TKey, TRec]) Set(value TRec) error {
	tyName := reflect.TypeOf(value).String()
	logrus.Infof("Set type=%s, value=%v", tyName, value)
	rootBlk, err := ctx.loadBlock(ctx.root)
	if err != nil {
		return err
	}
	key := value.GetKey()
	// 寻找插入点
	insertPos, _ := ctx.getInsertPos(rootBlk, key)
	if uint32(insertPos) == rootBlk.NumRecs {
		// 插入到最后一个位置
		rootBlk.Recs = append(rootBlk.Recs, value)
		rootBlk.NumRecs++
	} else {
		// 插入到指定位置
		var space TRec
		rootBlk.Recs = append(rootBlk.Recs, space)
		copy(rootBlk.Recs[insertPos+1:], rootBlk.Recs[insertPos:])
		rootBlk.Recs[insertPos] = value
		rootBlk.NumRecs++
	}
	// 写入磁盘
	blkBytes, err := BytesOf(rootBlk)
	if err != nil {
		return err
	}
	return ctx.dev.WriteBlock(ctx.root, Pad(blkBytes, BlockSize))
}

func (ctx *BtreeContext[TKey, TRec]) getInsertPos(rootBlk *DBtreeBlock[TRec], key TKey) (insertPos int, exactEqual bool) {
	exactEqual = false
	for i, rec := range rootBlk.Recs {
		if rec.GetKey() < key {
			insertPos = i
		}
		if rec.GetKey() == key {
			exactEqual = true
			return
		}
		if rec.GetKey() > key {
			break
		}
	}
	return
}

func (ctx *BtreeContext[TKey, TRec]) del(key TKey, delAll bool) error {
	rootBlk, err := ctx.loadBlock(ctx.root)
	if err != nil {
		return err
	}
	// search in recs
	for i, rec := range rootBlk.Recs {
		if rec.GetKey() == key {
			rootBlk.Recs = append(rootBlk.Recs[:i], rootBlk.Recs[i+1:]...)
			rootBlk.NumRecs--
			if !delAll {
				break
			}
		}
	}
	// 写入磁盘
	blkBytes, err := BytesOf(rootBlk)
	if err != nil {
		return err
	}
	return ctx.dev.WriteBlock(ctx.root, Pad(blkBytes, BlockSize))

}

// DelAll
func (ctx *BtreeContext[TKey, TRec]) DelAll(key TKey) error {
	return ctx.del(key, true)
}

// Del
func (ctx *BtreeContext[TKey, TRec]) Del(key TKey) error {
	return ctx.del(key, false)
}
