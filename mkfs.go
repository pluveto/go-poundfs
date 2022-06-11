package main

import (
	"github.com/sirupsen/logrus"
)

func Makefs(dev BlockDevice) (err error) {
	// 创建分配组
	agCount := uint32(4)
	totalBlocks := (dev).GetTotalBlockCount()
	logrus.Info("totalBlocks: ", totalBlocks)
	agBlocks := totalBlocks / uint32(agCount)
	agBlocksLast := totalBlocks%uint32(agCount) + agBlocks
	for agno := uint32(0); agno < agCount; agno++ {
		var err error
		blockOff := agno * agBlocks
		if agno == agCount-1 {
			err = MakeAg(dev, agno, blockOff, agBlocksLast, agCount)
		} else {
			err = MakeAg(dev, agno, blockOff, agBlocks, agCount)
		}

		if err != nil {
			return err
		}
	}
	return err
}

// MakeAg 创建分配组
// - agblocks 是此 AG 的真实大小
func MakeAg(dev BlockDevice, agno uint32, agBlockOff uint32, agblocks uint32, agcount uint32) error {
	logrus.Infof("initialize AG at 0x%x", agno*agblocks*BlockSize)
	// 定义 AG 的主要结构、b+tree 树根布局
	// 以下均是从 0 开始的
	sbBlk := agBlockOff
	agfBlk := sbBlk + 1
	agiBlk := agfBlk + 1
	afglBlk := agiBlk + 1
	flBlk := afglBlk + 1
	bnoRootBlk := flBlk + 1
	cntRootBlk := bnoRootBlk + 1
	inoRootBlk := cntRootBlk + 1
	freeRootBlk := inoRootBlk + 1

	dataBlk := sbBlk + 16
	dataBlkRel := dataBlk - agBlockOff

	if agBlockOff+agblocks < dataBlk+32 {
		return ErrAgToSmall
	}

	// 1. 创建超级块
	superblock := &Superblock{
		MagicNum:  SuperBlockMagicNum,
		BlockSize: BlockSize,
		AgBlocks:  agblocks,
		AgCount:   agcount,
		SeqNo:     agno,
	}
	superblockData, err := BytesOf(superblock)
	if err != nil {
		return err
	}
	err = dev.WriteBlock(uint64(sbBlk), Pad(superblockData, BlockSize))
	if err != nil {
		return err
	}
	// 2. 创建 AGF
	agf := &Agf{
		MagicNum: AgfMagicNum,
		SeqNo:    agno,
		BnoRoot:  bnoRootBlk,
		CntRoot:  cntRootBlk,
	}
	agfData, err := BytesOf(agf)
	if err != nil {
		return err
	}

	err = dev.WriteBlock(uint64(agfBlk), Pad(agfData, BlockSize))
	if err != nil {
		return err
	}
	// 3. 创建 AGI
	agi := &Agi{
		MagicNum: AgiMagicNum,
		SeqNo:    agno,
		Root:     inoRootBlk,
		FreeRoot: freeRootBlk,
	}
	agiData, err := BytesOf(agi)
	if err != nil {
		return err
	}

	err = dev.WriteBlock(uint64(agiBlk), Pad(agiData, BlockSize))
	if err != nil {
		return err
	}
	// 4. 创建 AGFL
	agfl := &Agfl{
		MagicNum: AgflMagicNum,
		SeqNo:    agno,
	}
	agflData, err := BytesOf(agfl)
	if err != nil {
		return err
	}

	err = dev.WriteBlock(uint64(afglBlk), Pad(agflData, BlockSize))
	if err != nil {
		return err
	}

	// === 接下来初始化空闲空间树 ===
	// 1. 创建空闲空间树的根节点
	btCtx := NewBtreeContext[uint64, DFreeBlockBtRec](dev, uint64(cntRootBlk))
	err = btCtx.InitBlock()
	if err != nil {
		return err
	}
	// 2. 计算本 AG 的剩余空间
	agFreeDataBlocks := agblocks - dataBlkRel
	initDiv := uint32(16)
	// 初始状态，把 agFreeBlocks 平均分配为 initDiv 份
	avgFreeCnt := agFreeDataBlocks / initDiv
	// 最后一项可能有多余
	lastFreeCnt := agFreeDataBlocks%initDiv + avgFreeCnt
	// 3. 创建空闲空间树的节点
	// 计算各个空闲块的起始块号
	for i := uint32(0); i < initDiv; i++ {
		// 创建空闲块记录
		freeBlock := DFreeBlockBtRec{
			BlockCount: uint64(avgFreeCnt),
			StartBlock: uint64(dataBlk + i*avgFreeCnt),
		}
		if i == initDiv-1 {
			freeBlock.BlockCount = uint64(lastFreeCnt)
		}
		// 写入空闲块记录
		err = btCtx.Set(freeBlock)
		if err != nil {
			return err
		}
	}
	// === 接下来初始化根节点 inode ===
	// 1. 创建根节点
	rootInodeCtx := NewInoContext(dev, uint64(inoRootBlk))
	err = rootInodeCtx.InitInode(S_IFDIR | 755)
	if err != nil {
		return err
	}
	rootInodeCtx.SetParent(uint64(inoRootBlk))
	return nil
}
