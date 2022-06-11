package main

type MountPoint struct {
	dev   BlockDevice
	sb    *Superblock
	AgCtx []*AgCtx
}

// NewMountPoint creates a new mount point.
// 此方法会自动从磁盘加载数据，所以后续不必手动 Load
func NewMountPoint(dev BlockDevice) (*MountPoint, error) {
	sb, err := loadSuperblock(dev)
	if err != nil {
		return nil, err
	}
	agctx := make([]*AgCtx, sb.AgCount)
	for i := uint32(0); i < sb.AgCount; i++ {
		agctx[i] = NewAgCtx(dev, uint32(i), sb.AgBlocks)
		agctx[i].Load()
	}
	return &MountPoint{
		dev:   dev,
		sb:    sb,
		AgCtx: agctx,
	}, nil
}

// SyncSuperblock writes the superblock to the disk.
func (mp *MountPoint) SyncSuperblock() error {
	sbbytes, err := BytesOf(mp.sb)
	if err != nil {
		return err
	}
	return (mp.dev).WriteBlock(0, Pad(sbbytes, BlockSize))
}

func (mp *MountPoint) GetSuperblock() Superblock {
	return *mp.sb
}

func (mp *MountPoint) GetAgOffsetBlk(agno int) uint32 {
	return mp.sb.AgBlocks * uint32(agno)
}

// loadSuperblock loads the superblock of the primary ag from the disk.
func loadSuperblock(dev BlockDevice) (*Superblock, error) {
	if dev == nil {
		return nil, ErrUnreachable
	}
	var sb Superblock
	sbbytes, err := (dev).ReadBlock(0)
	if err != nil {
		return nil, err
	}
	// check magic number
	if !CheckMagic(sbbytes[:4], SuperBlockMagicNum) {
		return nil, ErrInvalidStructBytes
	}
	err = StructOf(sbbytes, &sb)
	if err != nil {
		return nil, err
	}
	return &sb, nil
}

func (mp *MountPoint) AllocBlock(agno uint32, nblock uint64) (blockno uint64, err error) {
	cntroot := mp.AgCtx[agno].Agf.Meta.CntRoot
	btCtx := NewBtreeContext[uint64, DFreeBlockBtRec](mp.dev, uint64(cntroot))
	rec, index, err := btCtx.GetFirstMeet(nblock)
	if err != nil {
		return 0, err
	}
	if rec == nil {
		return 0, ErrNoSpace

	}
	// 创建 inode
	blockno = rec.StartBlock
	defer func() {
		rec.BlockCount -= nblock
		rec.StartBlock += nblock
		btCtx.SetByIndex(*rec, index)
	}()
	return

}
