package main

import "reflect"

// ======== 每个 AG 的前几部分 ========

const SuperBlockMagicNum = uint32(0x73666470)

type Superblock struct {
	MagicNum  uint32
	BlockSize uint32
	SeqNo     uint32 // AgNo
	AgBlocks  uint32 // 表示一个 AG 有多少 blocks
	AgCount   uint32 // 表示一共有多少 AG
}

const AgfBtNum = 3
const AgfMagicNum = 0x464741    // "AGF"
const AgiMagicNum = 0x494741    // "AGI"
const AgflMagicNum = 0x4C464741 // "AGFL"

type Agf struct {
	MagicNum uint32
	SeqNo    uint32
	BnoRoot  uint32
	CntRoot  uint32
}

type Agi struct {
	MagicNum uint32
	SeqNo    uint32
	Root     uint32 // 根节点 inode root block
	FreeRoot uint32
}

type Agfl struct {
	MagicNum uint32
	SeqNo    uint32
}

type AgMetaType interface{ Superblock | Agf | Agi | Agfl }

type AgMetaCtx[T AgMetaType] struct {
	Meta     *T
	Dev      BlockDevice
	AgNo     uint32
	AgBlocks uint32
}

func NewAgMetaCtx[T AgMetaType](dev BlockDevice, agno uint32, agblocks uint32) *AgMetaCtx[T] {
	return &AgMetaCtx[T]{
		Dev:      dev,
		Meta:     nil,
		AgNo:     agno,
		AgBlocks: agblocks,
	}
}

func getBlkOff[T AgMetaType](value T) uint64 {
	tyName := reflect.TypeOf(value).Name()
	switch tyName {
	case "Superblock":
		return 0
	case "Agf":
		return 1
	case "Agi":
		return 2
	case "Agfl":
		return 3
	default:
		panic(ErrUnreachable)
	}
}

func (ctx *AgMetaCtx[T]) Load() error {
	var instance T
	blkoff := getBlkOff(instance)
	bytes, err := ctx.Dev.ReadBlock(uint64(ctx.AgBlocks)*uint64(ctx.AgNo) + blkoff)
	if err != nil {
		return err
	}
	err = StructOf(bytes, &instance)
	if err != nil {
		return err
	}
	ctx.Meta = &instance
	return nil

}

type AgCtx struct {
	Superblock AgMetaCtx[Superblock]
	Agf        AgMetaCtx[Agf]
	Agi        AgMetaCtx[Agi]
	Agfl       AgMetaCtx[Agfl]
	agblocks   uint32
}

func NewAgCtx(dev BlockDevice, agno uint32, agblocks uint32) *AgCtx {
	ctx := &AgCtx{
		Superblock: *NewAgMetaCtx[Superblock](dev, agno, agblocks),
		Agf:        *NewAgMetaCtx[Agf](dev, agno, agblocks),
		Agi:        *NewAgMetaCtx[Agi](dev, agno, agblocks),
		Agfl:       *NewAgMetaCtx[Agfl](dev, agno, agblocks),
		agblocks:   agblocks,
	}
	return ctx
}

func (ctx *AgCtx) Load() error {
	err := ctx.Superblock.Load()
	if err != nil {
		return err
	}
	err = ctx.Agf.Load()
	if err != nil {
		return err
	}
	err = ctx.Agi.Load()
	if err != nil {
		return err
	}
	err = ctx.Agfl.Load()
	if err != nil {
		return err
	}
	return nil
}

// ======== 每个 AG 的后几部分 ========

const InodeMagicNum = 0x494e4f44 // "INOD"

type Inode struct {
	MagicNum uint32
	Mode     uint16
	Uid      uint16
	Gid      uint16
	Size     uint32
	Blocks   uint32
	Atime    uint32
	Mtime    uint32
	Ctime    uint32
	Blkno    uint32
	Dtime    uint32
	Gen      uint32
	Fsid     uint32
	FileType uint32
	Nlink    uint16
	Nsize    uint16
	Flags    uint32
	Reserved uint32
	Blocks2  [15]uint32
}

const DirMagicNum = 0x444e4944 // "DIRD"

type Dir struct {
	MagicNum uint32
	Ino      uint32
	Pdir     uint32
	Nentries uint16
	Nsize    uint16
	Reserved uint32
	Blocks   [15]uint32
}

const FileMagicNum = 0x464c4946 // "FILE"

type File struct {
	MagicNum uint32
	Ino      uint32
	Pdir     uint32
	Nentries uint16
	Nsize    uint16
	Reserved uint32
	Blocks   [15]uint32
}

const SymlinkMagicNum = 0x534d4c53 // "SYML"

type Symlink struct {
	MagicNum uint32
	Ino      uint32
	Pdir     uint32
	Nentries uint16
	Nsize    uint16
	Reserved uint32
	Blocks   [15]uint32
}
