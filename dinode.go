package main

import (
	"github.com/sirupsen/logrus"
)

const InodeMagic = uint16(0x4e49) // IN

const EOF = uint8(0xff)

const S_IFMT = 00170000
const S_IFSOCK = 0140000
const S_IFLNK = 0120000
const S_IFREG = 0100000
const S_IFBLK = 0060000
const S_IFDIR = 0040000
const S_IFCHR = 0020000
const S_IFIFO = 0010000

const S_IRWXU = 0000700 /* RWX mask for owner */
const S_IRUSR = 0000400 /* R for owner */
const S_IWUSR = 0000200 /* W for owner */
const S_IXUSR = 0000100 /* X for owner */
const S_IRWXG = 0000070 /* RWX mask for group */
const S_IRGRP = 0000040 /* R for group */
const S_IWGRP = 0000020 /* W for group */
const S_IXGRP = 0000010 /* X for group */

const S_IRWXO = 0000007 /* RWX mask for other */
const S_IROTH = 0000004 /* R for other */
const S_IWOTH = 0000002 /* W for other */
const S_IXOTH = 0000001 /* X for other */

const (
	FMT_LOCAL = iota
	FMT_EXTENTS
	FMT_BTREE
	FMT_DEV
)

type DInode struct {
	Magic       uint16 `struct:"uint16"` // IN
	Ino         uint64 `struct:"uint64"` // 绝对 inode number
	Mode        uint16 `struct:"uint16"` // rwx 等权限位。
	Format      int8   `struct:"int8"`   // 格式，常见有 FMT_LOCAL; FMT_EXTENTS; FMT_BTREE. FMT_DEV 用于字符或块设备
	Uid         uint32 `struct:"uint32"` // 文件所有人
	Gid         uint32 `struct:"uint32"` // 文件所属组
	Nlink       uint32 `struct:"uint32"` // 硬链接计数
	Flags       uint32 `struct:"uint32"` // 文件的标志位
	Atime       uint64 `struct:"uint64"` // 最后访问时间
	Mtime       uint64 `struct:"uint64"` // 最后修改时间
	Ctime       uint64 `struct:"uint64"` // 最后 inode 状态修改时间
	Size        uint64 `struct:"uint64"` // 决定了 EOF 的偏移量
	NLocBlk     uint64 `struct:"uint64"` // 直接数据块数量
	ForkOff     uint8  `struct:"uint8"`
	Changecount uint64 `struct:"uint64"` // inode 的 i_version，每次修改 inode 时加 1
	Crtime      uint64 `struct:"uint64"` // 创建时间
}

// 目录头
type DirSfHdr struct {
	Count   uint8  `struct:"uint8,sizeof=Entries"` // 目录条目数
	Parent  uint64 `struct:"uint64"`               // 父目录inode
	Entries []DirSfEntry
}

type DirSfEntry struct {
	Ino     uint64 `struct:"uint64"`            // inode number
	Namelen uint8  `struct:"uint8,sizeof=Name"` // 文件名长度
	Name    []uint8
}

type InoContext struct {
	dev       BlockDevice
	ino       uint64 // inode blockno
	coreCache *DInode
	dirSfHdr  *DirSfHdr
}

// const nreserve = 16 * 16

func NewInoContext(dev BlockDevice, ino uint64) *InoContext {
	return &InoContext{
		dev, ino, nil, nil,
	}
}
func (ctx *InoContext) InitInode(mode uint16) error {
	ino := DInode{
		Magic:       InodeMagic,
		Ino:         ctx.ino,
		Mode:        mode,
		Format:      FMT_LOCAL,
		Uid:         0,
		Gid:         0,
		Nlink:       1,
		Atime:       GetTimestampNsec(),
		Mtime:       GetTimestampNsec(),
		Ctime:       GetTimestampNsec(),
		Size:        0,
		ForkOff:     0,
		Changecount: 0,
		Crtime:      GetTimestampNsec(),
	}
	// if has S_IFDIR flag
	if mode&S_IFMT == S_IFDIR {
		ctx.initDirHdr()
	}
	ctx.coreCache = &ino
	return ctx.SyncInode()
}

func (ctx *InoContext) InitDataBlock() error {
	databytes := make([]byte, BlockSize)
	// fill 0xff
	for i := 0; i < BlockSize; i++ {
		databytes[i] = EOF
	}
	for i := 0; i < int(ctx.coreCache.NLocBlk); i++ {
		err := ctx.dev.WriteBlock(ctx.ino+1+uint64(i), databytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctx *InoContext) IsDir() bool {
	return ctx.coreCache.Mode&S_IFMT == S_IFDIR
}

func (ctx *InoContext) initDirHdr() {
	logrus.Debugf("initDirHdr of ino: %d", ctx.ino)
	dirSfHdr := DirSfHdr{
		Count:   0,
		Parent:  0,
		Entries: make([]DirSfEntry, 0),
	}
	ctx.dirSfHdr = &dirSfHdr
}

// SetParent sets the parent inode of the inode.
func (ctx *InoContext) SetParent(parentIno uint64) error {
	ctx.dirSfHdr.Parent = parentIno
	return ctx.SyncInode()
}
func (ctx *InoContext) ToBytes() ([]byte, error) {
	blkBuf := make([]byte, BlockSize)
	// 序列化 core
	ino := ctx.coreCache
	inoBytes, err := BytesOf(ino)
	if err != nil {
		return nil, err
	}
	copy(blkBuf, inoBytes)
	// 序列化 datafork
	// 从第 16 字节开始写 datafork
	dataforkOff := 256
	// 仅针对目录
	if ctx.dirSfHdr != nil {
		// 写入 header 到 datafork
		dirSfHdr := ctx.dirSfHdr
		dirSfHdrBytes, err := BytesOf(dirSfHdr)
		if err != nil {
			return nil, err
		}
		copy(blkBuf[dataforkOff:], dirSfHdrBytes)
	}
	return blkBuf, nil
}

func (ctx *InoContext) LoadInode() error {
	blkBuf, err := ctx.dev.ReadBlock(ctx.ino)
	logrus.Debugf("LoadInode of ino: %d loc=0x%x", ctx.ino, ctx.ino*BlockSize)
	if err != nil {
		return err
	}
	if !CheckMagic(blkBuf[:2], InodeMagic) {
		return ErrInvalidStructBytes
	}
	err = ctx.fromBytes(blkBuf)
	if err != nil {
		return err
	}
	if ctx.IsDir() && nil == ctx.dirSfHdr {
		ctx.initDirHdr()
	}
	return nil
}

func (ctx *InoContext) fromBytes(blkBuf []byte) error {
	// 反序列化 core
	ino := DInode{}
	inoSize, err := SizeOf(&ino)
	if err != nil {
		return err
	}
	err = StructOf(blkBuf[:inoSize], &ino)
	if err != nil {
		return err
	}
	ctx.coreCache = &ino
	// 针对目录
	if ino.Mode&S_IFMT == S_IFDIR {
		// 反序列化 datafork
		// 从第 nreserve 字节开始读 datafork
		dataforkOff := 256
		// 读入 header 到 datafork
		dirSfHdr := DirSfHdr{}
		err = StructOf(blkBuf[dataforkOff:], &dirSfHdr)
		if err != nil {
			return err
		}
		ctx.dirSfHdr = &dirSfHdr
	}
	return nil
}

func StrMode(mode uint16) string {
	// input 777 output "drwxrwxrwx"
	str := ""
	if mode&S_IFMT == S_IFDIR {
		str += "d"
	}
	if mode&S_IFMT == S_IFREG {
		str += "-"
	}
	if mode&S_IFMT == S_IFLNK {
		str += "l"
	}
	if mode&S_IFMT == S_IFSOCK {
		str += "s"
	}
	if mode&S_IFMT == S_IFBLK {
		str += "b"
	}
	if mode&S_IFMT == S_IFCHR {
		str += "c"
	}
	if mode&S_IFMT == S_IFIFO {
		str += "p"
	}
	if mode&S_IRUSR != 0 {
		str += "r"
	} else {
		str += "-"
	}
	if mode&S_IWUSR != 0 {
		str += "w"
	} else {
		str += "-"
	}
	if mode&S_IXUSR != 0 {
		str += "x"
	} else {
		str += "-"
	}
	if mode&S_IRGRP != 0 {
		str += "r"
	} else {
		str += "-"
	}
	if mode&S_IWGRP != 0 {
		str += "w"
	} else {
		str += "-"
	}
	if mode&S_IXGRP != 0 {
		str += "x"
	} else {
		str += "-"
	}
	if mode&S_IROTH != 0 {
		str += "r"
	} else {
		str += "-"
	}
	if mode&S_IWOTH != 0 {
		str += "w"
	} else {
		str += "-"
	}
	if mode&S_IXOTH != 0 {
		str += "x"
	} else {
		str += "-"
	}
	return str
}

func (ctx *InoContext) SyncInode() error {
	ctx.coreCache.Ctime = GetTimestampNsec()
	blkBuf, err := ctx.ToBytes()
	if err != nil {
		return err
	}
	logrus.Debugf("sync ino %d (loc: 0x%x) mode=%s", ctx.ino, ctx.ino*BlockSize, StrMode(ctx.coreCache.Mode))
	return ctx.dev.WriteBlock(ctx.ino, Pad(blkBuf, BlockSize))
}

func (ctx *InoContext) AddEntry(name string, ino uint64) error {
	logrus.Debugf("add entry %s of ino %d loc 0x%x", name, ctx.ino, ctx.ino*BlockSize)
	if ctx.coreCache.Mode&S_IFMT != S_IFDIR {
		return ErrNotDirectory
	}
	_, err := ctx.GetEntry(name)
	if err == nil {
		// return ErrEntryExists
		// 删除原有的 entry
		err = ctx.RemoveEntry(name)
		if err != nil {
			return err
		}
	}
	dirSfHdr := ctx.dirSfHdr
	dirSfHdr.Count++
	dirSfHdr.Entries = append(dirSfHdr.Entries, DirSfEntry{
		Namelen: uint8(len(name)), Name: []uint8(name),
		Ino: ino,
	})
	return nil
}

func (ctx *InoContext) GetEntry(name string) (uint64, error) {
	if ctx.dirSfHdr == nil {
		return 0, ErrUnreachable
	}
	if ctx.coreCache.Mode&S_IFMT != S_IFDIR {
		return 0, ErrNotDirectory
	}
	dirSfHdr := ctx.dirSfHdr
	for _, entry := range dirSfHdr.Entries {
		if string(entry.Name) == name {
			return entry.Ino, nil
		}
	}
	return 0, ErrNoEntry
}

func locateEof(blkBuf []byte) (uint64, bool) {
	for i := 0; i < len(blkBuf); i++ {
		if blkBuf[i] == EOF {
			return uint64(i), true
		}
	}
	return 0, false
}

func (ctx *InoContext) Bmap(blkno uint64) (uint64, error) {
	if ctx.coreCache.Format == FMT_LOCAL {
		return ctx.ino + 1 + blkno, nil
	}
	return 0, ErrNotImplemented
}

// Read 从当前文件 offset 开始读取 size 个字节到 bytes. 其中 size 不能超过 bytes 的长度.
//  	如果 size 超过文件长度，则只读文件长度部分
func (ctx *InoContext) Read(off uint64, bytes []byte) (uint64, error) {
	fsize := ctx.coreCache.Size
	if ctx.coreCache.Format == FMT_LOCAL {
		vstartBlk := off / BlockSize
		vendBlk := (off + uint64(len(bytes))) / BlockSize
		// virtual file final block
		vffinalBlk := fsize / BlockSize
		if vendBlk > vffinalBlk {
			logrus.Warn("read beyond file size")
		}
		// 已读字节数
		readLen := uint64(0)
		for vblk := vstartBlk; vblk <= vffinalBlk; vblk++ {
			blk, err := ctx.Bmap(vblk)
			if err != nil {
				return 0, err
			}
			blkBuf, err := ctx.dev.ReadBlock(blk)
			if err != nil {
				return 0, err
			}
			// 如果是读第一个块，则要从偏移处截取
			if vblk == vstartBlk {
				start := uint64(off % BlockSize)
				// 避免多读
				end := Min(uint64(len(bytes)), BlockSize, fsize)
				copy(bytes[0:0+end], blkBuf[start:end])
				readLen += uint64(end - start)
			} else if vblk == vendBlk {
				// 如果是读取范围的最后一个块
				// 有两种情况：是或不是此文件的最后一个块
				if vblk == vffinalBlk {
					// 是文件最后一个块，则不能超过 eof
					end := fsize % BlockSize
					copy(bytes[readLen:readLen+end], blkBuf[0:end])
					readLen += uint64(end)
				} else {
					end := len(bytes) % BlockSize
					copy(bytes[readLen:readLen+BlockSize], blkBuf[:end])
					readLen += uint64(end)
				}
			} else {
				// 对于中间块，可以直接全部读入
				copy(bytes[readLen:], blkBuf)
				readLen += BlockSize
			}
		}

		// blkBytes, err := ctx.dev.ReadBlock(datablk)
		// if err != nil {
		// 	return 0, err
		// }
		// readLen = uint64(len(bytes))
		// fsize := ctx.coreCache.Size
		// if readLen > fsize {
		// 	readLen = fsize
		// }
		// eof, hasEof := locateEof(blkBytes[off:])
		// if hasEof {
		// 	readLen = Min(readLen, uint64(eof))
		// }
		// copy(bytes, blkBytes[off:off+readLen])

		// update atime
		ctx.coreCache.Atime = GetTimestampNsec()
		err := ctx.SyncInode()
		if err != nil {
			return 0, err
		}
		return readLen, nil
	}
	return 0, ErrNotImplemented
}

// off 是文件内偏移
func (ctx *InoContext) Write(off uint64, bytes []byte) (length uint64, err error) {
	eof, ok := locateEof(bytes)
	if ok {
		logrus.Warnf("write eof at offset %d", eof)
	}

	if ctx.coreCache.Format == FMT_LOCAL {
		vstartBlk := off / BlockSize
		vendBlk := (off + uint64(len(bytes))) / BlockSize
		// TODO: 判断是否需要扩充文件
		// 使用 offRead 指针指向下一次要从 bytes 中读的位置。它是对 bytes 而言的指针。
		/*
			分三种情况讨论
			bytes:      |<------------->|
			blocks:  |<----->|<----->|<----->|
			            |offRead              读开头时，offRead 位于 0，读大小为 blocksize - off % blocksize。（BlockSize-start）
						                      block 内的写入位置为：off % blocksize:blocksize
			                 |offRead         读中间前，将 offRead 增加上次读取量，则对齐。读大小：往后读一个 block 即可（offRead:BlockSize）
			                         |offRead 读结尾前和读中间的情况一样，只不过读大小为 bytes 中剩余的长度
		*/
		offRead := uint64(0)
		// 此处的 vblk 是虚拟块号，这是因为实际文件可能处于一个绝对位置，或者分散在不同的磁盘位置
		for vblk := vstartBlk; vblk <= vendBlk; vblk++ {
			if offRead == uint64(len(bytes)) {
				break
			}
			if offRead > uint64(len(bytes)) {
				return 0, ErrUnreachable
			}
			// 转换为实际块号
			blk, err := ctx.Bmap(vblk)
			if err != nil {
				return 0, err
			}
			startBlk, err := ctx.Bmap(vstartBlk)
			if err != nil {
				return 0, err
			}
			endBlk, err := ctx.Bmap(vendBlk)
			if err != nil {
				return 0, err
			}

			// 对于头尾的块，可能只需要写入一部分，因此先读再写
			blkCur, err := ctx.dev.ReadBlock(blk)
			if err != nil {
				return 0, err
			}
			if blk == startBlk {
				inBlockStart := off % BlockSize
				inBytesEnd := Min(BlockSize-inBlockStart, uint64(len(bytes)))
				copy(blkCur[inBlockStart:], bytes[0:inBytesEnd])
				offRead += uint64(inBytesEnd)
			} else if blk == endBlk {
				inBlockEnd := off + uint64(len(bytes)) - offRead
				copy(blkCur[:inBlockEnd], bytes[offRead:])
				offRead += uint64(len(bytes[offRead:]))
			} else {
				copy(blkCur[:], bytes[offRead:offRead+BlockSize])
				offRead += BlockSize
			}
			// 写回到磁盘块
			err = ctx.dev.WriteBlock(blk, blkCur)
			if err != nil {
				return 0, err
			}
		}
		// update atime
		ctx.coreCache.Atime = GetTimestampNsec()
		// update file size
		if off+offRead > ctx.coreCache.Size {
			ctx.coreCache.Size = off + offRead
		}
		err = ctx.SyncInode()
		if err != nil {
			return 0, err
		}
		return offRead, nil

		// totalLen := uint64(len(bytes))
		// writtenLen := uint64(0)
		// writeToBlk := ctx.ino + 1
		// for writtenLen < totalLen {
		// 	// 写块缓冲
		// 	blkBytes := make([]byte, BlockSize)
		// 	// 写入本块长度
		// 	blkLen := Min(totalLen-writtenLen, BlockSize)
		// 	copy(blkBytes, bytes[writtenLen:writtenLen+blkLen])
		// 	err = ctx.dev.WriteBlock(writeToBlk, blkBytes)
		// 	if err != nil {
		// 		return 0, err
		// 	}
		// 	writtenLen += blkLen
		// 	writeToBlk++
		// }
		// // 更新文件大小
		// ctx.coreCache.Size = uint64(len(bytes) + int(off))
		// err = ctx.dev.WriteBlock(datablk, blkBytes)
		// if err != nil {
		// 	return 0, err
		// }
		// err = ctx.SyncInode()
		// if err != nil {
		// 	return 0, err
		// }
		// // update mtime
		// ctx.coreCache.Mtime = GetTimestampNsec()
		// err = ctx.SyncInode()
		// return totalLen, err
	}
	return 0, ErrNotImplemented
}

// Truncate truncates the file to a given length.
// We will not update it to the block device , you nede to call SyncInode to update it.
func (ctx *InoContext) Truncate(size uint64) error {
	if ctx.coreCache.Mode&S_IFMT == S_IFREG {
		// 更新文件大小
		ctx.coreCache.Size = uint64(size)
		return nil
	}
	return ErrNotImplemented
}
func (ctx *InoContext) GetChild(name string) (*InoContext, error) {
	childEnt, err := ctx.GetEntry(name)
	if err != nil {
		return nil, err
	}
	childCtx := NewInoContext(ctx.dev, childEnt)
	err = childCtx.LoadInode()
	if err != nil {
		return nil, err
	}
	return childCtx, nil

}

func (ctx *InoContext) RemoveEntry(name string) error {
	if ctx.coreCache.Mode&S_IFMT != S_IFDIR {
		return ErrNotDirectory
	}
	dirSfHdr := ctx.dirSfHdr
	for i, entry := range dirSfHdr.Entries {
		if string(entry.Name) == name {
			dirSfHdr.Entries = append(dirSfHdr.Entries[:i], dirSfHdr.Entries[i+1:]...)
			dirSfHdr.Count--
			return nil
		}
	}
	return ErrNoEntry
}

func (ctx *InoContext) GetEntries() ([]DirSfEntry, error) {
	if ctx.coreCache.Mode&S_IFMT != S_IFDIR {
		return nil, ErrNotDirectory
	}
	dirSfHdr := ctx.dirSfHdr
	return dirSfHdr.Entries, nil
}
