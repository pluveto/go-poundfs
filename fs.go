/**
实现文件系统的 Unix 接口（借助 Fuse 得以在用户态运行）

fh: 文件把手，俗称文件描述符
	https://libfuse.github.io/doxygen/fuse-3_810_84_2include_2fuse__common_8h_source.html
*/
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sirupsen/logrus"
)

type PoundFS struct {
	fuse.RawFileSystem
	dev        BlockDevice
	mp         *MountPoint
	openfiles  *OpenfileMap
	xattrCache map[string][]byte
}

const RootIno = 1

func NewPoundFS(dev BlockDevice) *PoundFS {

	mp, err := NewMountPoint(dev)
	if err != nil {
		logrus.Errorf("op=%s, err=%v", "Init", err)
		return nil
	}
	return &PoundFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		dev:           dev,
		mp:            mp,
		openfiles:     NewOpenfileMap(),
		xattrCache:    make(map[string][]byte),
	}
}

func (fs *PoundFS) Init(server *fuse.Server) {
	logrus.Debugf("[in] op=%s", "I[out] nit")
	logrus.Debugf("op=%s", "Init")
}

func (fs *PoundFS) String() string {
	return "poundfs"
}

func (fs *PoundFS) SetDebug(dbg bool) {
	logrus.Debugf("[in] op=%s", "SetDebug")
	logrus.Debugf("op=%s", "SetDebug")
}

func (fs *PoundFS) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	logrus.Debugf("[in ] op=%s", "StatFs")
	logrus.Debugf("[out] op=%s", "StatFs")
	// out = &fuse.StatfsOut{
	// }
	return fuse.ENOSYS
}

func (c *PoundFS) internalLookup(cancel <-chan struct{}, out *fuse.Attr, parent *InoContext, name string, header *fuse.InHeader) (node *InoContext, code fuse.Status) {
	logrus.Debugf("[in ] op=%s, in=%v, name=%s", "internalLookup", JsonStringify(header), name)
	child, err := parent.GetChild(name)
	if err != nil {
		logrus.Errorf("op=%s, err=%v, name=%s", "internalLookup", err, name)
		if ErrNoEntry == err {
			return nil, fuse.ENOENT
		}
		return nil, fuse.ENOSYS
	}
	code = fuse.OK
	return child, code
}

// Lookup 根据文件名查找文件
func (fs *PoundFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, name=%s", "Lookup", header.NodeId, name)
	parent := fs.GetInode(header.NodeId)
	if !parent.IsDir() {
		logrus.Errorf("Lookup %q called on non-Directory node %d", name, header.NodeId)
		return fuse.ENOTDIR
	}

	child, code := fs.internalLookup(cancel, &out.Attr, parent, name, header)
	if !code.Ok() {
		logrus.Errorf("Lookup %q failed: %v", name, code)
		return code
	}
	if child == nil {
		logrus.Infoln("Lookup returned fuse.OK with nil child", name)
		return fuse.ENOENT
	}
	inode := child.coreCache

	out.NodeId = inode.Ino
	out.Generation = 1
	out.Ino = inode.Ino
	out.Size = inode.Size
	// out.Blocks = inode.Blocks
	out.Atime = TimestampSecPart(inode.Atime)
	out.Mtime = TimestampSecPart(inode.Mtime)
	out.Ctime = TimestampSecPart(inode.Ctime)
	out.Atimensec = TimestampNsecPart(inode.Atime)
	out.Mtimensec = TimestampNsecPart(inode.Mtime)
	out.Ctimensec = TimestampNsecPart(inode.Ctime)
	out.Mode = uint32(inode.Mode)
	out.Nlink = inode.Nlink
	out.Owner = fuse.Owner{Uid: inode.Uid, Gid: inode.Gid}
	// out.Rdev = inode.Rdev
	out.Blksize = BlockSize
	// out.Padding = inode.Padding
	logrus.Infof("[out] op=%s, ino=%v, name=%s", "Lookup", header.NodeId, name)
	return fuse.OK
}

// 
func (fs *PoundFS) Forget(nodeID, nlookup uint64) {
	logrus.Infof("[in ] op=%s, node=%v, nlookup=%v", "Forget", nodeID, nlookup)
	logrus.Debugf("[out] op=%s", "Forget")
}

func (fs *PoundFS) GetInode(ino uint64) *InoContext {
	if ino == RootIno {
		// 将 inode 1 转换为真实 根节点 inode
		ino = uint64(fs.mp.AgCtx[0].Agi.Meta.Root)
	}
	inodeCtx := NewInoContext(fs.dev, ino)
	inodeCtx.LoadInode()
	return inodeCtx
}

func (fs *PoundFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v", "GetAttr", input.NodeId)
	inodeCtx := fs.GetInode(input.NodeId)
	inode := inodeCtx.coreCache

	if inode.Nlink == 0 {
		logrus.Error("GetAttr called on deleted inode", input.NodeId)
		return fuse.ENOENT
	}

	out.Ino = inode.Ino
	out.Size = inode.Size
	// out.Blocks = inode.Blocks
	out.Atime = TimestampSecPart(inode.Atime)
	out.Mtime = TimestampSecPart(inode.Mtime)
	out.Ctime = TimestampSecPart(inode.Ctime)
	out.Atimensec = TimestampNsecPart(inode.Atime)
	out.Mtimensec = TimestampNsecPart(inode.Mtime)
	out.Ctimensec = TimestampNsecPart(inode.Ctime)
	out.Mode = uint32(inode.Mode)
	out.Nlink = inode.Nlink
	out.Owner = fuse.Owner{Uid: inode.Uid, Gid: inode.Gid}
	// out.Rdev = inode.Rdev
	out.Blksize = BlockSize
	// out.Padding = inode.Padding
	logrus.Infof("[out] op=%s", "GetAttr")
	return fuse.OK
}

func (fs *PoundFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, flags=%v, mode=%v", "Open",
		input.NodeId, DecodeFlags(input.Flags), DecodeFlags(input.Mode))
	logrus.Debugf("[out] op=%s, in=%s", "Open", JsonStringify(out))
	out.Fh = fs.openfiles.Register(input.NodeId, input.Flags)
	// out.OpenFlags = input.Flags
	// out.OpenFlags = fuse.FOPEN_DIRECT_IO
	return fuse.OK
}

// https://github.com/libfuse/libfuse/issues/342
func (fs *PoundFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, valid=%v", "SetAttr", input.NodeId, input.Valid)

	inodeCtx := fs.GetInode(input.NodeId)
	inode := inodeCtx.coreCache
	if input.Valid&fuse.FATTR_MODE != 0 {
		inode.Mode = uint16(input.Mode)
		out.Mode = uint32(input.Mode)
	}
	if input.Valid&fuse.FATTR_UID != 0 {
		inode.Uid = input.Uid
		out.Owner.Uid = input.Uid
	}
	if input.Valid&fuse.FATTR_GID != 0 {
		inode.Gid = input.Gid
		out.Owner.Gid = input.Gid
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		// 注意，此处相当于 truncate 操作
		err := inodeCtx.Truncate(input.Size)
		if err != nil {
			logrus.Errorf("Truncate failed: %v", err)
			return fuse.EIO
		}
		logrus.Infof("Truncate %v size to %v", inode.Ino, input.Size)
		out.Size = input.Size
	}
	if input.Valid&fuse.FATTR_ATIME != 0 {
		inode.Atime = TimestampCombine(input.Atime, input.Atimensec)
		out.Atime = TimestampSecPart(input.Atime)
		out.Atimensec = TimestampNsecPart(input.Atime)
	}
	if input.Valid&fuse.FATTR_MTIME != 0 {
		inode.Mtime = TimestampCombine(input.Mtime, input.Mtimensec)
		out.Mtime = TimestampSecPart(input.Mtime)
		out.Mtimensec = TimestampNsecPart(input.Mtime)
	}
	if input.Valid&fuse.FATTR_CTIME != 0 {
		inode.Ctime = TimestampCombine(input.Ctime, input.Ctimensec)
		out.Ctime = TimestampSecPart(input.Ctime)
		out.Ctimensec = TimestampNsecPart(input.Ctime)
	}
	//
	// uint64_t fuse_file_info::lock_owner
	// Lock owner id. Available in locking operations and flush
	if input.Valid&fuse.FATTR_LOCKOWNER != 0 {
		logrus.Warnf("FATTR_LOCKOWNER lock_owner is %v (0x%x)", input.LockOwner, input.LockOwner)
		// inode.LockOwner = input.LockOwner
		// out.Owner = fuse.Owner{Uid: inode.Uid, Gid: inode.Gid}
	}

	// out.Ino = inode.Ino

	err := inodeCtx.SyncInode()
	if err != nil {
		logrus.Errorf("SetAttr failed: %v", err)
		return fuse.EIO
	}
	out.Attr = ConvertAttr(inodeCtx)
	logrus.Infof("[out] op=%s", "SetAttr")
	return fuse.OK
}

func ConvertAttr(inodeCtx *InoContext) fuse.Attr {
	inode := inodeCtx.coreCache
	return fuse.Attr{
		Ino:       inode.Ino,
		Size:      inode.Size,
		Blocks:    inode.NLocBlk,
		Atime:     TimestampSecPart(inode.Atime),
		Mtime:     TimestampSecPart(inode.Mtime),
		Ctime:     TimestampSecPart(inode.Ctime),
		Atimensec: TimestampNsecPart(inode.Atime),
		Mtimensec: TimestampNsecPart(inode.Mtime),
		Ctimensec: TimestampNsecPart(inode.Ctime),
		Mode:      uint32(inode.Mode),
		Nlink:     inode.Nlink,
		Owner:     fuse.Owner{Uid: inode.Uid, Gid: inode.Gid},
		// Rdev:      inode.Rdev,
		Blksize: BlockSize,
		// Padding:   inode.Padding,
		Padding: 0,
	}
}

func (fs *PoundFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	logrus.Infof("[in ] op=%s", "Readlink")
	logrus.Debugf("[out] op=%s", "Readlink")
	return nil, fuse.ENOSYS
}

func (fs *PoundFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, name=%s, parent_ino=%v, mode=%v, flags=nil", "Mknod", name, input.NodeId, input.Mode)
	parentInodeCtx := fs.GetInode(input.NodeId)
	parentInodeCtx.LoadInode()
	newBlk, err := fs.mp.AllocBlock(0, 5)
	if err != nil {
		logrus.Errorf("Mknod failed: %v", err)
		return fuse.EIO
	}
	newInodeCtx := NewInoContext(fs.dev, newBlk)
	err = newInodeCtx.InitInode(uint16(input.Mode))
	inode := newInodeCtx.coreCache
	inode.Uid = input.Uid
	inode.Gid = input.Gid
	if err != nil {
		logrus.Errorf("Mknod failed: %v", err)
		return fuse.EIO
	}
	err = newInodeCtx.InitDataBlock()
	if err != nil {
		logrus.Errorf("Mknod failed: %v", err)
		return fuse.EIO
	}
	err = newInodeCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Mknod failed: %v", err)
		return fuse.EIO
	}
	// 放到父目录
	err = parentInodeCtx.AddEntry(name, inode.Ino)
	if err != nil {
		logrus.Errorf("Mknod failed: %v", err)
		return fuse.EIO
	}
	parentInodeCtx.SyncInode()
	logrus.Debugf("[out] op=%s", "Mknod")

	out.NodeId = inode.Ino
	out.Atime = inode.Atime
	out.Ino = inode.Ino
	out.Size = inode.Size
	out.Blocks = 1
	out.Atime = TimestampSecPart(inode.Atime)
	out.Mtime = TimestampSecPart(inode.Mtime)
	out.Ctime = TimestampSecPart(inode.Ctime)
	out.Atimensec = TimestampNsecPart(inode.Atime)
	out.Mtimensec = TimestampNsecPart(inode.Mtime)
	out.Ctimensec = TimestampNsecPart(inode.Ctime)
	out.Mode = uint32(inode.Mode)
	out.Nlink = inode.Nlink
	out.Owner = fuse.Owner{Uid: inode.Uid, Gid: inode.Gid}
	return fuse.OK
}

func (fs *PoundFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, name=%v", "Mkdir", input.NodeId, name)
	parentInodeCtx := fs.GetInode(input.NodeId)	
	
	newBlk, err := fs.mp.AllocBlock(0, 5)
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	newDirInodeCtx := NewInoContext(fs.dev, newBlk)
	err = newDirInodeCtx.InitInode(uint16(fuse.S_IFDIR|input.Mode))
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	inode := newDirInodeCtx.coreCache
	inode.Uid = input.Uid
	inode.Gid = input.Gid
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	err = newDirInodeCtx.InitDataBlock()
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	err = newDirInodeCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	// 放到父目录
	err = parentInodeCtx.AddEntry(name, inode.Ino)
	if err != nil {
		logrus.Errorf("Mkdir failed: %v", err)
		return fuse.EIO
	}
	parentInodeCtx.SyncInode()

	out.Attr = ConvertAttr(newDirInodeCtx)
	out.Generation = NextGen()
	out.NodeId = inode.Ino

	logrus.Debugf("[out] op=%s", "Mkdir")
	return fuse.OK
}

func (fs *PoundFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, name=%s, ino=%v", "Unlink", name, header.NodeId)
	logrus.Debugf("[out] op=%s", "Unlink")

	// 删除条目、删除文件、回收空间
	dirInoCtx := fs.GetInode(header.NodeId)
	fileIno, err := dirInoCtx.GetEntry(name)
	if err != nil {
		logrus.Errorf("Unlink failed when get entry by name: %v %s", err, name)
		return fuse.EIO
	}
	fileInode := fs.GetInode(fileIno)
	err = dirInoCtx.RemoveEntry(name)
	if err != nil {
		logrus.Errorf("Unlink failed when remove entry by name: %v", err)
		return fuse.EIO
	}
	err = dirInoCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Unlink failed when sync inode of dir: %v", err)
		return fuse.EIO
	}
	// 减少硬链接计数
	fileInode.coreCache.Nlink--
	err = fileInode.SyncInode()
	if err != nil {
		logrus.Errorf("Unlink failed when sync inode of file: %v", err)
		return fuse.EIO
	}
	return fuse.OK
}

func (fs *PoundFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, name=%s, in=%s", "Rmdir", name, JsonStringify(header))
	logrus.Debugf("[out] op=%s", "Rmdir")
	return fuse.ENOSYS
}

func (fs *PoundFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, pointedTo=%s, linkName=%s, in=%s", "Symlink", pointedTo, linkName, JsonStringify(header))
	logrus.Debugf("[out] op=%s", "Symlink")
	return fuse.ENOSYS
}

// Rename 将文件或目录从一个目录移动到另一个目录
func (fs *PoundFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, old_dir_ino=%v, old_name=%s, new_dir_ino=%v, new_name=%s", "Rename", input.NodeId, oldName, input.Newdir, newName)
	var err error
	// 获取旧目录 inode
	oldDirInoCtx := fs.GetInode(input.NodeId)
	err = oldDirInoCtx.LoadInode()
	if err != nil {
		logrus.Errorf("Rename failed when load inode of old dir: %v", err)
		return fuse.EIO
	}
	// 获取文件 inode
	fileIno, err := oldDirInoCtx.GetEntry(oldName)
	if err != nil {
		logrus.Errorf("Rename failed when get entry by name: %v %s", err, oldName)
		return fuse.EIO
	}
	fileInodeCtx := fs.GetInode(fileIno)
	err = fileInodeCtx.LoadInode()
	if err != nil {
		logrus.Errorf("Rename failed when load inode: %v", err)
		return fuse.EIO
	}
	// 获取新目录 inode
	newDirInoCtx := fs.GetInode(input.Newdir)
	err = newDirInoCtx.LoadInode()
	if err != nil {
		logrus.Errorf("Rename failed when load inode: %v", err)
		return fuse.EIO
	}
	// 删除旧目录中的条目
	err = oldDirInoCtx.RemoveEntry(oldName)
	if err != nil {
		logrus.Errorf("Rename failed when remove entry by name: %v", err)
		return fuse.EIO
	}
	// 向新目录中添加条目
	err = newDirInoCtx.AddEntry(newName, fileInodeCtx.coreCache.Ino)
	if err != nil {
		logrus.Errorf("Rename failed when add entry by name: %v", err)
		return fuse.EIO
	}
	// 更新旧目录 inode
	err = oldDirInoCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Rename failed when sync inode of old dir: %v", err)
		return fuse.EIO
	}
	// 更新新目录 inode
	err = newDirInoCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Rename failed when sync inode of new dir: %v", err)
		return fuse.EIO
	}

	logrus.Debugf("[out] op=%s", "Rename")
	return fuse.OK
}

func (fs *PoundFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s", "Link")
	logrus.Debugf("[out] op=%s", "Link")
	return fuse.ENOSYS
}

func getXAttrCacheKey(ino uint64, key string) string {
	return fmt.Sprintf("%d_%s", ino, key)
}

func (fs *PoundFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, attr=%s ", "GetXAttr", header.NodeId, attr)
	logrus.Debugf("[out] op=%s", "GetXAttr")
	item, ok := fs.xattrCache[getXAttrCacheKey(header.NodeId, attr)]
	if !ok {
		return 0, fuse.ENODATA
	} else {
		copy(dest, item)
		return uint32(len(item)), fuse.OK
	}
}
// SetXAttr 设置文件的扩展属性
func (fs *PoundFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	logrus.Infof("[in ] op=%s, attr=%s, value=%v", "SetXAttr", attr, data)
	logrus.Debugf("[out] op=%s", "SetXAttr")
	fs.xattrCache[getXAttrCacheKey(input.InHeader.NodeId, attr)] = data

	return fuse.ENOSYS
}

// ListXAttr 获取文件的所有扩展属性
func (fs *PoundFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v", "ListXAttr", header.NodeId)
	logrus.Debugf("[out] op=%s", "ListXAttr")
	return 0, fuse.ENOSYS
}

// RemoveXAttr 删除文件的扩展属性
func (fs *PoundFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	logrus.Infof("[in ] op=%s, attr=%s", "RemoveXAttr", attr)
	logrus.Debugf("[out] op=%s", "RemoveXAttr")
	return fuse.ENOSYS
}

// Access 检查文件的访问权限
func (fs *PoundFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, mask=%v", "Access", input.NodeId, accessMaskToStr(input.Mask))
	logrus.Debugf("[out] op=%s", "Access")
	return fuse.OK
}

// Create 创建文件
func (fs *PoundFS) Create(cancel <-chan struct{}, input *fuse.CreateIn,
	name string, out *fuse.CreateOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s, name=%s, parent_ino=%v, mode=%v, flags=%v", "Create", name, input.NodeId, StrMode(uint16(input.Mode)), DecodeFlags(input.Flags))

	parentInodeCtx := fs.GetInode(input.NodeId)
	parentInodeCtx.LoadInode()
	newBlk, err := fs.mp.AllocBlock(0, 4096/BlockSize)
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	newInodeCtx := NewInoContext(fs.dev, newBlk)
	err = newInodeCtx.InitInode(uint16(input.Mode))
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	inode := newInodeCtx.coreCache
	inode.Flags = uint32(input.Flags)
	inode.Uid = input.Uid
	inode.Gid = input.Gid
	inode.NLocBlk = 4096 / BlockSize
	err = newInodeCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	err = newInodeCtx.InitDataBlock()
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	err = newInodeCtx.SyncInode()
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	// 放到父目录
	err = parentInodeCtx.AddEntry(name, inode.Ino)
	if err != nil {
		logrus.Errorf("Create failed: %v", err)
		return fuse.EIO
	}
	parentInodeCtx.SyncInode()

	out.NodeId = inode.Ino
	// Generation: 同一个文件， nodeid和gen的组合，必须在整个文件系统的生命周期中唯一
	out.Generation = NextGen()
	out.EntryOut = fuse.EntryOut{
		NodeId:     inode.Ino,
		Generation: 1,
		Attr: fuse.Attr{
			Ino:       inode.Ino,
			Blocks:    1,
			Nlink:     inode.Nlink,
			Mode:      uint32(input.Mode),
			Owner:     fuse.Owner{Uid: inode.Uid, Gid: inode.Gid},
			Size:      0,
			Atime:     TimestampSecPart(inode.Atime),
			Mtime:     TimestampSecPart(inode.Mtime),
			Ctime:     TimestampSecPart(inode.Ctime),
			Atimensec: TimestampNsecPart(inode.Atime),
			Mtimensec: TimestampNsecPart(inode.Mtime),
			Ctimensec: TimestampNsecPart(inode.Ctime),
		},
	}

	out.OpenOut = fuse.OpenOut{
		Fh:        fs.openfiles.Register(inode.Ino, input.Flags),
		OpenFlags: input.Flags,
	}

	logrus.Infof("[out] op=%s, ino=%v", "Create", inode.Ino)
	return fuse.OK
}

func (fs *PoundFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, flags=%v", "OpenDir", input.NodeId, DecodeFlags(input.Flags))
	inodeCtx := fs.GetInode(input.NodeId)
	if inodeCtx == nil {
		logrus.Error("OpenDir failed: inode not found")
		return fuse.ENOENT
	}
	out.Fh = fs.openfiles.Register(inodeCtx.ino, input.Flags)
	// out.OpenFlags = input.Flags
	// out.OpenFlags = fuse.FOPEN_DIRECT_IO
	logrus.Debugf("[out] op=%s, out=%s", "OpenDir", JsonStringify(out))
	return fuse.OK
}

// 当读文件时，会调用 Read Flush Release
func (fs *PoundFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, off=%d", "Read", input.NodeId, input.Offset)
	inodeCtx := fs.GetInode(input.NodeId)
	if inodeCtx == nil {
		return nil, fuse.ENOENT
	}
	nbytes, err := inodeCtx.Read(input.Offset, buf)
	if err != nil {
		return nil, fuse.EIO
	}
	logrus.Infof("[out] op=%s, ino=%v, nbytes=%v, out=%s", "Read", inodeCtx.ino, nbytes, PreviewBuffer(buf, int(Min(nbytes, 512))))
	return fuse.ReadResultData(buf), fuse.OK
}

func PreviewBuffer(buf []byte, length int) string {
	if len(buf) < length {
		length = len(buf)
	}
	str := string(buf[:length])
	strHex := hex.EncodeToString(buf[:length])
	return fmt.Sprintf("%s(%s)", str, strHex)
}

func (fs *PoundFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	logrus.Infof("[in ] op=%s", "GetLk")
	logrus.Debugf("[out] op=%s", "GetLk")
	return fuse.ENOSYS
}

func (fs *PoundFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	logrus.Infof("[in ] op=%s", "SetLk")
	logrus.Debugf("[out] op=%s", "SetLk")
	return fuse.ENOSYS
}

/**

F_SETLK, F_SETLKW, and F_GETLK are used to acquire, release, and
      test for the existence of record locks (also known as byte-range,
      file-segment, or file-region locks).  The third argument, lock,
      is a pointer to a structure that has at least the following
      fields (in unspecified order).

就是说，F_SETLK, F_SETLKW, F_GETLK 是用来获取、释放、和测试文件锁的，

*/
func (fs *PoundFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	logrus.Infof("[in ] op=%s", "SetLkw")
	logrus.Debugf("[out] op=%s", "SetLkw")
	// in.LkFlags LOCK_SH, LOCK_EX or LOCK_UN
	return fuse.OK
}

func (fs *PoundFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	logrus.Infof("[in ] op=%s, ino=%v, fh=%v, flags=%v", "Release", input.NodeId, input.Fh, DecodeFlags(input.Flags))
	// input.Flags
	// input.NodeId
	fs.openfiles.Remove(input.Fh)
	logrus.Debugf("[out] op=%s", "Release")
}

func (fs *PoundFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	logrus.Infof("[in ] op=%s, ino=%v, data=%s, len=%d", "Write", input.NodeId, PreviewBuffer(data, int(input.Size)), input.Size)
	inoCtx := fs.GetInode(input.NodeId)
	if inoCtx == nil {
		logrus.Errorf("Write failed: inode ino=%v not found", input.NodeId)
		return 0, fuse.ENOENT
	}
	nbytes, err := inoCtx.Write(input.Offset, data)
	if err != nil {
		logrus.Errorf("Write failed: %v", err)
		return 0, fuse.EIO
	}
	logrus.Infof("[out] op=%s "+Yellow("n=%v"), "Write", nbytes)
	return uint32(nbytes), fuse.OK
}

func (fs *PoundFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	logrus.Infof("[in ] op=%s, ino=%v, fh=%v", "Flush", input.NodeId, input.Fh)
	logrus.Debugf("[out] op=%s", "Flush")
	return fuse.OK
}

func (fs *PoundFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	logrus.Infof("[in ] op=%s", "Fsync")
	logrus.Debugf("[out] op=%s", "Fsync")
	return fuse.OK
}

func (fs *PoundFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	logrus.Infof("[in ] op=%s, ino=%v, off=%d", "ReadDir", input.NodeId, input.Offset)
	ino := input.InHeader.NodeId
	if ino == RootIno {
		ino = uint64(fs.mp.AgCtx[0].Agi.Meta.Root)
	}
	inodeCtx := NewInoContext(fs.dev, ino)
	err := inodeCtx.LoadInode()
	if err != nil {
		logrus.Errorf("ReadDir: failed to load inode %v", err)
		return fuse.ENOENT
	}
	ents, err := inodeCtx.GetEntries()
	if err != nil {
		logrus.Errorf("op=%s, ino=%d, err=%s", "ReadDir", ino, err)
		return fuse.EINTR
	}
	// . and ..
	// l.AddDirEntry(fuse.DirEntry{Name: ".", Ino: ino})
	// l.AddDirEntry(fuse.DirEntry{Name: "..", Ino: inodeCtx.dirSfHdr.Parent})
	for _, ent := range ents {
		fileInoCtx := NewInoContext(fs.dev, ent.Ino)
		err := fileInoCtx.LoadInode()
		if err != nil {
			logrus.Errorf("ReadDir: %v", err)
			return fuse.EINTR
		}
		var e fuse.DirEntry
		e.Name = string(ent.Name)
		e.Mode = uint32(fileInoCtx.coreCache.Mode)
		e.Ino = ent.Ino
		ok := l.AddDirEntry(e)
		if !ok {
			return fuse.EINTR
		}
	}
	logrus.Debugf("op=%s, ino=%d, entries=%d", "ReadDir", ino, len(ents)+2)
	return fuse.OK
}

/*
op=ReadDirPlus, in={
    "Length": 80,
    "Opcode": 44,
    "Unique": 22,
    "NodeId": 1,
    "Uid": 0,
    "Gid": 0,
    "Pid": 501374,
    "Fh": 0,
    "Offset": 0,
    "Size": 4096,
    "ReadFlags": 0,
    "LockOwner": 0,
    "Flags": 100352,
    "Padding": 0
}
*/
func (fs *PoundFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	logrus.Infof("op=%s, ino=%v, flags=%v, offset=%v, size=%v", "ReadDirPlus", input.NodeId, DecodeFlags(input.Flags), input.Offset, input.Size)
	ino := input.InHeader.NodeId
	if ino == RootIno {
		ino = uint64(fs.mp.AgCtx[0].Agi.Meta.Root)
	}
	inodeCtx := NewInoContext(fs.dev, ino)
	err := inodeCtx.LoadInode()
	if err != nil {
		logrus.Errorf("ReadDirPlus: failed to load inode %v", err)
		return fuse.ENOENT
	}
	ents, err := inodeCtx.GetEntries()
	if err != nil {
		logrus.Errorf("op=%s, ino=%d, err=%s", "ReadDirPlus", ino, err)
		return fuse.EINTR
	}
	if input.Offset > 0 {
		return fuse.OK
	}
	// . and ..
	l.AddDirLookupEntry(fuse.DirEntry{Name: ".", Ino: ino})
	l.AddDirLookupEntry(fuse.DirEntry{Name: "..", Ino: inodeCtx.dirSfHdr.Parent})
	for _, ent := range ents {
		fileInoCtx := NewInoContext(fs.dev, ent.Ino)
		err := fileInoCtx.LoadInode()
		if err != nil {
			logrus.Errorf("ReadDirPlus: %v", err)
			return fuse.EINTR
		}
		var e fuse.DirEntry
		e.Name = string(ent.Name)
		e.Mode = uint32(fileInoCtx.coreCache.Mode)
		e.Ino = ent.Ino
		entryDest := l.AddDirLookupEntry(e)
		entryDest.Ino = uint64(fuse.FUSE_UNKNOWN_INO)
		// No need to fill attributes for . and ..
		if e.Name == "." || e.Name == ".." {
			continue
		}
		logrus.Warnf("search for %s", e.Name)
		stat := fs.Lookup(cancel, &input.InHeader, e.Name, entryDest)
		if stat != fuse.OK {
			logrus.Errorf("ReadDirPlus: failed to lookup %s", e.Name)
			return stat
		}
		logrus.Infof("[out item] op=%s, ino=%d", "ReadDirPlus", entryDest.Ino)

	}
	logrus.Debugf("op=%s, ino=%d, entries=%d", "ReadDirPlus", ino, len(ents)+2)

	return fuse.OK
}

func (fs *PoundFS) ReleaseDir(input *fuse.ReleaseIn) {
	logrus.Infof("[in ] op=%s, ino=%v, fh=%d flags=%v", "ReleaseDir", input.NodeId, input.Fh, DecodeFlags(input.Flags))
	fs.openfiles.Remove(input.Fh)
	logrus.Debugf("[out]op=%s", "ReleaseDir")
}

func (fs *PoundFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	logrus.Debugf("[in ] op=%s", "FsyncDir")
	logrus.Debugf("[out] op=%s", "FsyncDir")
	return fuse.ENOSYS
}

func (fs *PoundFS) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	logrus.Debugf("[in ] op=%s", "Fallocate")
	logrus.Debugf("[out] op=%s", "Fallocate")
	return fuse.ENOSYS
}

func (fs *PoundFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	logrus.Debugf("[in ] op=%s", "CopyFileRange")
	logrus.Debugf("[out] op=%s", "CopyFileRange")
	return 0, fuse.ENOSYS
}

func (fs *PoundFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	logrus.Debugf("[in ] op=%s", "Lseek")
	logrus.Debugf("[out] op=%s", "Lseek")
	return fuse.ENOSYS
}
