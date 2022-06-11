package main

import "github.com/sirupsen/logrus"

// 打开文件表，管理系统级别的文件把手

type FileHandle struct {
	fh    uint64
	ino   uint64
	flags uint32
}

type OpenfileMap struct {
	// key: fh
	files map[uint64]*FileHandle
}

func NewOpenfileMap() *OpenfileMap {
	m := &OpenfileMap{
		files: map[uint64]*FileHandle{},
	}
	return m
}

func (m *OpenfileMap) Get(fh uint64) *FileHandle {
	return m.files[fh]
}

func (m *OpenfileMap) Register(ino uint64, flags uint32) uint64 {
	fh := NextGen()
	m.files[fh] = &FileHandle{
		fh:    fh,
		ino:   ino,
		flags: flags,
	}
	logrus.Infof(Red("[FS_HANDLE] Register fh=%v ino=%v flags=%v"), fh, ino, DecodeFlags(flags))
	return fh
}

func (m *OpenfileMap) Remove(fh uint64) {
	logrus.Infof(Red("[FS_HANDLE] Remove fh=%v, ino=%v"), fh, m.files[fh].ino)
	delete(m.files, fh)
}

var nextgen = 1

func NextGen() uint64 {
	defer func() {
		nextgen++
	}()
	return uint64(nextgen)
}

/*

什么是文件的世代号

摘自：https://libfuse.github.io/doxygen/structfuse__entry__param.html#a4c673ec62c76f7d63d326407beb1b463

Generation number for this entry.

If the file system will be exported over NFS, the ino/generation pairs
need to be unique over the file system's lifetime (rather than just the mount time).
So if the file system reuses an inode after it has been deleted,
it must assign a new, previously unused generation number to the inode at the same time.

如果文件系统将被导出到 NFS，则 ino/generation 对需要在文件系统的生命周期内（而不是只是挂载时间）唯一。
因此，如果文件系统在删除后重新使用 inode，它必须在同一时间给 inode 分配新的世代号
*/
