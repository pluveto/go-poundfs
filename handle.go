package main

import (
	log "github.com/sirupsen/logrus"

	"sync"
)

// inode 和 nodeid 是不同的概念
// 前者是文件系统中的 inode 后者是从做系统视角下的 inode

// handled 可以视为对文件描述符的抽象，是用 Go 对象对 fd 的封装
// 为了在线程间共享此对象，使用了 useCount 作为引用计数
type handled struct {
	ino uint64 // 说白了就是 id，但是是 nodeid
	regNum uint64 // 对此 fd 的第几个复用，也叫 generation
	useCount  int    // 对此复用的 fd 的二级复用次数
}

// HandleMap Go 空间中的对象 转换 为 64 位的句柄，这些句柄可以被 linux 内核使用。Linux 内核称其为 NodeId
type handleMap interface {
	// 注册一个对象，返回一个唯一的 (NodeId, regNum) 对
	Register(obj *handled) (handle, regNum uint64)
	Count() int
	// 获取 handled 以 id
	Decode(uint64) *handled
	// 减少 useCount 个引用计数，如果 useCount 达到 0，则顺带释放 handle。如果释放了，则返回值元组第一项为 true
	Forget(handle uint64, useCount int) (bool, *handled)
	// 通过 handle 查询其 ino
	Handle(obj *handled) uint64
	// 判断 nodeid 是否在 map 中
	Has(uint64) bool
}

// 确保 handle 有效
func (h *handled) verify() {
	if h.useCount < 0 {
		log.Panicf("negative lookup useCount %d", h.useCount)
	}
	if (h.useCount == 0) != (h.ino == 0) {
		log.Panicf("registration mismatch: lookup %d id %d", h.useCount, h.ino)
	}
}

type portableHandleMap struct {
	sync.RWMutex
	// 每当 NodeId 被重用，则 regNum 就会被增加，因此 (NodeId, Generation) 就是唯一的。
	regNum uint64
	// 把手表当前使用的句柄数量
	usedNodeIdCount int
	// 被 NodeId 关联的 handle
	handles []*handled
	// handles 中可以使用的 handle 的下标
	freeNodeIds []uint64
}

func newPortableHandleMap() *portableHandleMap {
	return &portableHandleMap{
		// 跳过 0 和 1
		handles: []*handled{nil, nil},
	}
}

func (m *portableHandleMap) Register(obj *handled) (handle, regNum uint64) {
	m.Lock()
	defer m.Unlock()
	// 复用已存在的把手
	if obj.useCount != 0 {
		obj.useCount++
		return obj.ino, obj.regNum
	}
	// Create a new handle number or recycle one on from the free list
	if len(m.freeNodeIds) == 0 {
		obj.ino = uint64(len(m.handles))
		m.handles = append(m.handles, obj)
	} else {
		obj.ino = m.freeNodeIds[len(m.freeNodeIds)-1]
		m.freeNodeIds = m.freeNodeIds[:len(m.freeNodeIds)-1]
		m.handles[obj.ino] = obj
	}
	// Increment regNum number to guarantee the (handle, regNum) tuple
	// is unique
	m.regNum++
	m.usedNodeIdCount++
	obj.regNum = m.regNum
	obj.useCount++

	return obj.ino, obj.regNum
}

func (m *portableHandleMap) Handle(obj *handled) (h uint64) {
	m.RLock()
	if obj.useCount == 0 {
		h = 0
	} else {
		h = obj.ino
	}
	m.RUnlock()
	return h
}

func (m *portableHandleMap) Count() int {
	m.RLock()
	c := m.usedNodeIdCount
	m.RUnlock()
	return c
}

func (m *portableHandleMap) Decode(ino uint64) *handled {
	m.RLock()
	v := m.handles[ino]
	m.RUnlock()
	return v
}

// h 表示 handle 的 id
func (m *portableHandleMap) Forget(ino uint64, useCount int) (forgotten bool, obj *handled) {
	m.Lock()
	obj = m.handles[ino]
	obj.useCount -= useCount
	if obj.useCount < 0 {
		log.Panicf("underflow: handle %d, useCount %d, object %d", ino, useCount, obj.useCount)
	} else if obj.useCount == 0 {
		m.handles[ino] = nil
		m.freeNodeIds = append(m.freeNodeIds, ino)
		m.usedNodeIdCount--
		forgotten = true
		obj.ino = 0
	}
	m.Unlock()
	return forgotten, obj
}

func (m *portableHandleMap) Has(ino uint64) bool {
	m.RLock()
	ok := m.handles[ino] != nil
	m.RUnlock()
	return ok
}
