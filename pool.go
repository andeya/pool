// 通用资源池，动态增加池成员。
package pool

import (
	"sync"
	"time"
)

// 池成员接口
type Fish interface {
	// 返回指针类型的资源实例
	New() Fish
	// 自毁方法，在被资源池删除时调用
	Close()
	// 释放至资源池之前，清理重置自身
	Clean()
}

// 资源池
type Pool struct {
	Cap  int
	Src  map[Fish]bool // Fish须为指针类型
	Fish               // 池成员接口
	sync.Mutex
}

// 新建一个资源池，默认容量为1024
func NewPool(fish Fish, size ...int) *Pool {
	if len(size) == 0 {
		size = append(size, 1024)
	}
	return &Pool{
		Cap:  size[0],
		Src:  make(map[Fish]bool),
		Fish: fish,
	}
}

// 并发安全地获取一个连接
func (self *Pool) GetOne() Fish {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	for {
		for k, v := range self.Src {
			if v {
				continue
			}
			self.Src[k] = true
			return k
		}
		if len(self.Src) <= self.Cap {
			self.increment()
		} else {
			time.Sleep(5e8)
		}
	}
	return nil
}

func (self *Pool) Free(m ...Fish) {
	for i, count := 0, len(m); i < count; i++ {
		m[i].Clean()
		self.Src[m[i]] = false
	}
}

// 关闭并删除指定连接
func (self *Pool) Remove(m ...Fish) {
	for _, c := range m {
		c.Close()
		delete(self.Src, c)
	}
}

// 重置资源池
func (self *Pool) Reset() {
	for k, _ := range self.Src {
		k.Close()
		delete(self.Src, k)
	}
}

// 根据情况自动动态增加连接
func (self *Pool) increment() {
	if len(self.Src) < self.Cap {
		self.Src[self.Fish.New()] = false
	}
}
