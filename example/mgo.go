package mgo

import (
	"log"

	mgo "gopkg.in/mgo.v2"

	"github.com/henrylee2cn/pool"
)

type MgoSrc struct {
	*mgo.Session
}

const (
	CONN_STR = "127.0.0.1:27017"
	MAX_CONN = 1024
	MAX_IDLE = 512
	GC_TIME  = 60e9
)

var (
	session, err = func() (session *mgo.Session, err error) {
		session, err = mgo.Dial(CONN_STR)
		if err != nil {
			log.Printf("MongoDB：%v\n", err)
		} else if err = session.Ping(); err != nil {
			log.Printf("MongoDB：%v\n", err)
		} else {
			session.SetPoolLimit(MAX_CONN)
		}
		return
	}()

	MgoPool = pool.ClassicPool(
		MAX_CONN,
		MAX_IDLE,
		func() (pool.Src, error) {
			return &MgoSrc{session.Clone()}, err
		},
		GC_TIME)
)

// 判断资源是否可用
func (self *MgoSrc) Usable() bool {
	if self.Session == nil || self.Session.Ping() != nil {
		return false
	}
	return true
}

// 使用后的重置方法
func (*MgoSrc) Reset() {}

// 被资源池删除前的自毁方法
func (self *MgoSrc) Close() {
	if self.Session == nil {
		return
	}
	self.Session.Close()
}

func Refresh() {
	session, err = mgo.Dial(CONN_STR)
	if err != nil {
		log.Printf("MongoDB：%v\n", err)
	} else if err = session.Ping(); err != nil {
		log.Printf("MongoDB：%v\n", err)
	} else {
		session.SetPoolLimit(MAX_CONN)
	}
}

func Error() error {
	return err
}
