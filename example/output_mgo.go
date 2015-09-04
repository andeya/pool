package collector

import (
	"github.com/henrylee2cn/pool"
	"gopkg.in/mgo.v2"
	"log"
	"time"
	// "gopkg.in/mgo.v2/bson"
)

type MgoOutput struct {
	// 数据库服务器地址
	Host string
	// 默认数据库
	DefaultDB string
	// key:蜘蛛规则清单
	// value:数据库名
	DBClass map[string]string
	// 非默认数据库时以当前时间为集合名
	// h: 精确到小时 (格式 2015-08-28-09)
	// d: 精确到天 (格式 2015-08-28)
	TableFmt string
}

var MGO_OUTPUT = &MgoOutput{
	Host:      "127.0.0.1:27017",
	DefaultDB: "pholcus",
	DBClass:   make(map[string]string),
	TableFmt:  "d",
}

/************************ MongoDB 输出 ***************************/
var mgoPool = pool.NewPool(new(mgoFish), 1024)

type mgoFish struct {
	*mgo.Session
}

func (self *mgoFish) New() pool.Fish {
	mgoSession, err := mgo.Dial(MGO_OUTPUT.Host)
	if err != nil {
		panic(err)
	}
	mgoSession.SetMode(mgo.Monotonic, true)
	return &mgoFish{Session: mgoSession}
}

// 判断连接有效性
func (self *mgoFish) Usable() bool {
	if self.Session.Ping() != nil {
		return false
	}
	return true
}

// 自毁方法，在被对象池删除时调用
func (self *mgoFish) Close() {
	self.Session.Close()
}

func (*mgoFish) Clean() {}

// 每个爬取任务的数据容器
type Collector struct {
	Name    string
	Keyword string
	Docker  []*DataCell
}

type DataCell struct {
	RuleName string      //规定Data中的key
	Data     interface{} //数据存储,key须与Rule的Fields保持一致
}

// 数据库输出
func Output(self *Collector, dataIndex int) {
	var err error
	//连接数据库
	mgoSession := mgoPool.GetOne().(*mgoFish)
	defer mgoPool.Free(mgoSession)

	dbname, tabname := dbOrTabName(self)
	db := mgoSession.DB(dbname)

	if tabname == "" {
		for _, datacell := range self.Docker {
			tabname = tabName(self, datacell.RuleName)
			collection := db.C(tabname)
			err = collection.Insert(datacell)
			if err != nil {
				log.Println(err)
			}
		}
		return
	}

	collection := db.C(tabname)

	for i, count := 0, len(self.Docker); i < count; i++ {
		err = collection.Insert((interface{})(self.Docker[i]))
		if err != nil {
			log.Println(err)
		}
	}
}

// 返回数据库及集合名称
func dbOrTabName(c *Collector) (dbName, tableName string) {
	if v, ok := MGO_OUTPUT.DBClass[c.Name]; ok {
		switch MGO_OUTPUT.TableFmt {
		case "h":
			return v, time.Now().Format("2006-01-02-15")
		case "d":
			fallthrough
		default:
			return v, time.Now().Format("2006-01-02")
		}
	}
	return MGO_OUTPUT.DefaultDB, ""
}

// 当输出数据库为MGO_OUTPUT.DefaultDB时，使用tabName获取table名
func tabName(c *Collector, ruleName string) string {
	var k = c.Keyword
	if k != "" {
		k = "-" + k
	}
	return c.Name + "-" + ruleName + k
}
