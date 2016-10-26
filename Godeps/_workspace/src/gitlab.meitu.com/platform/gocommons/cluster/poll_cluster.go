package cluster

import (
	"fmt"
	"gitlab.meitu.com/platform/gocommons/log"
	"sync/atomic"
	"time"
)

type Action int

const (
	Add = Action(iota)
	Del = Action(iota)

	QueueNum = 10

	MaxSleepTime = 10
)

type item struct {
	action Action
	addr   string
	opt    string
}

type connUnit struct {
	conn Connector
	done chan bool
	opt  string
}

//TODO 考虑这里是否使用opt
type Dial func(addr string, opt string) (Connector, error)

type PollCluster struct {
	stop bool
	done chan bool

	dial       Dial
	queue      chan *item
	readyQueue chan Connector
	resetQueue chan string

	id          uint32
	serversList []Connector
	serversMap  map[string]*connUnit
}

func NewPollCluster(d Dial) *PollCluster {
	pc := &PollCluster{
		stop:        false,
		done:        make(chan bool),
		dial:        d,
		queue:       make(chan *item, QueueNum),
		readyQueue:  make(chan Connector, QueueNum),
		resetQueue:  make(chan string, 1),
		serversList: make([]Connector, 0),
		serversMap:  make(map[string]*connUnit),
	}
	go pc.run()
	return pc
}

func (pc *PollCluster) Get(opt interface{}) Connector {
	for !pc.stop {
		list := pc.serversList
		if len(list) == 0 {
			return nil
		}
		id := atomic.AddUint32(&pc.id, 1) % uint32(len(list))
		if list[id].Ready() {
			return list[id]
		}
		pc.resetConn(list[id].Addr())
	}
	return nil
}

func (pc *PollCluster) GetAllConns() []Connector {
	return pc.serversList
}

func (pc *PollCluster) Add(addr string, opt string) error {
	if pc.stop {
		return fmt.Errorf("PollCluster is already stop")
	}
	it := &item{
		action: Add,
		addr:   addr,
		opt:    opt,
	}
	select {
	case pc.queue <- it:
	}
	log.Debugf("Add a connection '%s' to queue", addr)
	return nil
}

func (pc *PollCluster) Del(addr string) {
	if pc.stop {
		return
	}
	it := &item{
		action: Del,
		addr:   addr,
	}
	select {
	case pc.queue <- it:
	}
	log.Debugf("Del a connection '%s' to queue", addr)
}

func (pc *PollCluster) Stop() {
	pc.stop = true
	close(pc.done)
	//TODO close chan
}

func (pc *PollCluster) resetConn(addr string) {
	if pc.stop {
		return
	}
	select {
	case pc.resetQueue <- addr:
		log.Debugf("Reset a connection '%s' to queue", addr)
	default:
		//reset会在Get的时候调用，所以这里并不需要等待，直接返回即可
	}
}

func (pc *PollCluster) run() {
	for !pc.stop {
		select {
		case it, ok := <-pc.queue:
			if !ok {
				log.Debugf("queue is close, can't get data")
				return
			}
			switch it.action {
			case Add:
				err := pc.add(it.addr, it.opt)
				if err != nil {
					log.Warnf("%s", err.Error())
				}
			case Del:
				pc.del(it.addr)
			default:
				log.Warnf("Not found Action %v", it.action)
			}
		case conn, ok := <-pc.readyQueue:
			if !ok {
				log.Debugf("readyQueue close, can't get data")
				return
			}
			pc.ready(conn)
		case addr, ok := <-pc.resetQueue:
			if !ok {
				log.Debugf("resetQueue is close, can't get data")
				return
			}
			if cu, exist := pc.serversMap[addr]; exist {
				pc.add(addr, cu.opt)
			} else {
				log.Infof("%s is already close", addr)
			}
		case <-pc.done:
			log.Infof("close check connection")
			for len(pc.serversMap) > 0 {
				for addr := range pc.serversMap {
					pc.del(addr)
					break
				}
			}
			return
		}
	}
}

func (pc *PollCluster) add(addr, opt string) error {
	if len(addr) == 0 {
		return fmt.Errorf("Add: address is empty")
	}
	if cu, exists := pc.serversMap[addr]; exists {
		if cu.conn != nil && !cu.conn.Ready() {
			log.Warnf("connect %s is bad, reset", addr)
			pc.del(addr)
		} else {
			return fmt.Errorf("duplicate add server " + addr)
		}
	}

	cu := &connUnit{
		conn: nil,
		done: make(chan bool),
		opt:  opt,
	}
	pc.serversMap[addr] = cu

	go func() {
		n := 0
		t := time.NewTimer(24 * time.Hour) //设置一个无法超时的时间
		for !pc.stop {
			conn, err := pc.dial(addr, opt)
			if err == nil {
				select {
				case pc.readyQueue <- conn:
				}
				return
			}
			n++
			log.Errorf("Connect to %s failed, after %d second try. %s",
				addr, n, err.Error())
			t.Reset(time.Duration(n) * time.Second)
			select {
			case <-cu.done:
				log.Infof("user stop connect to %s", addr)
				return
			case <-t.C:
			}
			if n == MaxSleepTime {
				n = 0
			}
		}
	}()
	return nil
}

func (pc *PollCluster) del(addr string) error {
	if len(addr) == 0 {
		return fmt.Errorf("Del: address is empty")
	}
	cu, exists := pc.serversMap[addr]
	if !exists {
		return fmt.Errorf("Delete failed, '%s' not exist", addr)
	}

	delete(pc.serversMap, addr)

	for id := range pc.serversList {
		if pc.serversList[id].Addr() == addr {
			list := make([]Connector, len(pc.serversList)-1)
			copy(list, pc.serversList[0:id])
			copy(list[id:], pc.serversList[id+1:])
			pc.serversList = list
			break
		}
	}
	if cu.conn != nil {
		cu.conn.Close()
	}
	close(cu.done)
	log.Infof("Delete a connection '%s'", addr)
	return nil
}

func (pc *PollCluster) ready(conn Connector) {
	if conn == nil {
		log.Errorf("ready param conn is nil")
		return
	}
	cu, exists := pc.serversMap[conn.Addr()]
	if !exists {
		log.Errorf("in ready func, can't find '%s' in map.", conn.Addr())
		return
	}
	if cu.conn != nil {
		log.Errorf("already exist a connection")
		return
	}
	cu.conn = conn
	pc.serversList = append(pc.serversList, conn)
	log.Infof("Add a connection '%s'", conn.Addr())
}

func (pc *PollCluster) reset(addr string) error {
	if len(addr) == 0 {
		return fmt.Errorf("Reset: address is empty")
	}
	cu, exists := pc.serversMap[addr]
	if exists {
		if cu.conn == nil {
			log.Debugf("connection '%s' is already reset", addr)
			return nil
		}
		if cu.conn.Ready() {
			log.Debugf("connection is already recovery, don't need reset")
			return nil
		}

		delete(pc.serversMap, addr)
		for id := range pc.serversList {
			if pc.serversList[id].Addr() == addr {
				list := make([]Connector, len(pc.serversList)-1)
				copy(list, pc.serversList[0:id])
				copy(list[id:], pc.serversList[id+1:])
				pc.serversList = list
				break
			}
		}
		if cu.conn != nil {
			cu.conn.Close()
		}
		close(cu.done)
	}
	pc.add(addr, "")
	return nil
}
