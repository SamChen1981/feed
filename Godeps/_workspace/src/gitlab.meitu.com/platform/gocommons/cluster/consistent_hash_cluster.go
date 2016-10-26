package cluster

import (
	"gitlab.meitu.com/platform/gocommons/sharding"
	"sync"
)

type server struct {
	addr string
	conn Connector
}

type ConsistentHashCluster struct {
	shards  []*sharding.ShardServer
	servers map[string]*server
	sd      sharding.Sharding
	dial    Dial
	lock    sync.RWMutex
}

func NewConsistentHashCluster(dial Dial) *ConsistentHashCluster {
	return &ConsistentHashCluster{dial: dial, servers: make(map[string]*server)}
}

func (chc *ConsistentHashCluster) Get(opt interface{}) Connector {
	chc.lock.RLock()
	defer chc.lock.RUnlock()
	if chc.sd == nil {
		return nil
	}

	if len(chc.servers) == 0 {
		return nil
	}

	key := opt.(string)
	_, s := chc.sd.GetShardServer(key)
	return chc.servers[s.Address].conn
}

func (chc *ConsistentHashCluster) GetAllConns() []Connector {
	var conns []Connector
	for _, s := range chc.servers {
		conns = append(conns, s.conn)
	}
	return conns
}

func (chc *ConsistentHashCluster) Add(addr, opt string) error {
	conn, err := chc.dial(addr, opt)
	if err != nil {
		return err
	}
	chc.lock.Lock()
	chc.shards = append(chc.shards, &sharding.ShardServer{addr, 0})
	chc.servers[addr] = &server{addr, conn}
	//Create the new sharding whenever a new node is added
	chc.sd = sharding.NewKetamaSharding(chc.shards, false, 0)
	chc.lock.Unlock()
	return nil
}

func (chc *ConsistentHashCluster) Del(addr string) {
	chc.lock.Lock()
	defer chc.lock.Unlock()

	delete(chc.servers, addr)
	chc.shards = nil
	for addr, _ := range chc.servers {
		chc.shards = append(chc.shards, &sharding.ShardServer{addr, 0})
	}
	chc.sd = sharding.NewKetamaSharding(chc.shards, false, 0)
}

//Not supported
func (chc *ConsistentHashCluster) Stop() {
}
