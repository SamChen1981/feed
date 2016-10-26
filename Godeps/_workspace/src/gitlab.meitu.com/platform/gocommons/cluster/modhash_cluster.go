package cluster

import (
	"gitlab.meitu.com/platform/gocommons/sharding"
)

type ModhashCluster struct {
	addrs   []string
	conns   []Connector
	servers []*sharding.ShardServer
	dial    Dial
}

func NewModhashCluster(dial Dial) *ModhashCluster {
	return &ModhashCluster{dial: dial}
}

func (mhc *ModhashCluster) Get(opt interface{}) Connector {
	key := opt.(string)
	sd := sharding.NewCompatSharding(mhc.servers)
	idx, _ := sd.GetShardServer(key)
	return mhc.conns[idx]
}

func (mhc *ModhashCluster) GetAllConns() []Connector {
	return mhc.conns
}

func (mhc *ModhashCluster) Add(addr, opt string) error {
	mhc.addrs = append(mhc.addrs, addr)
	if conn, err := mhc.dial(addr, opt); err != nil {
		return err
	} else {
		mhc.conns = append(mhc.conns, conn)
		mhc.servers = append(mhc.servers, &sharding.ShardServer{addr, 0})
	}
	return nil
}

//Not supported
func (mhc *ModhashCluster) Del(addr string) {
}

//Not supported
func (mhc *ModhashCluster) Stop() {
}
