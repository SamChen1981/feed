package cluster

type HashCluster struct {
	pollCluster *PollCluster
	connMap     map[string]Connector
}

func NewHashCluster(d Dial) *HashCluster {
	hc := &HashCluster{
		pollCluster: NewPollCluster(d),
		connMap:     make(map[string]Connector),
	}
	return hc
}

func (hc *HashCluster) Get(k interface{}) Connector {
	if k == nil {
		return nil
	}
	key, ok := k.(string)
	if !ok {
		return nil
	}
	conn, ok := hc.connMap[key]
	for ok {
		if !conn.Ready() {
			delete(hc.connMap, key)
			break
		}
		return conn
	}
	conn = hc.pollCluster.Get(nil)
	if conn != nil {
		hc.connMap[key] = conn
	}
	return conn
}

func (hc *HashCluster) GetAllConns() []Connector {
	//FIXME
	return nil
}

func (hc *HashCluster) Add(addr string, opt string) error {
	return hc.pollCluster.Add(addr, opt)
}

func (hc *HashCluster) Del(addr string) {
	hc.pollCluster.Del(addr)
}

func (hc *HashCluster) Stop() {
	hc.pollCluster.Stop()
}
