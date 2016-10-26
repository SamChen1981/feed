package cluster

import (
	"fmt"
	"gitlab.meitu.com/platform/go-radar/radar.v2"
)

const (
	actionChanLen = 100
)

type EtcdObserver struct {
	actions  chan *radar.Response
	server   radar.Servicer
	consumer EtcdConsumer
	vc       EtcdValueConsumer
	sc       EtcdStatusConsumer
	rc       EtcdReportConsumer
}

func NewEtcdObserver(root string, addrs []string, c EtcdConsumer) (*EtcdObserver, error) {
	if len(root) == 0 || len(addrs) == 0 || c == nil {
		return nil, fmt.Errorf("NewEtcdObserver param is bad")
	}
	ec := &EtcdObserver{
		consumer: c,
		actions:  make(chan *radar.Response, actionChanLen),
	}
	ec.vc, _ = c.(EtcdValueConsumer)
	ec.sc, _ = c.(EtcdStatusConsumer)
	ec.rc, _ = c.(EtcdReportConsumer)
	var err error
	ec.server, err = radar.CreateEtcdService(root, addrs)
	if err != nil {
		return nil, err
	}

	nodes, err := ec.server.Watch(ec.actions)
	if err != nil {
		return nil, err
	}
	if nodes != nil {
		for _, node := range nodes {
			ec.consumer.Add(node.Addr, node.Value)
		}
	}

	go ec.handler()
	return ec, nil
}

func (ec *EtcdObserver) handler() {
	for resp := range ec.actions {
		switch resp.Action {
		case radar.Delete:
			ec.consumer.Del(resp.Node.Addr)
		case radar.Add:
			ec.consumer.Add(resp.Node.Addr, resp.Node.Value)
		case radar.SetValue:
			if ec.vc != nil {
				ec.vc.SetValue(resp.Node.Addr, resp.Node.Value)
			}
		case radar.SetReport:
			if ec.rc != nil {
				ec.rc.SetReport(resp.Node.Addr, resp.Node.Report)
			}
		case radar.SetStatus:
			if ec.sc != nil {
				ec.sc.SetStatus(resp.Node.Addr, resp.Node.Status)
			}
		default:
			//log.Errorf("Unsupported action %s", resp.Action)
		}
	}
}

func (ec *EtcdObserver) Stop() {
	ec.server.StopWatch()
}
