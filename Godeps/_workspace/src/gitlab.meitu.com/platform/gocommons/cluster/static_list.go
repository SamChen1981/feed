package cluster

import (
	"errors"
	"fmt"
	"sync"
)

type Item struct {
	Addr string
	Opt  string
}

type StaticList struct {
	cluster Cluster
	addrs   map[string]bool
	lock    sync.Mutex
}

func NewStaticList(addrs []string, c Cluster) (*StaticList, error) {
	if addrs == nil || len(addrs) == 0 {
		return nil, errors.New("addrs is empty")
	}
	if c == nil {
		return nil, errors.New("cluster is nil")
	}
	sl := &StaticList{
		cluster: c,
		addrs:   make(map[string]bool),
	}
	for _, addr := range addrs {
		err := sl.Add(addr)
		if err != nil {
			return nil, err
		}
	}
	return sl, nil
}

func (this *StaticList) Add(addr string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	if _, ok := this.addrs[addr]; ok {
		return errors.New("duplicate addrs " + addr)
	}
	err := this.cluster.Add(addr, "")
	if err != nil {
		return err
	}
	this.addrs[addr] = true
	return nil
}

func (this *StaticList) Del(addr string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if _, ok := this.addrs[addr]; !ok {
		return
	}
	delete(this.addrs, addr)
	this.cluster.Del(addr)
}

func (this *StaticList) Update(addrs []string) string {
	m := make(map[string]bool)
	for _, a := range addrs {
		m[a] = true
	}
	delList := diff(this.addrs, m)
	for _, addr := range delList {
		this.Del(addr)
	}
	addList := diff(m, this.addrs)
	for _, addr := range addList {
		this.Add(addr)
	}
	return fmt.Sprintf("delete address %v, add address %v", delList, addList)
}

func (this *StaticList) Stop() {
}

func diff(a, b map[string]bool) []string {
	res := make([]string, 0)
	for k, _ := range a {
		if _, ok := b[k]; !ok {
			res = append(res, k)
		}
	}
	return res
}
