// Package sharding contains sharding algorithm implementation that help you distribute data across multi server
package sharding

import (
	"strconv"
	"strings"
)

// ShardServer definition contains server address and weight
type ShardServer struct {
	Address string
	Weight  int64
}

// ParseServerString parse input hosts string into ShardServer arrays.
// Input hosts string pattern: host:port[/weight][,host:port[/weight]]...
func ParseServerString(hosts string) []*ShardServer {
	hs := strings.Split(hosts, ",")
	servers := make([]*ShardServer, 0, 0)
	for _, hs := range hs {
		addr := hs
		weight := int64(0)
		if strings.Contains(hs, "/") {
			hw := strings.Split(hs, "/")
			addr = hw[0]
			weight, _ = strconv.ParseInt(hw[1], 10, 64)
		}
		servers = append(
			servers,
			&ShardServer{
				Address: addr,
				Weight:  weight,
			},
		)
	}
	return servers
}

// GetShardServers returns ShardServer arrays for input hosts string array
func GetShardServers(hosts []string) []*ShardServer {
	servers := make([]*ShardServer, 0, 0)
	for _, hs := range hosts {
		addr := hs
		weight := int64(0)
		if strings.Contains(hs, "/") {
			hw := strings.Split(hs, "/")
			addr = hw[0]
			weight, _ = strconv.ParseInt(hw[1], 10, 64)
		}
		servers = append(
			servers,
			&ShardServer{
				Address: addr,
				Weight:  weight,
			},
		)
	}
	return servers
}

// Sharding interface
type Sharding interface {
	// GetShardServer returns sharding server index and instance for given key
	GetShardServer(key string) (index int, server *ShardServer)
	// GetServers returns all sharding servers
	GetServers() []*ShardServer
}
