package sharding

import (
	"hash/crc32"
)

func compatHash(input string) uint32 {
	return (crc32.ChecksumIEEE([]byte(input)) >> 16) & 0x7fff
}

type compatHashing struct {
	servers []*ShardServer
}

func (c *compatHashing) GetServers() []*ShardServer {
	return c.servers
}

func (c *compatHashing) GetShardServer(key string) (index int, server *ShardServer) {
	index = c.GetShardIndex(key)
	server = c.servers[index]
	return
}

func (c *compatHashing) GetShardIndex(key string) int {
	return int(compatHash(key)) % len(c.servers)
}

// NewCompatSharding returns new compat shading implementation
func NewCompatSharding(servers []*ShardServer) Sharding {
	return &compatHashing{
		servers: servers,
	}
}
