package sharding

import (
	"crypto/md5"
	"fmt"
	"math"
	"sort"
	"strings"
)

const defaultWeight = int64(0)
const ketamaPointsPerServer = 160
const ketamaPointsPerHash = 4

func ketamaHash(input string, alignment int) uint32 {
	digest := md5.Sum([]byte(input))
	return ((uint32(digest[3+alignment*4]&0xFF) << 24) |
		(uint32(digest[2+alignment*4]&0xFF) << 16) |
		(uint32(digest[1+alignment*4]&0xFF) << 8) |
		uint32(digest[alignment*4]&0xFF))
}

type virtualPoint struct {
	pointer uint32
	shardId int
}

type virtualPoints []virtualPoint

func (vp virtualPoints) Len() int {
	return len(vp)
}

func (vp virtualPoints) Less(i, j int) bool {
	return vp[i].pointer < vp[j].pointer
}

func (vp virtualPoints) Swap(i, j int) {
	vp[i], vp[j] = vp[j], vp[i]
}

func (vp virtualPoints) Sort() {
	sort.Sort(vp)
}

type ketamaHashing struct {
	servers       []*ShardServer
	isWeighted    bool
	virtualPoints virtualPoints
}

func (k *ketamaHashing) GetServers() []*ShardServer {
	return k.servers
}

func (k *ketamaHashing) GetShardServer(key string) (index int, server *ShardServer) {
	index = k.GetShardIndex(key)
	server = k.servers[index]
	return
}

func (k *ketamaHashing) GetShardIndex(key string) int {
	if k.servers == nil || len(k.servers) == 0 {
		return -1
	}

	if len(k.servers) == 1 {
		return 0
	}

	h := ketamaHash(key, 0)

	i := sort.Search(
		len(k.virtualPoints),
		func(i int) bool {
			return k.virtualPoints[i].pointer >= h
		},
	)

	if i >= len(k.virtualPoints) {
		i = 0
	}

	return k.virtualPoints[i].shardId
}

// NewKetamaSharding return ketama sharding algorithm implementation
//
// When trimDefaultPort is set to true and defaultPort is specified,
// the server address with defaultPort will be trimmed,
// for example: if defaultPort = 11211, 192.168.1.101:11211 will be trimmed to 192.168.1.101
func NewKetamaSharding(shardServers []*ShardServer, trimDefaultPort bool, defaultPort int) Sharding {
	serverCount := len(shardServers)
	totalWeight := int64(0)
	for _, h := range shardServers {
		totalWeight += h.Weight
	}
	isWeighted := totalWeight > 0

	points := make(virtualPoints, 0)
	pointerPerServer := ketamaPointsPerServer
	pointerPerHash := ketamaPointsPerHash
	for index, h := range shardServers {
		if isWeighted {
			pct := float64(h.Weight) / float64(totalWeight)
			pointerPerServer = int(math.Floor(pct*ketamaPointsPerServer/4*float64(serverCount)+0.0000000001) * 4)
			pointerPerHash = ketamaPointsPerHash
		}
		hashCount := pointerPerServer / pointerPerHash

		address := h.Address
		if trimDefaultPort {
			// 默认端口不出现在hash的input中
			strings.Replace(h.Address, fmt.Sprintf(":%d", defaultPort), "", -1)
		}

		for hi := 0; hi < hashCount; hi++ {
			key := fmt.Sprintf("%s-%d", address, hi)
			for i := 0; i < pointerPerHash; i++ {
				points = append(
					points,
					virtualPoint{
						shardId: index,
						pointer: ketamaHash(key, i),
					},
				)
			}
		}
	}

	points.Sort()

	return &ketamaHashing{
		servers:       shardServers,
		virtualPoints: points,
		isWeighted:    isWeighted,
	}
}
