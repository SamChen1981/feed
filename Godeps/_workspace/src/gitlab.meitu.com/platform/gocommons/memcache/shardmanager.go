// Package memcache contains memcache shard manager implementation with rehash and fail fast feature,
// support connection timeout option.
package memcache

import (
	"net"
	"sync"
	"time"

	"github.com/dropbox/godropbox/container/set"
	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/memcache"
	"github.com/dropbox/godropbox/net2"
	"gitlab.meitu.com/platform/gocommons/network"
	"gitlab.meitu.com/platform/gocommons/sharding"
)

type Hashing int

const (
	Compat Hashing = 1 + iota
	Ketama
)

const MaxRetryDelay int64 = 2 * 60 // 2 minute
const tcpProtocol string = "tcp"

// NewMemcachedClient return new memcache.Client instance
func NewMemcachedClient(options Options) memcache.Client {
	if options.ShardServers == nil || len(options.ShardServers) == 0 {
		return nil
	}

	var newSharding func(servers []*sharding.ShardServer) sharding.Sharding
	switch options.KeyHashing {
	case Compat:
		newSharding = sharding.NewCompatSharding
		break
	case Ketama:
		newSharding = func(servers []*sharding.ShardServer) sharding.Sharding {
			return sharding.NewKetamaSharding(servers, true, 11211)
		}
		break
	default:
		errors.New("key hashing algorithm not supported!")
		break
	}

	shardManager := &MeituShardManager{}
	shardManager.init(newSharding, options)

	return memcache.NewShardedClient(shardManager, options.UseAsciiProtocol)
}

type Options struct {

	// Protocol
	UseAsciiProtocol bool

	// Key hashing algorithm
	KeyHashing Hashing

	// Shard servers
	ShardServers []*sharding.ShardServer

	// The maximum number of connections that can be active per host at any
	// given time (A non-positive value indicates the number of connections
	// is unbounded).
	MaxActiveConnections int32

	// The maximum number of idle connections per host that are kept alive by
	// the connection pool.
	MaxIdleConnections uint32

	// The maximum amount of time an idle connection can alive (if specified).
	MaxIdleTime *time.Duration

	// The maximum amount of time a get connection request can wait for (if specified).
	// Zero value means no wait
	MaxWaitTime *time.Duration

	// This specifies the timeout for establish a connection to the server.
	ConnectionTimeout time.Duration

	// This specifies the timeout for any Read() operation.
	ReadTimeout time.Duration

	// This specifies the timeout for any Write() operation.
	WriteTimeout time.Duration

	// logError specifies function for logging error
	LogError func(err error)

	// logError specifies function for logging info
	LogInfo func(v ...interface{})
}

// MeituShardManager is a custom ShardManager with rehash feature added.
type MeituShardManager struct {
	newSharding func(servers []*sharding.ShardServer) sharding.Sharding
	sharding    sharding.Sharding
	pool        net2.ConnectionPool

	rwMutex      sync.RWMutex
	shardServers []*sharding.ShardServer // guarded by rwMutex
	shardIdMap   map[string]int          // guarded by rwMutex

	downMutex    sync.Mutex
	downServers  map[string]int64 // down server map, guarded by downMutex
	downDuration map[string]int64 // server down duration, guarded by downMutex

	logError func(err error)
	logInfo  func(v ...interface{})
}

// Initializes the MeituShardManager.
func (m *MeituShardManager) init(newSharding func(servers []*sharding.ShardServer) sharding.Sharding, options Options) {
	m.rwMutex = sync.RWMutex{}
	m.newSharding = newSharding
	m.pool = m.newConnectionPool(options)
	m.updateShardStates(options.ShardServers)

	m.downMutex = sync.Mutex{}
	m.downServers = make(map[string]int64, 0)
	m.downDuration = make(map[string]int64, 0)

	m.logError = options.LogError
	m.logInfo = options.LogInfo
}

func (m *MeituShardManager) newConnectionPool(options Options) net2.ConnectionPool {

	dial := func(network string, address string) (net.Conn, error) {
		return net.DialTimeout(network, address, options.ConnectionTimeout)
	}

	openFunc := func(loc string) (interface{}, error) {
		network, address := network.ParseResourceLocation(loc)
		if m.isServerDown(loc) {
			return nil, errors.New("server " + address + " is down now")
		}

		conn, err := dial(network, address)
		if err != nil {
			m.markServerDown(loc)
		}

		return conn, err
	}

	closeFunc := func(handle interface{}) error {
		return handle.(net.Conn).Close()
	}

	netOptions := network.Options{
		MaxActiveConnections: options.MaxActiveConnections,
		MaxIdleConnections:   options.MaxIdleConnections,
		MaxIdleTime:          options.MaxIdleTime,
		MaxWaitTime:          options.MaxWaitTime,
		ConnectionTimeout:    options.ConnectionTimeout,
		DialFunc:             dial,
		OpenFunc:             openFunc,
		CloseFunc:            closeFunc,
	}

	return network.NewMultiConnectionPool(netOptions)
}

// This updates the shard manager to use new shard states.
func (m *MeituShardManager) updateShardStates(shardServers []*sharding.ShardServer) {
	newAddrs := set.NewSet()
	for _, server := range shardServers {
		newAddrs.Add(server.Address)
	}

	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	oldAddrs := set.NewSet()
	for _, server := range m.shardServers {
		oldAddrs.Add(server.Address)
	}

	for address := range set.Subtract(newAddrs, oldAddrs).Iter() {
		if err := m.pool.Register(tcpProtocol, address.(string)); err != nil {
			m.logError(err)
		}
	}

	for address := range set.Subtract(oldAddrs, newAddrs).Iter() {
		if err := m.pool.Unregister(tcpProtocol, address.(string)); err != nil {
			m.logError(err)
		}
	}

	m.shardIdMap = make(map[string]int, 0)
	for index, server := range shardServers {
		m.shardIdMap[server.Address] = index
	}
	m.shardServers = shardServers
	m.sharding = m.newSharding(shardServers)
}

func (m *MeituShardManager) markServerDown(server string) {
	m.downMutex.Lock()
	defer m.downMutex.Unlock()

	m.downServers[server] = time.Now().Unix()
	duration := int64(1) // 1s
	if d, inMap := m.downDuration[server]; inMap {
		duration = d * 2
	}
	if duration > MaxRetryDelay {
		duration = MaxRetryDelay
	}
	m.downDuration[server] = duration
}

func (m *MeituShardManager) isServerDown(server string) bool {
	if downAt, inMap := m.downServers[server]; inMap {
		duration, inMap := m.downDuration[server]
		if inMap && downAt+duration < time.Now().Unix() {
			m.downMutex.Lock()
			defer m.downMutex.Unlock()

			delete(m.downDuration, server)
			delete(m.downServers, server)
			return false
		}

		return true
	}

	return false
}

// See ShardManager interface for documentation.
func (m *MeituShardManager) GetShard(key string) (shardId int, conn net2.ManagedConn, err error) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	shardId, shardServer := m.sharding.GetShardServer(key)
	servers := m.sharding.GetServers()
	serverCount := len(servers)
	isReshardAble := serverCount > 1
	var reshardServers []*sharding.ShardServer
	defer func() {
		reshardServers = nil
	}()

	for conn == nil {
		conn, err = m.pool.Get(tcpProtocol, shardServer.Address)
		if err == nil {
			shardId, _ = m.shardIdMap[shardServer.Address]
			break
		} else if isReshardAble {
			m.logError(err)
			conn = nil
			if reshardServers == nil {
				reshardServers = make([]*sharding.ShardServer, serverCount, serverCount)
				copy(reshardServers, servers)
			}

			if len(reshardServers) <= 1 {
				shardId = -1
				break
			}

			reshardServers = append(reshardServers[0:shardId], reshardServers[shardId+1:]...)
			shardId, shardServer = m.newSharding(reshardServers).GetShardServer(key)
		}
	}

	return
}

// See ShardManager interface for documentation.
func (m *MeituShardManager) GetShardsForKeys(keys []string) map[int]*memcache.ShardMapping {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	results := make(map[int]*memcache.ShardMapping)
	for _, key := range keys {
		shardId, _ := m.sharding.GetShardServer(key)

		entry, inMap := results[shardId]
		if !inMap {
			entry = &memcache.ShardMapping{}
			shardId, conn, err := m.GetShard(key)
			if shardId == -1 {
				continue
			} else if err != nil {
				m.logError(err)
				entry.ConnErr = err
			} else {
				entry.Connection = conn
			}
			entry.Keys = make([]string, 0, 1)
			results[shardId] = entry
		}
		entry.Keys = append(entry.Keys, key)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *MeituShardManager) GetShardsForItems(items []*memcache.Item) map[int]*memcache.ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	results := make(map[int]*memcache.ShardMapping)
	for _, item := range items {
		shardId, _ := m.sharding.GetShardServer(item.Key)
		entry, inMap := results[shardId]
		if !inMap {
			entry = &memcache.ShardMapping{}
			shardId, conn, err := m.GetShard(item.Key)
			if shardId == -1 {
				continue
			} else if err != nil {
				m.logError(err)
				entry.ConnErr = err
			} else {
				entry.Connection = conn
			}
			entry.Items = make([]*memcache.Item, 0, 1)
			results[shardId] = entry
		}
		entry.Items = append(entry.Items, item)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *MeituShardManager) GetShardsForSentinels(items []*memcache.Item) map[int]*memcache.ShardMapping {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	results := make(map[int]*memcache.ShardMapping)

	for _, item := range items {
		shardId, _ := m.sharding.GetShardServer(item.Key)
		entry, inMap := results[shardId]
		if !inMap {
			entry = &memcache.ShardMapping{}
			shardId, conn, err := m.GetShard(item.Key)
			if shardId == -1 {
				continue
			} else if err != nil {
				m.logError(err)
				entry.ConnErr = err
			} else {
				entry.Connection = conn
			}
			entry.Items = make([]*memcache.Item, 0, 1)
			results[shardId] = entry
		}
		entry.Items = append(entry.Items, item)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *MeituShardManager) GetAllShards() map[int]net2.ManagedConn {
	results := make(map[int]net2.ManagedConn)

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	for i, server := range m.shardServers {
		conn, err := m.pool.Get(tcpProtocol, server.Address)
		if err != nil {
			m.logError(err)
			conn = nil
		}
		results[i] = conn
	}

	return results
}
