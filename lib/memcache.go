package lib

import (
	"errors"
	mc "github.com/dropbox/godropbox/memcache"
	"gitlab.meitu.com/platform/gocommons/memcache"
	"gitlab.meitu.com/platform/gocommons/sharding"
	"feed/storage"
	"feed/storage/phpmemcached"
	"golang.org/x/net/context"
	"time"
)

type MemcachedStorageOpts struct {
	ReplicaWritePolicy storage.WritePolicy
	SetBackMaster      bool
	SetBackExpiration  uint32
	ReplicaExpiration  uint32
	LE                 func(c context.Context, err error, v ...interface{})
	LI                 func(c context.Context, v ...interface{})
}

//暂时采用默认配置
func NewMemcacheClient(serverlist string) mc.Client {
	servers := sharding.ParseServerString(serverlist)

	maxIdle := time.Minute * 3
	maxWait := time.Millisecond * 2000
	return memcache.NewMemcachedClient(
		memcache.Options{
			UseAsciiProtocol:     true, // 默认使用Binary协议
			KeyHashing:           memcache.Ketama,
			ShardServers:         servers,
			MaxActiveConnections: 4800,
			MaxIdleConnections:   64,
			MaxIdleTime:          &maxIdle,
			MaxWaitTime:          &maxWait,
			ConnectionTimeout:    time.Second,
			ReadTimeout:          2 * time.Second,
			WriteTimeout:         2 * time.Second,
			LogError: func(err error) {
				//
			},
			LogInfo: func(...interface{}) {
				//
			},
		})
}

func NewMemcachedStorage(mcMasterStr, mcSlaveStr string, mcReplicaStrs []string, opts *MemcachedStorageOpts) (*storage.MemcacheStorage, error) {

	if mcMasterStr == "" || mcSlaveStr == "" || mcReplicaStrs == nil {
		return nil, errors.New("invalid mcConfig")
	}
	mcMaster := NewMemcacheClient(mcMasterStr)
	mcSlave := NewMemcacheClient(mcSlaveStr)

	mcReplicas := make([]mc.Client, 0, len(mcReplicaStrs))
	for _, rp := range mcReplicaStrs {
		mcrp := NewMemcacheClient(rp)
		mcReplicas = append(mcReplicas, mcrp)
	}

	mcStorageOpts := storage.MemcacheStorageOptions{
		SetBackMaster:      opts.SetBackMaster,
		ReplicaWritePolicy: opts.ReplicaWritePolicy,
		SetBackExpiration:  opts.SetBackExpiration,
		ReplicaExpiration:  opts.ReplicaExpiration,
		LogError:           opts.LE,
		LogInfo:            opts.LI,
	}
	// 兼容 php-memcached 的压缩解压
	mcStorageOpts.Adaptor = phpmemcached.NewItemAdaptor(phpmemcached.NewOptions())
	mcStorage := storage.NewMemcacheStorage(
		mcMaster, mcSlave, mcReplicas,
		mcStorageOpts,
	)
	return mcStorage, nil
}
