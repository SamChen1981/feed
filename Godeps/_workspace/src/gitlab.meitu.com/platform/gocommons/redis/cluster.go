package redis

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"gitlab.meitu.com/platform/gocommons/instrument"
	"gitlab.meitu.com/platform/gocommons/log"
	"gitlab.meitu.com/platform/gocommons/sharding"
)

type Cluster struct {
	sharding sharding.Sharding
	mutex    sync.RWMutex
	pool     []*client
	logger   log.Logger
}

type Options struct {
	ConnTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	MaxActives, MaxIdle                                 int
	Password                                            string
	Logger                                              log.Logger
}

func NewCluster(servers []string, options Options, instrumentOpts *instrument.Options) *Cluster {
	cluster := new(Cluster)
	cluster.pool = make([]*client, len(servers))

	masters := make([]string, 0, len(servers))
	for i, server := range servers {
		master, slave, db := parseServer(server)
		masters = append(masters, master)

		cli := new(client)
		cli.master = &redisNode{
			server: master,
			Pool: func() *redis.Pool {
				pool := &redis.Pool{
					MaxIdle:     options.MaxIdle,
					IdleTimeout: options.IdleTimeout,
					Dial: func() (redis.Conn, error) {
						c, err := redis.Dial(
							"tcp",
							master,
							redis.DialDatabase(db),
							redis.DialPassword(options.Password),
							redis.DialConnectTimeout(options.ConnTimeout),
							redis.DialReadTimeout(options.ReadTimeout),
							redis.DialWriteTimeout(options.WriteTimeout),
						)
						if err != nil {
							return nil, err
						}
						return c, err
					},
					MaxActive: options.MaxActives,
				}

				if instrumentOpts == nil {
					return pool
				}

				return instrument.NewRedisPool(pool, instrumentOpts)
			}(),
		}

		// allow nil slaves
		if slave != "" {
			cli.slave = &redisNode{
				server: slave,
				Pool: func() *redis.Pool {
					pool := &redis.Pool{
						MaxIdle:     options.MaxIdle,
						IdleTimeout: options.IdleTimeout,
						Dial: func() (redis.Conn, error) {
							c, err := redis.Dial(
								"tcp",
								slave,
								redis.DialDatabase(db),
								redis.DialPassword(options.Password),
								redis.DialConnectTimeout(options.ConnTimeout),
								redis.DialReadTimeout(options.ReadTimeout),
								redis.DialWriteTimeout(options.WriteTimeout),
							)
							if err != nil {
								return nil, err
							}
							return c, err
						},
						MaxActive: options.MaxActives,
					}

					if instrumentOpts == nil {
						return pool
					}

					return instrument.NewRedisPool(pool, instrumentOpts)
				}(),
			}
		}

		// call init
		cli.init()

		cluster.pool[i] = cli
	}

	cluster.sharding = sharding.NewKetamaSharding(sharding.GetShardServers(masters), true, 6379)

	cluster.logger = options.Logger

	return cluster
}

func (c *Cluster) getClient(key string) *client {
	index, _ := c.sharding.GetShardServer(key)
	return c.pool[index]
}

func parseServer(server string) (master, slave string, db int) {
	parts := strings.Split(server, ";")
	master, slave = parts[0], parts[1]
	db, _ = strconv.Atoi(parts[2])
	return
}
