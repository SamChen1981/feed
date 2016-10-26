package lib

import (
	"math/rand"
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisOption struct {
	DB             int
	MaxIdle        int
	MaxActive      int
	Retries        int
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
	Auth           string
}

type RedisPool struct {
	master *redis.Pool
	slaves []*redis.Pool
	opts   *RedisOption
}

// 设置默认的选项
func setDefaultOption(opts *RedisOption) *RedisOption {
	defaultTimeout := 3 * time.Second
	if opts == nil {
		opts = &RedisOption{
			DB:             0,
			MaxIdle:        64,
			MaxActive:      128,
			Retries:        0,
			ConnectTimeout: defaultTimeout,
			ReadTimeout:    defaultTimeout,
			WriteTimeout:   defaultTimeout,
			IdleTimeout:    180 * time.Second,
		}
	}
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = defaultTimeout
	}
	if opts.ReadTimeout <= 0 { // 如果没有写超时，就跟连接超时一致
		opts.ReadTimeout = opts.ConnectTimeout
	}
	if opts.WriteTimeout <= 0 { // 如果写超时没设置，就跟读一致
		opts.WriteTimeout = opts.ReadTimeout
	}
	if opts.MaxIdle <= 0 {
		opts.MaxIdle = 64
	}
	if opts.MaxActive <= 0 {
		opts.MaxActive = 128
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 180 * time.Second
	}
	return opts
}

/*
* 初始化连接池
 */
func initRedisPool(host string, opts *RedisOption) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     opts.MaxIdle,
		MaxActive:   opts.MaxActive,
		IdleTimeout: opts.IdleTimeout,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", host, opts.ConnectTimeout, opts.ReadTimeout, opts.WriteTimeout)
			if err != nil {
				return nil, err
			}
			// 支持 auth 认证
			if opts.Auth != "" {
				c.Send("AUTH", opts.Auth)
			}
			c.Do("SELECT", opts.DB)
			return c, nil
		},
	}
}

/*
* 创建连接池, 如果有主库和从库都会创建一个连接池
 */
func NewRedisPool(master string, slaves []string, opts *RedisOption) *RedisPool {
	if master == "" {
		return nil
	}

	redisPool := new(RedisPool)
	opts = setDefaultOption(opts)
	redisPool.opts = opts
	redisPool.master = initRedisPool(master, opts)
	if slaves != nil && len(slaves) > 0 {
		redisPool.slaves = make([]*redis.Pool, len(slaves))
	}
	for idx, slave := range slaves {
		redisPool.slaves[idx] = initRedisPool(slave, opts)
	}
	return redisPool
}

/*
* 获取连接, 这里的策略是手动指定需要从 master 还是 slave 取连接
* index = -1 表示从主库获取
* 1. 主库只会有一个， 直接从这个池子获取连接
* 2. 从库可能有多个, 随机取一个连接池来获取连接
* 3. 如果没有从库连接池, 就从主库连接池获取连接
 */
func (rp *RedisPool) chooseRedisPool(index int) redis.Conn {
	var conn redis.Conn
	if index == -1 || rp.slaves == nil || len(rp.slaves) <= 0 {
		conn = rp.master.Get()
	} else {
		index := index % len(rp.slaves)
		conn = rp.slaves[index].Get()
	}

	if conn.Err() != nil {
		return nil
	}
	return conn
}

/*
* 从连接池获取连接池, 添加了重试的策略
* 这里不在操作的时候重试考虑到有些操作并不是幂等
 */
func (rp *RedisPool) GetClient(isMaster bool) redis.Conn {
	var conn redis.Conn

	if isMaster { // 主从连接池获取连接，获取不到不重试
		return rp.chooseRedisPool(-1)
	}

	retries := rp.opts.Retries + 1 // 加一是由于正常获取和重试次数叠加
	if rp.slaves == nil || len(rp.slaves) <= 1 {
		retries = 1
	} else if rp.slaves != nil && len(rp.slaves) < retries {
		retries = len(rp.slaves)
	}

	ind := -1
	if rp.slaves != nil && len(rp.slaves) > 0 {
		ind = rand.Intn(len(rp.slaves))
	}
	for i := 0; i < retries; i++ {
		conn = rp.chooseRedisPool(ind)
		if conn != nil {
			return conn
		}
		ind += 1 // chooseRedisPool内部取模, 这里不用管
	}
	return nil
}
