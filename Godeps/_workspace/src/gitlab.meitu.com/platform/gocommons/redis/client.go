package redis

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Client consists of a master node and a slave node if necessary
type client struct {
	master, slave *redisNode
}

const (
	failCountThreshold = 20
	saveInterval       = time.Millisecond * 200
)

// init tries to save
func (c *client) init() {
	go func() {
		ticker := time.NewTicker(saveInterval)
		for _ = range ticker.C {
			if !c.master.isAlive() {
				result, err := redis.String(c.master.Get().Do("ping"))
				if err == nil && strings.ToLower(result) == "pong" {
					c.master.setAlive()
				}
			}
			if c.slave != nil && !c.slave.isAlive() {
				result, err := redis.String(c.slave.Get().Do("ping"))
				if err == nil && strings.ToLower(result) == "pong" {
					c.slave.setAlive()
				}
			}
		}
	}()
}

// RedisNode represents a master or a slave
type redisNode struct {
	server string
	*redis.Pool
	failCount uint32
}

func (n *redisNode) isAlive() bool {
	return atomic.LoadUint32(&n.failCount) < failCountThreshold
}

func (n *redisNode) setAlive() {
	atomic.StoreUint32(&n.failCount, 0)
}

func (n *redisNode) markFail() (dead bool) {
	return atomic.AddUint32(&n.failCount, 1) == failCountThreshold
}
