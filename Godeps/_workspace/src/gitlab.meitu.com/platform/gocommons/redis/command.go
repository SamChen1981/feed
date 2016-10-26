package redis

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

const (
	okResponse      = "OK"
	timeoutRetryNum = 3
	badConnRetryNum = 3
)

func (r *Cluster) getConn(node *redisNode) redis.Conn {
	conn := node.Get()
	var connRetryCount int
	for conn.Err() != nil && connRetryCount < badConnRetryNum {
		conn = node.Get()
		connRetryCount++
	}

	return conn
}

func (r *Cluster) doRead(cli *client, command string, args ...interface{}) (reply interface{}, err error) {
	if cli.slave != nil {
		return r.executeCommand(cli.slave, command, args...)
	}

	return r.executeCommand(cli.master, command, args...)
}

func (r *Cluster) doWrite(cli *client, command string, args ...interface{}) (reply interface{}, err error) {
	return r.executeCommand(cli.master, command, args...)
}

var nodeDownErrorFormat string = "redis node %v is dead at the moment!"

func (r *Cluster) executeCommand(node *redisNode, command string, args ...interface{}) (reply interface{}, err error) {
	if !node.isAlive() {
		return nil, fmt.Errorf(nodeDownErrorFormat, node.server)
	}

	conn := r.getConn(node)
	defer conn.Close()

	if conn.Err() != nil {
		if node.markFail() {
			return nil, fmt.Errorf(nodeDownErrorFormat, node.server)
		} else {
			return nil, fmt.Errorf("redis get connection on node %v failed: %v", node.server, conn.Err())
		}
	}

	var retryCount int
	reply, err = conn.Do(command, args...)
	// retry if needed
	for err != nil && retryCount < timeoutRetryNum {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			reply, err = conn.Do(command, args...)
			retryCount++
		} else {
			break
		}
	}
	// check error
	if err != nil {
		// only mark on network error
		if _, ok := err.(net.Error); ok {
			if node.markFail() {
				return nil, fmt.Errorf(nodeDownErrorFormat, node.server)
			}
		}
	}

	node.setAlive()
	return
}

/**
MGet returns the values of all specified keys.
For every key that does not hold a string value or does not exist, the special value nil is returned.
Because of this, the operation never fails.
*/
func (r *Cluster) MGet(keys ...string) ([]interface{}, error) {
	hashMap := make(map[*client][]string)
	for _, key := range keys {
		hashMap[r.getClient(key)] = append(hashMap[r.getClient(key)], key)
	}

	tmpResult := make(map[string]interface{})
	errString := ""

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for cliVal, keysVal := range hashMap {
		wg.Add(1)
		go func(cli *client, keys []string) {
			defer wg.Done()

			args := make([]interface{}, 0, len(keys))
			for _, key := range keys {
				args = append(args, key)
			}

			vs, err := redis.Values(r.doRead(cli, "MGET", args...))

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errString += (err.Error() + "; ")
				return
			}

			for i, key := range keys {
				tmpResult[key] = vs[i]
			}

		}(cliVal, keysVal)
	}

	wg.Wait()

	result := make([]interface{}, len(keys), len(keys))
	for index, key := range keys {
		result[index] = tmpResult[key]
	}

	var mgetErr error = nil
	if len(errString) > 0 {
		mgetErr = redis.Error(errString)
	}

	return result, mgetErr
}

func (r *Cluster) Exists(key string) (bool, error) {
	return redis.Bool(r.doRead(r.getClient(key), "EXISTS", key))
}

func (r *Cluster) Get(key string) (interface{}, error) {
	return r.doRead(r.getClient(key), "GET", key)
}

func (r *Cluster) GetSet(key string, args ...interface{}) (interface{}, error) {
	getSetArgs := append([]interface{}{key}, args...)
	return r.doWrite(r.getClient(key), "GETSET", getSetArgs...)
}

func (r *Cluster) set(key string, args ...interface{}) (bool, error) {
	setArgs := append([]interface{}{key}, args...)
	result, err := redis.String(r.doWrite(r.getClient(key), "SET", setArgs...))
	return result == okResponse, err
}

func (r *Cluster) Set(key string, val interface{}) (bool, error) {
	return r.set(key, val)
}

func (r *Cluster) SetWithExpires(key string, value interface{}, expiresInSeconds int64) (bool, error) {
	return r.set(key, value, "EX", expiresInSeconds)
}

func (r *Cluster) SetIfNotExists(key string, value interface{}) (bool, error) {
	return r.set(key, value, "NX")
}

func (r *Cluster) SetIfExists(key string, value interface{}) (bool, error) {
	return r.set(key, value, "XX")
}

func (r *Cluster) Expire(key string, ttlSeconds int64) (bool, error) {
	return redis.Bool(r.doWrite(r.getClient(key), "EXPIRE", key, ttlSeconds))
}

func (r *Cluster) ExpireAt(key string, timestamp int64) (bool, error) {
	return redis.Bool(r.doWrite(r.getClient(key), "EXPIREAT", key, timestamp))
}

func (r *Cluster) Incr(key string, value int) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "INCR", key))
}

func (r *Cluster) IncrBy(key string, value int) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "INCRBY", key, value))
}

func (r *Cluster) Del(keys ...string) (int, error) {
	hashMap := make(map[*client][]string)
	for _, key := range keys {
		hashMap[r.getClient(key)] = append(hashMap[r.getClient(key)], key)
	}

	var (
		result    uint32
		errString string
	)

	var wg sync.WaitGroup
	for cliVal, keysVal := range hashMap {
		wg.Add(1)
		go func(cli *client, keys []string) {
			defer wg.Done()
			args := make([]interface{}, 0, len(keys))
			for _, key := range keys {
				args = append(args, key)
			}
			n, err := redis.Int(r.doWrite(cli, "DEL", args...))
			if err != nil {
				errString += (err.Error() + "; ")
			}
			atomic.AddUint32(&result, uint32(n))
		}(cliVal, keysVal)
	}

	wg.Wait()

	var delErr error = nil
	if len(errString) > 0 {
		delErr = redis.Error(errString)
	}

	return int(result), delErr
}

func (r *Cluster) HIncrBy(key string, field interface{}, value int) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "HINCRBY", key, field, value))
}

func (r *Cluster) HGet(key string, field interface{}) (interface{}, error) {
	return r.doRead(r.getClient(key), "HGET", key, field)
}

func (r *Cluster) HSet(key string, field, value interface{}) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "HSET", key, field, value))
}

func (r *Cluster) HDel(key string, fields ...interface{}) (int, error) {
	args := append([]interface{}{key}, fields...)
	return redis.Int(r.doWrite(r.getClient(key), "HDEL", args...))
}

func (r *Cluster) HMSet(key string, kvs ...interface{}) (bool, error) {
	args := append([]interface{}{key}, kvs...)
	result, err := redis.String(r.doWrite(r.getClient(key), "HMSET", args...))
	return result == okResponse, err
}

func (r *Cluster) HMGet(key string, fields ...interface{}) (interface{}, error) {
	args := append([]interface{}{key}, fields...)
	return r.doRead(r.getClient(key), "HMGET", args...)
}

func (r *Cluster) HGetAll(key string) (interface{}, error) {
	return r.doRead(r.getClient(key), "HGETALL", key)
}

func (r *Cluster) SAdd(key string, members ...interface{}) (int, error) {
	args := append([]interface{}{key}, members...)
	return redis.Int(r.doWrite(r.getClient(key), "SADD", args...))
}

func (r *Cluster) SMembers(key string) (interface{}, error) {
	return r.doRead(r.getClient(key), "SMEMBERS", key)
}

func (r *Cluster) SRem(key string, members ...interface{}) (int, error) {
	args := append([]interface{}{key}, members...)
	return redis.Int(r.doWrite(r.getClient(key), "SREM", args...))
}

func (r *Cluster) ZAdd(key string, scoremembers ...interface{}) (int, error) {
	if len(scoremembers)%2 != 0 {
		return 0, fmt.Errorf("zadd for %v expects even number of score members", key)
	}
	args := append([]interface{}{key}, scoremembers...)
	return redis.Int(r.doWrite(r.getClient(key), "ZADD", args...))
}

func (r *Cluster) ZRem(key string, members ...interface{}) (int, error) {
	args := append([]interface{}{key}, members...)
	return redis.Int(r.doWrite(r.getClient(key), "ZREM", args...))
}

func (r *Cluster) ZCount(key string, min, max interface{}) (int, error) {
	return redis.Int(r.doRead(r.getClient(key), "ZCOUNT", key, min, max))
}

func (r *Cluster) ZRangeByScore(key string, min, max interface{}) (interface{}, error) {
	return r.doRead(r.getClient(key), "ZRANGEBYSCORE", key, min, max)
}

func (r *Cluster) ZRangeByScoreWithScores(key string, min, max interface{}) (interface{}, error) {
	return r.doRead(r.getClient(key), "ZRANGEBYSCORE", key, min, max, "WITHSCORES")
}

func (r *Cluster) ZRangeByScoreWithLimit(key string, min, max interface{}, offset, count int) (interface{}, error) {
	return r.doRead(r.getClient(key), "ZRANGEBYSCORE", key, min, max, "LIMIT", offset, count)
}

func (r *Cluster) ZRevRangeByScoreWithLimit(key string, max, min interface{}, offset, count int) (interface{}, error) {
	return r.doRead(r.getClient(key), "ZREVRANGEBYSCORE", key, max, min, "limit", offset, count)
}

func (r *Cluster) ZRemRangeByScore(key string, min, max interface{}) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "ZREMRANGEBYSCORE", key, min, max))
}

func (r *Cluster) ZRemRangeByRank(key string, start, stop int) (int, error) {
	return redis.Int(r.doWrite(r.getClient(key), "ZREMRANGEBYRANK", key, start, stop))
}

func (r *Cluster) ZRange(key string, start, stop int) (interface{}, error) {
	return r.doRead(r.getClient(key), "ZRANGE", key, start, stop)
}

type Command struct {
	Name string
	Args []interface{}
}

// Exec executes a series of commands, aka pipeline as a single commit. We only support
// commands that "operate on the same key" as a pipeline. Exec fetches a connection from
// master pool to do the whole thing. Caution: result of operations on different keys is
// not predictable.
func (r *Cluster) Exec(key string, commands ...Command) (bool, error) {
	cli := r.getClient(key)
	node := cli.master

	conn := r.getConn(node)
	defer conn.Close()
	if conn.Err() != nil {
		if node.markFail() {
			return false, fmt.Errorf(nodeDownErrorFormat, node.server)
		} else {
			return false, fmt.Errorf("redis exec pipeline for key %v get connection failed: %v", key, conn.Err())
		}
	}

	for _, command := range commands {
		var retryCount int
		err := conn.Send(command.Name, command.Args...)
		// retry if needed
		for err != nil && retryCount < timeoutRetryNum {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				err = conn.Send(command.Name, command.Args...)
				retryCount++
			} else {
				break
			}
		}
		// check error
		if err != nil {
			// only mark on network error
			if _, ok := err.(net.Error); ok {
				if node.markFail() {
					return false, fmt.Errorf(nodeDownErrorFormat, node.server)
				}
			}

			return false, fmt.Errorf("redis send command %v %v failed after retry %v times: %v",
				command.Name, command.Args, retryCount, err)
		}
	}

	var retryCount int
	_, err := conn.Do("")
	// retry if needed
	for err != nil && retryCount < timeoutRetryNum {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			_, err = conn.Do("")
			retryCount++
		} else {
			break
		}
	}
	// check error
	if err != nil {
		// only mark fail on network error
		if _, ok := err.(net.Error); ok {
			if node.markFail() {
				return false, fmt.Errorf(nodeDownErrorFormat, node.server)
			}
		}

		return false, fmt.Errorf("redis flush pipeline commands for key %v failed after retry %v times: %v",
			key, retryCount, err)
	}

	node.setAlive()
	return true, nil
}

type CommandExecutor func(conn redis.Conn)

func (r *Cluster) Do(key string, commands ...CommandExecutor) {
	cli := r.getClient(key)
	node := cli.master

	conn := r.getConn(node)
	defer conn.Close()

	if conn.Err() != nil {
		if node.markFail() && r.logger != nil {
			r.logger.Errorf(nodeDownErrorFormat, node.server)
		}
		if r.logger != nil {
			r.logger.Errorf("redis get connection for key %v failed: %v", key, conn.Err())
		}
		return
	}

	for _, command := range commands {
		command(conn)
	}
}
