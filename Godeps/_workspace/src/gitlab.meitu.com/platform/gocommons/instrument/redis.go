package instrument

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type instrumentRedisConn struct {
	options *Options
	conn    redis.Conn
}

// NewRedisPool wraps specified redis.Pool instance with instrument code
func NewRedisPool(pool *redis.Pool, opts *Options) *redis.Pool {
	if opts == nil {
		opts = defaultOptions
	} else {
		if opts.LogVerbose == nil {
			opts.LogVerbose = defaultOptions.LogVerbose
		}
		if opts.ObserveLatency == nil {
			opts.ObserveLatency = defaultOptions.ObserveLatency
		}
	}

	originalDial := pool.Dial
	pool.Dial = func() (redis.Conn, error) {
		conn, err := originalDial()
		if err == nil {
			return instrumentRedisConn{
				conn:    conn,
				options: opts,
			}, err
		}

		return conn, err
	}
	return pool
}

func (i instrumentRedisConn) Close() error {
	return i.conn.Close()
}

func (i instrumentRedisConn) Err() error {
	return i.conn.Err()
}

func (i instrumentRedisConn) instrument(method string, command string, startedAt time.Time, args ...interface{}) {
	if command == "" {
		return
	}
	latency := time.Since(startedAt)
	i.options.ObserveLatency(latency)
	i.options.LogVerbose(i.options.Tag, method, command, args, "| latency:", int64(latency)/int64(time.Millisecond), "ms")
}

func (i instrumentRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	defer i.instrument("Redis.Do:", commandName, time.Now(), args)
	return i.conn.Do(commandName, args...)
}

func (i instrumentRedisConn) Send(commandName string, args ...interface{}) error {
	defer i.instrument("Redis.Send:", commandName, time.Now(), args)
	return i.conn.Send(commandName, args...)
}

func (i instrumentRedisConn) Flush() error {
	return i.conn.Flush()
}

func (i instrumentRedisConn) Receive() (reply interface{}, err error) {
	return i.conn.Receive()
}
