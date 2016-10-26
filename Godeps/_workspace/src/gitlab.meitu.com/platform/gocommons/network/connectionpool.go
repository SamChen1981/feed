package network

import (
	"net"
	"strings"
	"time"

	"github.com/dropbox/godropbox/net2"
	rp "github.com/dropbox/godropbox/resource_pool"
)

const defaultDialTimeout = 1 * time.Second

type Options struct {

	// The maximum number of connections that can be active per host at any
	// given time (A non-positive value indicates the number of connections
	// is unbounded).
	MaxActiveConnections int32

	// The maximum amount of time a get connection request can wait for (if specified).
	// Zero value means no wait
	MaxWaitTime *time.Duration

	// The maximum number of idle connections per host that are kept alive by
	// the connection pool.
	MaxIdleConnections uint32

	// The maximum amount of time an idle connection can alive (if specified).
	MaxIdleTime *time.Duration

	// This specifies the timeout for establish a connection to the server.
	ConnectionTimeout time.Duration

	// This specifies the timeout for any Read() operation.
	ReadTimeout time.Duration

	// This specifies the timeout for any Write() operation.
	WriteTimeout time.Duration

	DialFunc func(network string, address string) (net.Conn, error)

	OpenFunc func(loc string) (interface{}, error)

	CloseFunc func(handle interface{}) error
}

func (o Options) getConnectionTimeout() time.Duration {
	if o.ConnectionTimeout <= 0 {
		return defaultDialTimeout
	} else {
		return o.ConnectionTimeout
	}
}

func (o Options) getResourceOptions() rp.Options {
	dial := o.DialFunc
	if dial == nil {
		dial = func(network string, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, o.getConnectionTimeout())
		}
	}

	openFunc := o.OpenFunc
	if openFunc == nil {
		openFunc = func(loc string) (interface{}, error) {
			network, address := ParseResourceLocation(loc)
			return dial(network, address)
		}
	}

	closeFunc := o.CloseFunc
	if closeFunc == nil {
		closeFunc = func(handle interface{}) error {
			return handle.(net.Conn).Close()
		}
	}

	return rp.Options{
		MaxActiveHandles: o.MaxActiveConnections,
		MaxIdleHandles:   o.MaxIdleConnections,
		MaxIdleTime:      o.MaxIdleTime,
		Open:             openFunc,
		Close:            closeFunc,
		NowFunc:          time.Now,
	}
}

func (o Options) getConnectionOptions() net2.ConnectionOptions {

	dial := o.DialFunc
	if dial == nil {
		dial = func(network string, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, o.getConnectionTimeout())
		}
	}

	return net2.ConnectionOptions{
		MaxActiveConnections: o.MaxActiveConnections,
		MaxIdleConnections:   o.MaxIdleConnections,
		MaxIdleTime:          o.MaxIdleTime,
		ReadTimeout:          o.ReadTimeout,
		WriteTimeout:         o.WriteTimeout,
		Dial:                 dial,
		NowFunc:              time.Now,
	}
}

func ParseResourceLocation(resourceLocation string) (
	network string,
	address string) {

	idx := strings.Index(resourceLocation, " ")
	if idx >= 0 {
		return resourceLocation[:idx], resourceLocation[idx+1:]
	}

	return "", resourceLocation
}

// A thin wrapper around the underlying resource pool.
type MeituConnectionPool struct {
	options net2.ConnectionOptions

	pool rp.ResourcePool
}

//// This returns a connection pool where all connections are connected
//// to the same (network, address)
func NewSimpleConnectionPool(options Options) net2.ConnectionPool {
	return &MeituConnectionPool{
		options: options.getConnectionOptions(),
		pool:    NewMeituResourcePool(options),
	}
}

// This returns a connection pool that manages multiple (network, address)
// entries.  The connections to each (network, address) entry acts
// independently. For example ("tcp", "localhost:11211") could act as memcache
// shard 0 and ("tcp", "localhost:11212") could act as memcache shard 1.
func NewMultiConnectionPool(options Options) net2.ConnectionPool {
	return &MeituConnectionPool{
		options: options.getConnectionOptions(),
		pool: rp.NewMultiResourcePool(options.getResourceOptions(),
			func(poolOptions rp.Options) rp.ResourcePool {
				return NewMeituResourcePool(options)
			},
		),
	}
}

// See ConnectionPool for documentation.
func (p *MeituConnectionPool) NumActive() int32 {
	return p.pool.NumActive()
}

// See ConnectionPool for documentation.
func (p *MeituConnectionPool) ActiveHighWaterMark() int32 {
	return p.pool.ActiveHighWaterMark()
}

// This returns the number of alive idle connections.  This method is not part
// of ConnectionPool's API.  It is used only for testing.
func (p *MeituConnectionPool) NumIdle() int {
	return p.pool.NumIdle()
}

// BaseConnectionPool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *MeituConnectionPool) Register(network string, address string) error {
	return p.pool.Register(network + " " + address)
}

// BaseConnectionPool has nothing to do on Unregister.
func (p *MeituConnectionPool) Unregister(network string, address string) error {
	return nil
}

func (p *MeituConnectionPool) ListRegistered() []net2.NetworkAddress {
	result := make([]net2.NetworkAddress, 0, 1)
	for _, location := range p.pool.ListRegistered() {
		network, address := ParseResourceLocation(location)

		result = append(
			result,
			net2.NetworkAddress{
				Network: network,
				Address: address,
			})
	}
	return result
}

// This gets an active connection from the connection pool.  Note that network
// and address arguments are ignored (The connections with point to the
// network/address provided by the first Register call).
func (p *MeituConnectionPool) Get(
	network string,
	address string) (net2.ManagedConn, error) {

	handle, err := p.pool.Get(network + " " + address)
	if err != nil {
		return nil, err
	}
	return net2.NewManagedConn(network, address, handle, p, p.options), nil
}

// See ConnectionPool for documentation.
func (p *MeituConnectionPool) Release(conn net2.ManagedConn) error {
	return conn.ReleaseConnection()
}

// See ConnectionPool for documentation.
func (p *MeituConnectionPool) Discard(conn net2.ManagedConn) error {
	return conn.DiscardConnection()
}

// See ConnectionPool for documentation.
func (p *MeituConnectionPool) EnterLameDuckMode() {
	p.pool.EnterLameDuckMode()
}
