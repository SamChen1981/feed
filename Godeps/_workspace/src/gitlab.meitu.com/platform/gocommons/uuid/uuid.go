// Package uuid is used to get a 64bit uuid from redis-uuid server.
//
// uuid格式:
// 1位兼容位恒为0 + 41位时间信息（精确到毫秒）+ 6位IDC信息（64个IDC）+ 6位业务>信息（64个业务）+ 10位自增信息（>1000/秒）.
package uuid

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dropbox/godropbox/net2"
	"gitlab.meitu.com/platform/gocommons/network"
)

const tcp = "tcp"
const requestPattern = "GET %s\r\n"
const opError = "ERR"

type UUIDClient struct {
	pool        net2.ConnectionPool
	servers     []string
	serverCount int
}

type BufferReaderPool struct {
	pool sync.Pool
}

// Get returns a bufio.Reader which reads from r. The buffer size is that of the pool.
func (bufPool *BufferReaderPool) Get(r io.Reader) *bufio.Reader {
	buf := bufPool.pool.Get().(*bufio.Reader)
	buf.Reset(r)
	return buf
}

// Put puts the bufio.Reader back into the pool.
func (bufPool *BufferReaderPool) Put(b *bufio.Reader) {
	b.Reset(nil)
	bufPool.pool.Put(b)
}

// bufferReaderPool is a pool which returns bufio.Reader with a buffer.
var bufferReaderPool *BufferReaderPool

func init() {
	bufferReaderPool = newBufferReaderPoolWithSize(64)
}

func newBufferReaderPoolWithSize(size int) *BufferReaderPool {
	pool := sync.Pool{
		New: func() interface{} { return bufio.NewReaderSize(nil, size) },
	}
	return &BufferReaderPool{pool: pool}
}

// GetNewUUID get new uuid by key
func (u UUIDClient) GetNewUUID(key string) (int64, error) {
	server := u.servers[0]
	if u.serverCount > 1 {
		server = u.servers[int(crc32.ChecksumIEEE([]byte(key))>>16)%u.serverCount]
	}

	conn, err := u.pool.Get(tcp, server)
	if err != nil {
		return 0, err
	}

	defer conn.ReleaseConnection()
	if _, err := fmt.Fprintf(conn, requestPattern, key); err != nil {
		return 0, err
	}

	reader := bufferReaderPool.Get(conn)
	defer bufferReaderPool.Put(reader)

	// the first line will always be "VALUE uuid 0 19\r\n" when get ok
	// the first line will be "ERR <message>\r\n" when error occurred
	line, _, err := reader.ReadLine()
	if err != nil {
		return 0, err
	}

	slice := strings.Split(string(line), " ")
	if slice[0] == opError {
		return 0, errors.New(string(line))
	}

	size, _ := strconv.Atoi(slice[3])
	value, err := reader.Peek(size)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(value), 10, 64)
}

// NewUUIDClient returns new uuid client.
// The uuidServer parameter is in the format of host:port
func NewUUIDClient(uuidServers ...string) UUIDClient {
	maxIdle := time.Duration(120 * time.Second)
	maxWait := time.Duration(time.Millisecond * 200)
	pool := network.NewMultiConnectionPool(network.Options{
		MaxActiveConnections: 1024,
		MaxIdleConnections:   1024,
		MaxIdleTime:          &maxIdle,
		ConnectionTimeout:    time.Duration(1 * time.Second),
		ReadTimeout:          time.Duration(2 * time.Second),
		MaxWaitTime:          &maxWait,
	})

	for _, server := range uuidServers {
		pool.Register(tcp, server)
	}

	return UUIDClient{
		pool:        pool,
		servers:     uuidServers,
		serverCount: len(uuidServers),
	}
}
