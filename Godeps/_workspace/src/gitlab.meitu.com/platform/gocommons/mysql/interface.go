package mysql

import (
	"time"

	"gitlab.meitu.com/platform/gocommons/sql"
)

type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
type Stmt interface {
	Exec(args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
	Close() error
}
type Tx interface {
	Execer
	Queryer
	Commit() error
	Rollback() error
}

type DB interface {
	Execer
	Queryer
	Prepare(query string) (*sql.Stmt, error)
	Begin() (*sql.Tx, error)
	Ping() error
	Close() error
}

type ConnectionOptions struct {
	// This specifies the timeout for establish a connection to the server.
	ConnectionTimeout time.Duration

	// This specifies the timeout for any Read() operation.
	ReadTimeout time.Duration

	// This specifies the timeout for any Write() operation.
	WriteTimeout time.Duration

	// The maximum number of connections that can be opened at any given time
	// (A non-positive value indicates the number of connections is unbounded).
	MaxOpen int

	// The maximum number of idle connections per host that are kept alive by
	// the connection pool.
	MaxIdle int

	// The maximum amount of time an connection can alive (if specified).
	MaxLifeTime time.Duration

	// The maximum amount of time a get connection request can wait for (if specified).
	// Zero value means no wait timeout
	MaxWaitTime time.Duration
}
