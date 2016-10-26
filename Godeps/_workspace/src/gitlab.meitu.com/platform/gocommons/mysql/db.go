package mysql

import (
	"time"

	"github.com/go-sql-driver/mysql"
	"gitlab.meitu.com/platform/gocommons/sql"
)

const DriverName string = "mysql"

func init() {
	sql.Register(DriverName, &mysql.MySQLDriver{})
}

var DefaultConnectionOptions = ConnectionOptions{
	ConnectionTimeout: 3 * time.Second,
	ReadTimeout:       3 * time.Second,
	WriteTimeout:      3 * time.Second,
	MaxWaitTime:       200 * time.Millisecond,
	MaxOpen:           32,
	MaxIdle:           32,
}

func Open(dsn string, opts ConnectionOptions) (DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if opts.ConnectionTimeout > 0 {
		cfg.Timeout = opts.ConnectionTimeout
	}
	if opts.ReadTimeout > 0 {
		cfg.ReadTimeout = opts.ReadTimeout
	}
	if opts.WriteTimeout > 0 {
		cfg.WriteTimeout = opts.WriteTimeout
	}

	db, err := sql.Open(DriverName, cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if opts.MaxWaitTime > 0 {
		db.SetConnWaitTimeout(opts.MaxWaitTime)
	}
	if opts.MaxIdle > 0 {
		db.SetMaxIdleConns(opts.MaxIdle)
	}
	if opts.MaxOpen > 0 {
		db.SetMaxOpenConns(opts.MaxOpen)
	}
	if opts.MaxLifeTime > 0 {
		db.SetConnMaxLifetime(opts.MaxLifeTime)
	}
	return db, nil
}
