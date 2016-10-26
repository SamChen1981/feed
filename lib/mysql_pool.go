package lib

import (
	"database/sql"
	"errors"
	"math/rand"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlOptions struct {
	MaxIdle        int
	MaxActive      int
	Retries        int
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
}

type MysqlPool struct {
	master    *sql.DB
	slaves    []*sql.DB
	mysqlOpts *MysqlOptions
}

func setDefaultMysqlOption(opts *MysqlOptions) *MysqlOptions {
	defaultTimeout := 3 * time.Second
	if opts == nil {
		opts = &MysqlOptions{
			MaxIdle:        64,
			MaxActive:      128,
			Retries:        0,
			ConnectTimeout: defaultTimeout,
			ReadTimeout:    defaultTimeout,
		}
	}
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = defaultTimeout
	}
	if opts.ReadTimeout <= 0 { // 如果没有写超时，就跟连接超时一致
		opts.ReadTimeout = opts.ConnectTimeout
	}
	if opts.MaxIdle <= 0 {
		opts.MaxIdle = 64
	}
	if opts.MaxActive <= 0 {
		opts.MaxActive = 128
	}
	return opts
}

func initMysqlPool(host, user, passwd, dbName string, opts *MysqlOptions) (*sql.DB, error) {
	config := &mysql.Config{
		User:        user,
		Passwd:      passwd,
		DBName:      dbName,
		Net:         "tcp",
		Addr:        host,
		Timeout:     opts.ConnectTimeout,
		ReadTimeout: opts.ReadTimeout,
	}
	dataSourceName := config.FormatDSN()
	db, err := sql.Open("mysql", dataSourceName)
	// db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/feed?charset=utf8")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(opts.MaxActive)
	db.SetMaxIdleConns(opts.MaxIdle)
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewMysqlPool(master string, slaves []string, user, passwd, dbName string, opts *MysqlOptions) (*MysqlPool, error) {
	if user == "" || passwd == "" || dbName == "" {
		return nil, errors.New("invalid user or passwd or dbname")
	}
	if master == "" {
		return nil, errors.New("invalid mtaser")
	}
	opts = setDefaultMysqlOption(opts)
	mysqlPool := new(MysqlPool)
	mysqlPool.mysqlOpts = opts
	masterDb, err := initMysqlPool(master, user, passwd, dbName, opts)
	if err != nil {
		return nil, err
	}
	mysqlPool.master = masterDb

	if slaves != nil && len(slaves) > 0 {
		mysqlPool.slaves = make([]*sql.DB, len(slaves))
	}
	for idx, slave := range slaves {
		slaveDb, err := initMysqlPool(slave, user, passwd, dbName, opts)
		if err != nil {
			return nil, err
		}
		mysqlPool.slaves[idx] = slaveDb
	}

	return mysqlPool, nil
}

func (mp *MysqlPool) GetClient(isMaster bool) *sql.DB {
	if isMaster {
		client := mp.master
		err := client.Ping()
		if err == nil {
			return client
		}
	}

	retries := mp.mysqlOpts.Retries + 1
	if mp.slaves == nil || len(mp.slaves) <= 1 {
		retries = 1
	} else if mp.slaves != nil && len(mp.slaves) < retries {
		retries = len(mp.slaves)
	}

	ind := 0
	if mp.slaves != nil && len(mp.slaves) >= 1 {
		ind = rand.Intn(len(mp.slaves))
	}
	i := 0
	for ; i < retries; i++ {
		err := mp.slaves[ind].Ping()
		if err == nil {
			return mp.slaves[ind]
		}
		ind++
		ind = ind % len(mp.slaves)
	}
	return nil
}