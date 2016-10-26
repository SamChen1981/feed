package mysql

import (
	"errors"
	"strings"
	"time"

	"gitlab.meitu.com/platform/gocommons/mysql/sqlparser"
	"gitlab.meitu.com/platform/gocommons/sql"
)

const masterComment string = "/*master*/"

var errMasterRequired error = errors.New("master is required")
var errDBNameRequired error = errors.New("db name is required")

type Proxy struct {
	dbName         string
	master         DB
	slave          DB
	observeLatency func(latency time.Duration)
	logVerbose     func(v ...interface{})
}

type ProxyOptions struct {
	DBName         string
	Master         DB
	Slave          DB
	ObserveLatency func(latency time.Duration)
	LogVerbose     func(v ...interface{})
}

func NewProxy(opts ProxyOptions) (*Proxy, error) {
	if opts.DBName == "" {
		return nil, errDBNameRequired
	}
	if opts.Master == nil {
		return nil, errMasterRequired
	}

	proxy := &Proxy{
		dbName: opts.DBName,
		master: opts.Master,
		slave:  opts.Slave,
	}

	if opts.ObserveLatency == nil {
		proxy.observeLatency = func(latency time.Duration) {}
	} else {
		proxy.observeLatency = opts.ObserveLatency
	}
	if opts.LogVerbose == nil {
		proxy.logVerbose = func(v ...interface{}) {}
	} else {
		proxy.logVerbose = opts.LogVerbose
	}

	return proxy, nil
}

func (p *Proxy) Exec(sql string, args ...interface{}) (sql.Result, error) {
	sql = strings.TrimRight(sql, ";")
	defer p.postExecution(time.Now(), sql, args...)
	return p.master.Exec(sql, args...)
}

func (p *Proxy) postExecution(startAt time.Time, query string, args ...interface{}) {
	latency := time.Since(startAt)
	p.observeLatency(latency)
	p.logVerbose("db: ", p.dbName, ", sql: \"", query, "\", args: ", args,
		", latency(ms): ", int64(latency)/int64(time.Millisecond))
}

func (p *Proxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	query = strings.TrimRight(query, ";")
	st, _ := sqlparser.Parse(query)

	defer p.postExecution(time.Now(), query, args...)

	if p.slave == nil || p.hasMasterComment(st) {
		return p.master.Query(query, args...)
	}
	return p.slave.Query(query, args...)
}

func (p *Proxy) QueryRow(query string, args ...interface{}) *sql.Row {
	query = strings.TrimRight(query, ";")
	st, _ := sqlparser.Parse(query)

	defer p.postExecution(time.Now(), query, args...)

	if p.slave == nil || p.hasMasterComment(st) {
		return p.master.QueryRow(query, args...)
	}
	return p.slave.QueryRow(query, args...)
}

func (p *Proxy) Prepare(query string) (Stmt, error) {
	query = strings.TrimRight(query, ";")
	st, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	var stmt *sql.Stmt
	switch st.(type) {
	case *sqlparser.Select:
		if p.slave == nil || p.hasMasterComment(st) {
			stmt, err = p.master.Prepare(query)
		} else {
			stmt, err = p.slave.Prepare(query)
		}

	//case *sqlparser.Insert:
	//	fallthrough
	//case *sqlparser.Update:
	//	fallthrough
	//case *sqlparser.Delete:
	//	fallthrough
	//case *sqlparser.DDL:
	//	fallthrough
	//case *sqlparser.Set:
	//	fallthrough
	//case *sqlparser.Other:
	//	fallthrough
	default:
		stmt, err = p.master.Prepare(query)
	}

	if err == nil {
		return &ProxyStmt{
			query: query,
			stmt:  stmt,
			proxy: p,
		}, nil
	}

	return nil, err
}

func (p *Proxy) hasMasterComment(st sqlparser.Statement) bool {
	switch st.(type) {
	case *sqlparser.Select:
		comments := st.(*sqlparser.Select).Comments
		if len(comments) > 0 {
			comment := string(comments[0])
			if 0 < len(comment) && strings.ToLower(comment) == masterComment {
				return true
			}
		}
	default:
		return false
	}
	return false
}

func (p *Proxy) Begin() (*ProxyTx, error) {
	defer p.postExecution(time.Now(), "START TRANSACTION")
	if tx, err := p.master.Begin(); err == nil {
		return &ProxyTx{
			tx:    tx,
			proxy: p,
		}, nil
	} else {
		return nil, err
	}
}

type ProxyStmt struct {
	query string
	stmt  *sql.Stmt
	proxy *Proxy
}

func (s *ProxyStmt) postExecution(startedAt time.Time, args ...interface{}) {
	latency := time.Since(startedAt)
	s.proxy.observeLatency(latency)
	s.proxy.logVerbose("exec within stmt, db: ", s.proxy.dbName, ", sql: \"", s.query, "\", args: ", args,
		", latency(ms): ", int64(latency)/int64(time.Millisecond))
}

func (s *ProxyStmt) Exec(args ...interface{}) (sql.Result, error) {
	defer s.postExecution(time.Now(), args...)
	return s.stmt.Exec(args...)
}

func (s *ProxyStmt) Query(args ...interface{}) (*sql.Rows, error) {
	defer s.postExecution(time.Now(), args...)
	return s.stmt.Query(args...)
}

func (s *ProxyStmt) QueryRow(args ...interface{}) *sql.Row {
	defer s.postExecution(time.Now(), args...)
	return s.stmt.QueryRow(args...)
}

func (s *ProxyStmt) Close() error {
	return s.stmt.Close()
}

type ProxyTx struct {
	tx    *sql.Tx
	proxy *Proxy
}

func (t *ProxyTx) postExecution(startedAt time.Time, query string, args ...interface{}) {
	latency := time.Since(startedAt)
	t.proxy.observeLatency(latency)
	t.proxy.logVerbose("exec within transaction, db: ", t.proxy.dbName, ", sql: \"", query, "\", args: ", args,
		", latency(ms): ", int64(latency)/int64(time.Millisecond))
}

func (t *ProxyTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer t.postExecution(time.Now(), query, args...)
	return t.tx.Exec(query, args...)
}

func (t *ProxyTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	defer t.postExecution(time.Now(), query, args...)
	return t.tx.Query(query, args...)
}

func (t *ProxyTx) QueryRow(query string, args ...interface{}) *sql.Row {
	defer t.postExecution(time.Now(), query, args...)
	return t.tx.QueryRow(query, args...)
}

func (t *ProxyTx) Prepare(query string) (*ProxyStmt, error) {
	stmt, err := t.tx.Prepare(query)
	if err == nil {
		return &ProxyStmt{
			query: query,
			stmt:  stmt,
			proxy: t.proxy,
		}, nil
	}

	return nil, err
}

func (t *ProxyTx) Stmt(stmt ProxyStmt) *ProxyStmt {
	return &ProxyStmt{
		query: stmt.query,
		stmt:  t.tx.Stmt(stmt.stmt),
		proxy: t.proxy,
	}
}

func (t *ProxyTx) Commit() error {
	defer t.postExecution(time.Now(), "COMMIT")
	return t.tx.Commit()
}

func (t *ProxyTx) Rollback() error {
	defer t.postExecution(time.Now(), "ROLLBACK")
	return t.tx.Rollback()
}
