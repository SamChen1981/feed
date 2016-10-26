package instrument

import (
	"database/sql"
	"database/sql/driver"
	"time"
)

type DB struct {
	innerDB *sql.DB
	options *Options
}

// NewDB wraps specified sql.DB instance with instrument code
func NewDB(db *sql.DB, opts *Options) *DB {
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
	return &DB{
		innerDB: db,
		options: opts,
	}
}

func (d *DB) instrument(method string, startedAt time.Time, query string, args ...interface{}) {
	latency := time.Since(startedAt)
	d.options.ObserveLatency(latency)
	d.options.LogVerbose(d.options.Tag, method, query, args, "| latency:", int64(latency)/int64(time.Millisecond), "ms")
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer d.instrument("DB.Exec:", time.Now(), query, args...)
	return d.innerDB.Exec(query, args...)
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	defer d.instrument("DB.Query:", time.Now(), query, args...)
	return d.innerDB.Query(query, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	defer d.instrument("DB.QueryRow:", time.Now(), query, args...)
	return d.innerDB.QueryRow(query, args...)
}

func (d *DB) Prepare(query string) (*Stmt, error) {
	defer d.instrument("DB.Prepare:", time.Now(), query)
	if stmt, err := d.innerDB.Prepare(query); err == nil {
		return newStmt(stmt, query, d), nil
	} else {
		return nil, err
	}
}

func (d *DB) Begin() (*Tx, error) {
	if tx, err := d.innerDB.Begin(); err == nil {
		return newTx(tx, d), nil
	} else {
		return nil, err
	}
}

func (d *DB) Close() error {
	return d.innerDB.Close()
}

func (d *DB) Driver() driver.Driver {
	return d.innerDB.Driver()
}

func (d *DB) Ping() error {
	return d.innerDB.Ping()
}

func (d *DB) SetMaxIdleConns(n int) {
	d.innerDB.SetMaxIdleConns(n)
}

func (d *DB) SetMaxOpenConns(n int) {
	d.innerDB.SetMaxOpenConns(n)
}

type Stmt struct {
	query string
	stmt  *sql.Stmt
	db    *DB
}

func newStmt(stmt *sql.Stmt, query string, db *DB) *Stmt {
	return &Stmt{
		stmt:  stmt,
		query: query,
		db:    db,
	}
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	defer s.db.instrument("Stmt.Exec:", time.Now(), s.query, args...)
	return s.stmt.Exec(args...)
}

func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	defer s.db.instrument("Stmt.Query:", time.Now(), s.query, args...)
	return s.stmt.Query(args...)
}

func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	defer s.db.instrument("Stmt.QueryRow:", time.Now(), s.query, args...)
	return s.stmt.QueryRow(args...)
}

func (s *Stmt) Close() error {
	return s.stmt.Close()
}

type Tx struct {
	tx *sql.Tx
	db *DB
}

func newTx(tx *sql.Tx, db *DB) *Tx {
	return &Tx{
		tx: tx,
		db: db,
	}
}

func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	defer t.db.instrument("Tx.Exec:", time.Now(), query, args...)
	return t.tx.Exec(query, args...)
}

func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	defer t.db.instrument("Tx.Query:", time.Now(), query, args...)
	return t.tx.Query(query, args...)
}

func (t *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	defer t.db.instrument("Tx.QueryRow:", time.Now(), query, args...)
	return t.tx.QueryRow(query, args...)
}

func (t *Tx) Prepare(query string) (*sql.Stmt, error) {
	defer t.db.instrument("Tx.Prepare:", time.Now(), query)
	return t.tx.Prepare(query)
}

func (t *Tx) Stmt(stmt *sql.Stmt) *Stmt {
	return newStmt(t.tx.Stmt(stmt), "", t.db)
}

func (t *Tx) Commit() error {
	defer t.db.instrument("Tx.Commit:", time.Now(), "Commit")
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	defer t.db.instrument("Tx.Rollback:", time.Now(), "Rollback")
	return t.tx.Rollback()
}
