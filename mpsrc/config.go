package mpsrc

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"feed/storage"
	"os"
	"strings"
	"time"
)

type TomlConfig struct {
	LogDir    string
	CpuNum    int
	DB        DBConfig        `toml:"mysql"`
	Redis     RedisConfig     `toml:"redis"`
	Admin     HttpConfig      `toml:admin`
	Http      HttpConfig      `toml:http`
	Memcached MemcachedConfig `toml:memcached`
	Kafka     KafkaConfig     `toml:kafka`
}

type HttpConfig struct {
	Host          string
	Port          int
	NeedAccessLog bool
}

type RedisConfig struct {
	Master         string
	Slaves         []string
	MaxConns       int
	ReadTimeout    time.Duration
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
	MaxIdle        int
	DB             int
	Auth           string
}

type DBConfig struct {
	Master         string
	Slaves         []string
	MaxIdle        int
	MaxConns       int
	ReadTimeout    time.Duration
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
	User           string
	Passwd         string
	DBName         string
}

type MemcachedConfig struct {
	ReplicaWritePolicy storage.WritePolicy
	SetBackMaster      bool
	SetBackExpiration  uint32
	ReplicaExpiration  uint32
	Master             string
	Slave              string
	Replicas           []string
}

type KafkaConfig struct {
	Addr    string
	ProAddr string
}

const (
	DEFAULT_MAINDIR = "/usr/local/feed"
	DEFAULT_LOGSDIR = "/www/feed/logs"
	DEFAULT_CONF    = "./conf/feed-for-test.toml"
)

func loadConfig(conf string) (*TomlConfig, error) {
	_, err := os.Stat(conf)
	if err != nil {
		return nil, err
	}

	var config TomlConfig
	if _, err := toml.DecodeFile(conf, &config); err != nil {
		return nil, err
	}

	(&config).setDefault()
	return &config, nil
}

func setRedisDefault(r *RedisConfig) {
	if r.ReadTimeout > 0 {
		r.ReadTimeout = r.ReadTimeout * time.Millisecond
	}
	if r.ConnectTimeout > 0 {
		r.ConnectTimeout = r.ConnectTimeout * time.Millisecond
	}
	if r.IdleTimeout > 0 {
		r.IdleTimeout = r.IdleTimeout * time.Millisecond
	}
	if r.DB < 0 {
		r.DB = 0
	}
}

func setMemchacheDefault() {
	//todo
}

func setKafkaDefault() {
	//todo 
}

func setDBDefault(db *DBConfig) {
	if db.ReadTimeout > 0 {
		db.ReadTimeout = db.ReadTimeout * time.Millisecond
	}
	if db.ConnectTimeout > 0 {
		db.ConnectTimeout = db.ConnectTimeout * time.Millisecond
	}
	if db.IdleTimeout > 0 {
		db.IdleTimeout = db.IdleTimeout * time.Millisecond
	}
}

func (c *TomlConfig) setDefault() {
	if c.LogDir == "" {
		c.LogDir = DEFAULT_LOGSDIR
	}
	if c.Admin.Host == "" {
		c.Admin.Host = "127.0.0.1"
	}
	if c.Admin.Port <= 0 {
		c.Admin.Port = 8899
	}
	if c.CpuNum <= 0 {
		c.CpuNum = 0
	}
	setRedisDefault(&c.Redis)
	setDBDefault(&c.DB)
}

func (r *RedisConfig) toString() string {
	var slaves string
	if r.Slaves == nil {
		slaves = ""
	} else {
		slaves = strings.Join(r.Slaves, ",")
	}
	return fmt.Sprintf("redis: {\nmaster: %s\nslaves: %s\nmaxconns: %d\n"+
		"readtimeout: %s\nconnecttimeout: %s\n}\n",
		r.Master, slaves, r.MaxConns,
		r.ReadTimeout, r.ConnectTimeout)
}

func (m *MemcachedConfig) toString() string {
	return ""
}

func (k *KafkaConfig) toString() string {
	return ""
}

func (h *HttpConfig) toString() string {
	return fmt.Sprintf("{host: %s, port: %d, needaccesslog: %t}",
		h.Host,
		h.Port,
		h.NeedAccessLog,
	)
}

func (db *DBConfig) toString() string {
	var dbSlaves string
	if db.Slaves == nil {
		dbSlaves = ""
	} else {
		dbSlaves = strings.Join(db.Slaves, ",")
	}
	return fmt.Sprintf("db: {\nmaster: %s\nslaves: %s\nmaxconns: %d\n"+
		"readtimeout: %s\nconnecttimeout: %s\nuser: %s\npasswd: %s\ndbname: %s\n}\n",
		db.Master, dbSlaves, db.MaxConns,
		db.ReadTimeout, db.ConnectTimeout, db.User, db.Passwd, db.DBName)
}

func (c *TomlConfig) toString() string {
	return fmt.Sprintf("logdir: %s\ncpunum:%d\nadmin:%s\n%s\n%s\n%s\n",
		c.LogDir, c.CpuNum,
		(&c.Admin).toString(),
		(&c.Redis).toString(),
		(&c.DB).toString())
}
