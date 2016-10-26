package mpsrc

import (
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	var c *TomlConfig
	var err error

	_, err = loadConfig("/xxx/yyy")
	if err == nil {
		t.Error("Test loadConfig failed")
	}
	_, err = loadConfig("config.go")
	if err == nil {
		t.Error("Test loadConfig failed")
	}

	c, err = loadConfig("../conf/feed-for-test.toml")
	if err != nil {
		t.Error("Test loadConfig failed")
	}
	if c.LogDir != "/www/feed/logs" || c.CpuNum != 0 ||
		c.Admin.Port != 8899 || c.Admin.Host != "127.0.0.1" || !c.Admin.NeedAccessLog {
		t.Error("Test loadConfig failed")
	}
	if c.Redis.MaxConns != 100 || c.Redis.MaxIdle != 10 ||
		c.Redis.ReadTimeout != 3000*time.Millisecond ||
		c.Redis.ConnectTimeout != 3000*time.Millisecond ||
		c.Redis.Auth != "test_auth" ||
		c.Redis.IdleTimeout != 180*time.Second {
		t.Error("Test load redis config failed")
	}
}
