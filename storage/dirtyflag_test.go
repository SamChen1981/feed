package storage

import (
	"testing"

	"github.com/dropbox/godropbox/memcache"
	"golang.org/x/net/context"
)

func TestDirtyFlag(t *testing.T) {
	mcStorage := &DefaultProxy{
		PreferredStorage: NewMemcacheStorage(
			memcache.NewMockClient(), nil, nil,
			MemcacheStorageOptions{
				LogError: func(c context.Context, err error, v ...interface{}) {
					t.Errorf("%v %v", err, v)
				},
				LogInfo: func(c context.Context, v ...interface{}) {
					t.Log(v...)
				},
			},
		),
	}

	key := "test_dirty"
	item := &Item{}
	item.MarkDirty()
	item.Value = []byte(key)
	mcStorage.Set(nil, key, item)

	it := mcStorage.Get(nil, key)
	if !it.Dirty() {
		t.Errorf("expected: %v, got: %v", true, it.Dirty())
	}
}
