package instrument

import (
	"time"

	"github.com/dropbox/godropbox/memcache"
)

type instrumentMemcache struct {
	options *Options
	client  memcache.Client
}

// NewMemcacheClient wraps wraps specified memcache.Client instance with instrument code
func NewMemcacheClient(client memcache.Client, opts *Options) memcache.Client {
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

	return instrumentMemcache{
		options: opts,
		client:  client,
	}
}

func (i instrumentMemcache) instrument(operation string, startedAt time.Time, keys ...string) {
	latency := time.Since(startedAt)
	i.options.ObserveLatency(latency)
	i.options.LogVerbose(i.options.Tag, operation, keys, "| latency:", int64(latency)/int64(time.Millisecond), "ms")
}

func (i instrumentMemcache) Get(key string) memcache.GetResponse {
	defer i.instrument("Memcache.Get:", time.Now(), key)
	return i.client.Get(key)
}

func (i instrumentMemcache) GetMulti(keys []string) map[string]memcache.GetResponse {
	defer i.instrument("Memcache.GetMulti:", time.Now(), keys...)
	return i.client.GetMulti(keys)
}

func (i instrumentMemcache) getKeysFromItems(items []*memcache.Item) []string {
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = item.Key
	}
	return keys
}

func (i instrumentMemcache) Set(item *memcache.Item) memcache.MutateResponse {
	defer i.instrument("Memcache.Set:", time.Now(), item.Key)
	return i.client.Set(item)
}

func (i instrumentMemcache) SetMulti(items []*memcache.Item) []memcache.MutateResponse {
	defer i.instrument("Memcache.SetMulti:", time.Now(), i.getKeysFromItems(items)...)
	return i.client.SetMulti(items)
}

func (i instrumentMemcache) SetSentinels(items []*memcache.Item) []memcache.MutateResponse {
	return i.client.SetSentinels(items)
}

func (i instrumentMemcache) Add(item *memcache.Item) memcache.MutateResponse {
	defer i.instrument("Memcache.Add:", time.Now(), item.Key)
	return i.client.Add(item)
}

func (i instrumentMemcache) AddMulti(items []*memcache.Item) []memcache.MutateResponse {
	defer i.instrument("Memcache.AddMulti:", time.Now(), i.getKeysFromItems(items)...)
	return i.client.AddMulti(items)
}

func (i instrumentMemcache) Replace(item *memcache.Item) memcache.MutateResponse {
	defer i.instrument("Memcache.Replace:", time.Now(), item.Key)
	return i.client.Replace(item)
}

func (i instrumentMemcache) Delete(key string) memcache.MutateResponse {
	defer i.instrument("Memcache.Delete:", time.Now(), key)
	return i.client.Delete(key)
}

func (i instrumentMemcache) DeleteMulti(keys []string) []memcache.MutateResponse {
	defer i.instrument("Memcache.DeleteMulti:", time.Now(), keys...)
	return i.client.DeleteMulti(keys)
}

func (i instrumentMemcache) Append(key string, value []byte) memcache.MutateResponse {
	defer i.instrument("Memcache.Append:", time.Now(), key)
	return i.client.Append(key, value)
}

func (i instrumentMemcache) Prepend(key string, value []byte) memcache.MutateResponse {
	defer i.instrument("Memcache.Prepend:", time.Now(), key)
	return i.client.Prepend(key, value)
}

func (i instrumentMemcache) Increment(key string, delta uint64, initValue uint64, expiration uint32) memcache.CountResponse {
	defer i.instrument("Memcache.Increment:", time.Now(), key)
	return i.client.Increment(key, delta, initValue, expiration)
}

func (i instrumentMemcache) Decrement(key string, delta uint64, initValue uint64, expiration uint32) memcache.CountResponse {
	defer i.instrument("Memcache.Decrement:", time.Now(), key)
	return i.client.Decrement(key, delta, initValue, expiration)
}

func (i instrumentMemcache) Flush(expiration uint32) memcache.Response {
	return i.client.Flush(expiration)
}

func (i instrumentMemcache) Stat(statsKey string) memcache.StatResponse {
	return i.client.Stat(statsKey)
}

func (i instrumentMemcache) Version() memcache.VersionResponse {
	return i.client.Version()
}

func (i instrumentMemcache) Verbosity(verbosity uint32) memcache.Response {
	return i.client.Verbosity(verbosity)
}
