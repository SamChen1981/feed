package storage

import (
	"sync"

	"golang.org/x/net/context"
)

// ReadThroughProcessor will be called when read through happens
type ReadThroughProcessor func(c context.Context, key string, item *Item, storage Storage)

// Proxy interface definition
type Proxy interface {
	GetMulti(c context.Context, keys ...string) map[string]*Item
	Set(c context.Context, key string, item *Item) bool
	Get(c context.Context, key string) *Item
	GetPreferredStorage() Storage
	GetBackupStorage() Storage
}

type ReadStrategy int

const (
	CacheAndDB ReadStrategy = iota
	CacheOnly
	CacheMasterOnly
	DBOnly
)

const ReadStrategyContextKey string = "ReadStrategy"

func SetReadStrategyToContent(c context.Context, strategy ReadStrategy) context.Context {
	return context.WithValue(c, ReadStrategyContextKey, strategy)
}

func GetReadStrategyFromContext(c context.Context) ReadStrategy {
	if c == nil {
		return CacheAndDB
	}

	var rs ReadStrategy
	if c.Value(ReadStrategyContextKey) != nil {
		rs, _ = c.Value(ReadStrategyContextKey).(ReadStrategy)
	} else {
		rs = CacheAndDB
	}
	return rs
}

// DefaultProxy is default storage proxy interface implementation
type DefaultProxy struct {
	ReadThroughProcessor ReadThroughProcessor
	PreferredStorage     Storage
	BackupStorage        Storage
}

func (s *DefaultProxy) GetMulti(c context.Context, keys ...string) map[string]*Item {
	rs := GetReadStrategyFromContext(c)
	switch rs {
	case CacheOnly:
		return s.PreferredStorage.GetMulti(c, keys...)
	case CacheMasterOnly:
		return s.PreferredStorage.GetMultiFromMaster(c, keys...)
	case DBOnly:
		return s.BackupStorage.GetMulti(c, keys...)
	default:
		result := s.PreferredStorage.GetMulti(c, keys...)

		if len(result) < len(keys) {
			leftKeys := make([]string, 0, len(keys))
			for _, key := range keys {
				if item, ok := result[key]; !ok || item == nil {
					leftKeys = append(leftKeys, key)
				}
			}
			extra := s.BackupStorage.GetMulti(c, leftKeys...)

			for key, item := range extra {
				if item != nil {
					result[key] = item
				}
			}

			// set back for preferred storage
			if s.ReadThroughProcessor != nil {
				var wg sync.WaitGroup
				for keyVal, itemVal := range extra {
					if itemVal == nil {
						continue
					}
					wg.Add(1)
					go func(key string, item *Item) {
						defer wg.Done()
						s.ReadThroughProcessor(c, key, item, s.PreferredStorage)
					}(keyVal, itemVal)
				}
				wg.Wait()
			}
		}
		return result
	}
}

func (s *DefaultProxy) Set(c context.Context, key string, item *Item) bool {
	s.PreferredStorage.Set(c, key, item)
	if s.BackupStorage != nil {
		s.BackupStorage.Set(c, key, item)
	}
	return true
}

func (s *DefaultProxy) Get(c context.Context, key string) *Item {
	rs := GetReadStrategyFromContext(c)
	switch rs {
	case CacheOnly:
		return s.PreferredStorage.Get(c, key)
	case CacheMasterOnly:
		return s.PreferredStorage.GetFromMaster(c, key)
	case DBOnly:
		return s.BackupStorage.Get(c, key)
	default:
		result := s.PreferredStorage.Get(c, key)
		if result == nil && s.BackupStorage != nil {
			if result = s.BackupStorage.Get(c, key); result != nil {
				s.ReadThroughProcessor(c, key, result, s.PreferredStorage)
			}
		}
		return result
	}
}

func (s *DefaultProxy) GetPreferredStorage() Storage {
	return s.PreferredStorage
}

func (s *DefaultProxy) GetBackupStorage() Storage {
	return s.BackupStorage
}
