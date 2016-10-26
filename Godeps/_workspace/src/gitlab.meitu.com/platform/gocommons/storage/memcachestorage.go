package storage

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/memcache"
	"gitlab.meitu.com/platform/gocommons/flag"
	"golang.org/x/net/context"
)

var keyNotFoundError = errors.New("Key not found")

type MemcacheStorage struct {
	options           MemcacheStorageOptions
	master            memcache.Client
	slave             memcache.Client
	replicas          []memcache.Client
	setBackExpiration uint32
	replicaExpiration uint32
	logError          func(c context.Context, err error, v ...interface{})
	logInfo           func(c context.Context, v ...interface{})
	factor            uint32
}

type MemcacheStorageOptions struct {
	// MasterAsOneReplica bool
	// SlaveAsOneReplica  bool
	SetBackMaster      bool
	SetBackExpiration  uint32 // a relative seconds from now
	ReplicaExpiration  uint32 // a relative seconds from now
	ReplicaWritePolicy WritePolicy
	Adaptor            ItemAdaptor
	LogError           func(c context.Context, err error, v ...interface{})
	LogInfo            func(c context.Context, v ...interface{})
}

func DefaultMemcacheStorageOptions() MemcacheStorageOptions {
	return MemcacheStorageOptions{
		// MasterAsOneReplica: true,
		// SlaveAsOneReplica:  true,
		SetBackMaster:      true,
		ReplicaWritePolicy: WriteAlways,
	}
}

type WritePolicy int

const (
	WriteAlways      WritePolicy = 0
	WriteIfExists    WritePolicy = 1
	WriteIfNotExists WritePolicy = 2
)

func NewMemcacheStorage(master, slave memcache.Client, replicas []memcache.Client,
	options MemcacheStorageOptions) *MemcacheStorage {

	if replicas == nil {
		replicas = make([]memcache.Client, 0, 2)
	}

	// set master and slave as replicas anyway
	if master != nil {
		replicas = append(replicas, master)
	}
	if slave != nil {
		replicas = append(replicas, slave)
	}

	expiration := options.SetBackExpiration
	replicaExpiration := options.ReplicaExpiration
	if replicaExpiration == 0 && expiration > 0 {
		replicaExpiration = expiration
	}

	logError := options.LogError
	if logError == nil {
		logError = func(c context.Context, err error, v ...interface{}) {}
	}
	logIngo := options.LogInfo
	if logIngo == nil {
		logIngo = func(c context.Context, v ...interface{}) {}
	}
	return &MemcacheStorage{
		options:           options,
		master:            master,
		slave:             slave,
		replicas:          replicas,
		setBackExpiration: expiration,
		replicaExpiration: replicaExpiration,
		logError:          logError,
		logInfo:           logIngo,
	}
}

// Get retrieves a single value from the storage.
// procedure:
//            ------- <---
//           |replica| <-|---|
//               |       |   |
//            -------- ---   |
//           | master |<--   |
//               |       |   |
//            --------   |   |
//           | slave  |-------
//
// replica can also be master or slave
func (m *MemcacheStorage) Get(c context.Context, key string) *Item {
	var replica memcache.Client

	// choose a replica, can also be master or slave
	replica = m.chooseReplica()
	if replica != nil {
		getResponse := replica.Get(key)
		if err := getResponse.Error(); err != nil {
			m.logError(c, err, " when get on mc replica, key: ", key)
		} else if status := getResponse.Status(); status == memcache.StatusNoError {
			item := &Item{
				Value:       getResponse.Value(),
				Flags:       getResponse.Flags(),
				DataVersion: 0, // 抹掉Version，从副本返回的Version，不一定和Master中的是一致的
			}
			if m.options.Adaptor != nil {
				item, err = m.options.Adaptor.AdaptGetItem(item)
				if err != nil {
					m.logError(c, err, " when adapt get response value from mc replica, key: ", key)
					return nil
				}
			}
			return item
		} else if status == memcache.StatusKeyNotFound {
			// m.logInfo(c, keyNotFoundError, " when get on mc replica, key: ", key)
		}
	}

	if m.master != replica {
		getResponse := m.master.Get(key)
		if err := getResponse.Error(); err != nil {
			m.logError(c, err, " when get on mc master, key: ", key)
		} else if status := getResponse.Status(); status == memcache.StatusNoError {
			if !(replica == nil || flag.ContainsFlag(getResponse.Flags(), flagDirty)) {
				//set back for replica
				item := &memcache.Item{
					Key:           key,
					Value:         getResponse.Value(),
					Flags:         getResponse.Flags(),
					DataVersionId: 0,
				}
				if m.replicaExpiration > 0 {
					item.Expiration = uint32(time.Now().Unix()) + m.replicaExpiration
				}
				setResponse := replica.Set(item)
				if err := setResponse.Error(); err != nil {
					m.logError(c, err, " when set on mc replica with master value, key: ", key)
				}
			}

			item := &Item{
				Value:       getResponse.Value(),
				Flags:       getResponse.Flags(),
				DataVersion: getResponse.DataVersionId(),
			}

			if m.options.Adaptor != nil {
				item, err = m.options.Adaptor.AdaptGetItem(item)
				if err != nil {
					m.logError(c, err, " when adapt get response value from mc master, key: ", key)
					return nil
				}
			}

			return item
		} else if status == memcache.StatusKeyNotFound {
			// m.logInfo(c, keyNotFoundError, " when get on mc master, key: ", key)
		}
	}

	if m.slave != nil && m.slave != replica {
		getResponse := m.slave.Get(key)
		if err := getResponse.Error(); err != nil {
			m.logError(c, err, " when get on mc slave, key: ", key)
		} else if status := getResponse.Status(); status == memcache.StatusNoError {
			if !flag.ContainsFlag(getResponse.Flags(), flagDirty) {
				item := &memcache.Item{
					Key:           key,
					Value:         getResponse.Value(),
					Flags:         getResponse.Flags(),
					DataVersionId: 0,
				}
				now := uint32(time.Now().Unix())
				// set back for replica
				if replica != nil {
					if m.replicaExpiration > 0 {
						item.Expiration = now + m.replicaExpiration
					}
					setResponse := replica.Set(item)
					if err := setResponse.Error(); err != nil {
						m.logError(c, err, " when set to mc replica with slave value, key: ", key)
					}
				}

				// set back for master
				if m.options.SetBackMaster && m.master != replica {
					if m.setBackExpiration > 0 {
						item.Expiration = now + m.setBackExpiration
					}
					setResponse := m.master.Set(item)
					if err := setResponse.Error(); err != nil {
						m.logError(c, err, " when setback to mc master with slave value, key: ", key)
					}
				}
			}

			item := &Item{
				Value:       getResponse.Value(),
				Flags:       getResponse.Flags(),
				DataVersion: 0, // 抹掉Version，从Slave返回的Version，不一定和Master中的是一致的
			}

			if m.options.Adaptor != nil {
				item, err = m.options.Adaptor.AdaptGetItem(item)
				if err != nil {
					m.logError(c, err, " when adapt get response value from mc master, key: ", key)
					return nil
				}
			}

			return item
		} else if status == memcache.StatusKeyNotFound {
			// m.logInfo(c, keyNotFoundError, " when get on mc slave, key: ", key)
		}
	}

	return nil
}

// GetMulti retrieves multiple values from the storage.  The items are returned
// in the same order as the input keys.
func (m *MemcacheStorage) GetMulti(c context.Context, keys ...string) map[string]*Item {
	results := make(map[string]*Item)
	var multiResponse map[string]memcache.GetResponse
	replica := m.chooseReplica()
	if replica != nil {
		multiResponse = replica.GetMulti(keys)
	}

	replicaMissedKeys := make([]string, 0, len(keys))
	if multiResponse == nil {
		replicaMissedKeys = keys
	} else {
		for key, response := range multiResponse {
			if err := response.Error(); err != nil {
				replicaMissedKeys = append(replicaMissedKeys, key)
				m.logError(c, err, " when get multi on mc replica, key:", key)
			} else if status := response.Status(); status == memcache.StatusNoError {
				item := &Item{
					Value:       response.Value(),
					Flags:       response.Flags(),
					DataVersion: 0,
				}

				if m.options.Adaptor == nil {
					results[key] = item
				} else {
					if item, err = m.options.Adaptor.AdaptGetItem(item); err == nil {
						results[key] = item
					} else {
						m.logError(c, err, " when adapt get response value from mc replica, key: ", key)
					}
				}
			} else if status == memcache.StatusKeyNotFound {
				replicaMissedKeys = append(replicaMissedKeys, key)
				// m.logInfo(c, keyNotFoundError, " when get multi on mc replica, key:", key)
			}
		}
	}
	if len(replicaMissedKeys) == 0 {
		return results
	}

	masterMissedKeys := make([]string, 0, len(replicaMissedKeys))
	if m.master != replica {
		missedItems := m.master.GetMulti(replicaMissedKeys)
		setBackItems := make([]*memcache.Item, 0, len(replicaMissedKeys))
		for missedKey, response := range missedItems {
			if err := response.Error(); err != nil {
				m.logError(c, err, " when get multi on mc master, key:", missedKey)
				masterMissedKeys = append(masterMissedKeys, missedKey)
			} else if status := response.Status(); status == memcache.StatusNoError {
				item := &Item{
					Value:       response.Value(),
					Flags:       response.Flags(),
					DataVersion: response.DataVersionId(),
				}

				if m.options.Adaptor == nil {
					results[missedKey] = item
				} else {
					if item, err = m.options.Adaptor.AdaptGetItem(item); err == nil {
						results[missedKey] = item
					} else {
						m.logError(c, err, " when adapt get response value from mc master, key: ", missedKey)
					}
				}

				if !flag.ContainsFlag(response.Flags(), flagDirty) {
					setBackItems = append(
						setBackItems,
						&memcache.Item{
							Key:           missedKey,
							Value:         response.Value(),
							Flags:         response.Flags(),
							DataVersionId: 0,
						},
					)
				}
			} else if status == memcache.StatusKeyNotFound {
				// m.logInfo(c, keyNotFoundError, " when get multi on mc master, key:", missedKey)
				masterMissedKeys = append(masterMissedKeys, missedKey)
			}
			delete(missedItems, missedKey)
		}
		missedItems = nil

		if replica != nil && len(setBackItems) > 0 {
			if m.replicaExpiration > 0 {
				replicaExpiration := uint32(time.Now().Unix()) + m.replicaExpiration
				for _, item := range setBackItems {
					item.Expiration = replicaExpiration
				}
			}
			setMultiResponses := replica.SetMulti(setBackItems)
			for _, response := range setMultiResponses {
				if err := response.Error(); err != nil {
					m.logError(c, err, " when set mulit on mc replica with master value, key:", response.Key())
				}
			}
		}
	}
	if len(masterMissedKeys) == 0 {
		return results
	}

	if m.slave != nil && m.slave != replica {
		masterMissedItems := m.slave.GetMulti(masterMissedKeys)
		setBackItems := make([]*memcache.Item, 0, len(masterMissedKeys))
		for key, response := range masterMissedItems {
			if err := response.Error(); err != nil {
				m.logError(c, err, " when get multi on mc slave, key:", key)
			} else if status := response.Status(); status == memcache.StatusNoError {
				item := &Item{
					Value:       response.Value(),
					Flags:       response.Flags(),
					DataVersion: response.DataVersionId(),
				}

				if m.options.Adaptor == nil {
					results[key] = item
				} else {
					if item, err = m.options.Adaptor.AdaptGetItem(item); err == nil {
						results[key] = item
					} else {
						m.logError(c, err, " when adapt get response value from mc slave, key: ", key)
					}
				}

				if !flag.ContainsFlag(response.Flags(), flagDirty) {
					setBackItems = append(
						setBackItems,
						&memcache.Item{
							Key:           key,
							Value:         response.Value(),
							Flags:         response.Flags(),
							DataVersionId: 0,
						},
					)
				}
			} else if status == memcache.StatusKeyNotFound {
				// m.logInfo(c, keyNotFoundError, " when get multi on mc slave, key:", key)
			}
			delete(masterMissedItems, key)
		}
		masterMissedItems = nil

		if len(setBackItems) > 0 {
			var setMultiResponses []memcache.MutateResponse
			if replica != nil {
				if m.replicaExpiration > 0 {
					replicaExpiration := uint32(time.Now().Unix()) + m.replicaExpiration
					for _, item := range setBackItems {
						item.Expiration = replicaExpiration
					}
				}
				setMultiResponses = replica.SetMulti(setBackItems)
				for _, response := range setMultiResponses {
					if err := response.Error(); err != nil {
						m.logError(c, err, " when set mulit on mc replica with slave value, key:", response.Key())
					}
				}
			}
			if m.options.SetBackMaster && m.master != replica {
				if m.setBackExpiration > 0 {
					setBackExpiration := uint32(time.Now().Unix()) + m.setBackExpiration
					for _, item := range setBackItems {
						item.Expiration = setBackExpiration
					}
				}
				setMultiResponses = m.master.SetMulti(setBackItems)
				for _, response := range setMultiResponses {
					if err := response.Error(); err != nil {
						m.logError(c, err, " when set multi on mc master with slave value, key:", response.Key())
					}
				}
			}
		}
	}

	return results
}

func (m *MemcacheStorage) chooseReplica() memcache.Client {
	count := len(m.replicas)
	if count == 0 {
		return nil
	}
	atomic.AddUint32(&m.factor, 1)
	if m.factor > 1000000000 {
		atomic.StoreUint32(&m.factor, 0)
	}
	index := int(m.factor) % count
	if index == count {
		return nil
	}

	return m.replicas[index]
}

func (m *MemcacheStorage) GetFromMaster(c context.Context, key string) *Item {
	response := m.master.Get(key)
	if err := response.Error(); err != nil {
		m.logError(c, err, " when get on mc master, key:", key)
	} else if status := response.Status(); status == memcache.StatusNoError {
		item := &Item{
			Value:       response.Value(),
			Flags:       response.Flags(),
			DataVersion: response.DataVersionId(),
		}
		if m.options.Adaptor != nil {
			item, err = m.options.Adaptor.AdaptGetItem(item)
			if err != nil {
				m.logError(c, err, " when adapt get response value from mc master, key: ", key)
				return nil
			}
		}
		return item
	} else if response.Status() == memcache.StatusKeyNotFound {
		// m.logInfo(c, keyNotFoundError, " when get on mc master, key:", key)
	}
	return nil
}

func (m *MemcacheStorage) GetMultiFromMaster(c context.Context, keys ...string) map[string]*Item {
	results := make(map[string]*Item)
	multiResponse := m.master.GetMulti(keys)
	for key, response := range multiResponse {
		if err := response.Error(); err != nil {
			m.logError(c, err, " when get multi on mc master, key:", key)
		} else if status := response.Status(); status == memcache.StatusNoError {
			item := &Item{
				Value:       response.Value(),
				Flags:       response.Flags(),
				DataVersion: response.DataVersionId(),
			}

			if m.options.Adaptor == nil {
				results[key] = item
				continue
			}

			if item, err = m.options.Adaptor.AdaptGetItem(item); err == nil {
				results[key] = item
			} else {
				m.logError(c, err, " when adapt get response value from mc master, key: ", key)
			}
		} else if status == memcache.StatusKeyNotFound {
			// m.logInfo(c, keyNotFoundError, " when get multi on master, key:", key)
		}
	}
	return results
}

// Set stores a single item into the storage.
// Set Order: Master-->Slave-->Replicas
func (m *MemcacheStorage) Set(c context.Context, key string, item *Item) bool {
	var err error
	if m.options.Adaptor != nil {
		item, err = m.options.Adaptor.AdaptSetItem(item)
	}
	if err != nil {
		m.logError(c, err, " when adapt set value, key: ", key)
		return false
	}

	mcItem := &memcache.Item{
		Key:           key,
		Flags:         item.Flags,
		Value:         item.Value.([]byte),
		DataVersionId: item.DataVersion,
		Expiration:    uint32(item.ExpireAt),
	}

	setResponse := m.master.Set(mcItem)
	if setResponse.Status() == memcache.StatusKeyExists {
		m.logInfo(c, " key exists when set on master, key:", key)
	} else if err := setResponse.Error(); err != nil {
		m.logError(c, err, " when set on master, item:", *item)
		return false
	}

	// 在设置Slave和Replica时，把CAS Version置为0
	mcItem.DataVersionId = 0
	if m.slave != nil {
		setResponse = m.slave.Set(mcItem)
		if err := setResponse.Error(); err != nil {
			m.logError(c, err, " when set on slave, key:", key)
			return false
		}
	}

	// exclude master and slave from replicas
	if m.slave != nil && len(m.replicas) > 2 {
		toWriteReplicas := make([]memcache.Client, 0, len(m.replicas)-2)
		for _, replica := range m.replicas {
			if replica == m.slave || replica == m.master {
				continue
			}
			toWriteReplicas = append(toWriteReplicas, replica)
		}
		m.writeWithPolicy(mcItem, m.options.ReplicaWritePolicy, toWriteReplicas...)
	}

	return true
}

func (m *MemcacheStorage) SetMulti(c context.Context, itemMap map[string]*Item) bool {
	items := make([]*memcache.Item, 0)
	var err error
	for key, item := range itemMap {
		if m.options.Adaptor != nil {
			item, err = m.options.Adaptor.AdaptSetItem(item)
		}

		if err == nil {
			items = append(items, &memcache.Item{
				Key:           key,
				Value:         item.Value.([]byte),
				Flags:         item.Flags,
				DataVersionId: item.DataVersion,
				Expiration:    uint32(item.ExpireAt),
			})
		} else {
			m.logError(c, err, " when adapt set value, key: ", key)
		}
	}

	err = m.setMulti(c, m.master, items)
	if err != nil {
		m.logError(c, err, " when set multi on mc master")
		return false
	}

	// 在设置Slave和Replica时，把CAS Version置为0
	for _, item := range items {
		item.DataVersionId = 0
	}

	if m.slave != nil {
		err := m.setMulti(c, m.slave, items)
		if err != nil {
			m.logError(c, err, " when set multi on mc slave")
			return false
		}
	}

	// exclude master and slave from replicas
	if m.slave != nil && len(m.replicas) > 2 {
		toWriteReplicas := make([]memcache.Client, 0, len(m.replicas)-2)
		for _, replica := range m.replicas {
			if replica == m.slave || replica == m.master {
				continue
			}
			toWriteReplicas = append(toWriteReplicas, replica)
		}
		m.writeMultiWithPolicy(items, m.options.ReplicaWritePolicy, toWriteReplicas...)
	}

	return true
}

func (m *MemcacheStorage) setMulti(c context.Context, client memcache.Client, items []*memcache.Item) error {
	var err error
	count := len(items)
	masterChan := make(chan memcache.MutateResponse, count)
	for _, item := range items {
		go func(itemVal *memcache.Item) {
			masterChan <- client.Set(itemVal)
		}(item)
	}

	for i := 0; i < count; i++ {
		response := <-masterChan
		if err = response.Error(); err != nil {
			m.logError(c, err, " when set on mc, key: ", response.Key())
		}
	}

	return err
}

func (m *MemcacheStorage) writeMultiWithPolicy(items []*memcache.Item, policy WritePolicy, clients ...memcache.Client) {
	switch policy {
	case WriteAlways:
		for _, client := range clients {
			client.SetMulti(items)
		}
		break
	case WriteIfExists:
		for _, client := range clients {
			for _, item := range items {
				client.Replace(item)
			}
		}
		break
	case WriteIfNotExists:
		for _, client := range clients {
			client.AddMulti(items)
		}
		break
	}
}

func (m *MemcacheStorage) writeWithPolicy(item *memcache.Item, policy WritePolicy, clients ...memcache.Client) {
	switch policy {
	case WriteAlways:
		for _, client := range clients {
			client.Set(item)
		}
		break
	case WriteIfExists:
		for _, client := range clients {
			client.Replace(item)
		}
		break
	case WriteIfNotExists:
		for _, client := range clients {
			client.Add(item)
		}
		break
	}
}

// Delete removes a single item from the storage.
func (m *MemcacheStorage) Delete(c context.Context, key string) bool {
	var delResponse memcache.MutateResponse

	for _, replica := range m.replicas {
		delResponse = replica.Delete(key)
		if err := delResponse.Error(); err != nil && delResponse.Status() != memcache.StatusKeyNotFound {
			m.logError(c, err, " when delete on mc replica, key:", key)
			return false
		}
	}

	return true
}
