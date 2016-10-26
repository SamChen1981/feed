// Package storage is a utility package for building storage layers.
package storage

import (
	"gitlab.meitu.com/platform/gocommons/flag"
	"golang.org/x/net/context"
)

// Storage interface define a generic key value store. The storage may be persistent
// (e.g., a database) or volatile (e.g., cache).
type Storage interface {
	// Get retrieves a single value from the storage.
	Get(c context.Context, key string) *Item

	// GetMulti retrieves multiple values from the storage.
	GetMulti(c context.Context, keys ...string) map[string]*Item

	// Set stores a single item into the storage.
	Set(c context.Context, key string, item *Item) bool

	// SetMulti stores multiple items into the storage.
	SetMulti(c context.Context, itemMap map[string]*Item) bool

	// Delete removes a single item from the storage.
	Delete(c context.Context, key string) bool

	// GetFromMaster retrieves a single value from the master storage node.
	GetFromMaster(c context.Context, key string) *Item

	// GetMultiFromMaster retrieves multiple value from the master storage node.
	GetMultiFromMaster(c context.Context, keys ...string) map[string]*Item
}

type ItemAdaptor interface {
	AdaptGetItem(item *Item) (*Item, error)
	AdaptSetItem(item *Item) (*Item, error)
}

type Item struct {
	// The item's value.
	Value interface{}

	// Flags are server-opaque flags whose semantics are entirely up to the app.
	Flags uint32

	// aka CAS (check and set)
	DataVersion uint64

	// ExpireAt: Unix epoch time.
	// Zero means the Item does not expires.
	ExpireAt int64
}

var flagDirty = flag.Flag{30}

func (i *Item) Dirty() bool {
	return flag.ContainsFlag(i.Flags, flagDirty)
}

func (i *Item) MarkDirty() {
	i.Flags = flag.MarkFlag(i.Flags, flagDirty)
}
