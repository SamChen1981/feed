package network

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/godropbox/resource_pool"
)

type idleHandle struct {
	handle    interface{}
	keepUntil *time.Time
}

// A resource pool implementation where all handles are associated to the same resource location.
type MeituResourcePool struct {
	options resource_pool.Options

	numActive    *int32 // atomic counter
	maxWaitTime  *time.Duration
	idlesChannel chan *idleHandle

	activeHighWaterMark *int32 // atomic / monotonically increasing value

	rwMutex    sync.RWMutex
	location   string // guard by mutex
	isLameDuck bool   // guarded by mutex
}

// This returns a SimpleResourcePool, where all handles are associated to a single resource location.
func NewMeituResourcePool(options Options) resource_pool.ResourcePool {
	numActive := new(int32)
	atomic.StoreInt32(numActive, 0)

	activeHighWaterMark := new(int32)
	atomic.StoreInt32(activeHighWaterMark, 0)

	return &MeituResourcePool{
		rwMutex:             sync.RWMutex{},
		location:            "",
		isLameDuck:          false,
		options:             options.getResourceOptions(),
		maxWaitTime:         options.MaxWaitTime,
		numActive:           numActive,
		activeHighWaterMark: activeHighWaterMark,
		idlesChannel:        make(chan *idleHandle, options.MaxIdleConnections),
	}
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) NumActive() int32 {
	return atomic.LoadInt32(p.numActive)
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) ActiveHighWaterMark() int32 {
	return atomic.LoadInt32(p.activeHighWaterMark)
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) NumIdle() int {
	return len(p.idlesChannel)
}

// SimpleResourcePool can only register a single (network, address) entry.
// Register should be call before any Get calls.
func (p *MeituResourcePool) Register(resourceLocation string) error {
	if resourceLocation == "" {
		return errors.New("Invalid resource location")
	}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.isLameDuck {
		return fmt.Errorf(
			"Cannot register %s to lame duck resource pool",
			resourceLocation)
	}

	if p.location == "" {
		p.location = resourceLocation
		return nil
	}
	return errors.New("MeituResourcePool can only register one location")
}

// SimpleResourcePool does not support Unregister.
func (p *MeituResourcePool) Unregister(resourceLocation string) error {
	return errors.New("MeituResourcePool does not support Unregister")
}

func (p *MeituResourcePool) ListRegistered() []string {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.location != "" {
		return []string{p.location}
	}
	return []string{}
}

// This gets an active resource from the resource pool.  Note that the
// resourceLocation argument is ignored (The handles are associated to the
// resource location provided by the first Register call).
func (p *MeituResourcePool) Get(unused string) (resource_pool.ManagedHandle, error) {
	activeCount := atomic.AddInt32(p.numActive, 1)
	if p.options.MaxActiveHandles > 0 &&
		activeCount > p.options.MaxActiveHandles {

		atomic.AddInt32(p.numActive, -1)
		if p.maxWaitTime == nil || *p.maxWaitTime == 0 {
			return nil, fmt.Errorf("Too many handles to %s", p.location)
		}

		select {
		case <-time.After(*p.maxWaitTime):
			return nil, fmt.Errorf("get handles for %s timeout, current idle: %d", p.location, len(p.idlesChannel))
		case idle := <-p.idlesChannel:
			atomic.AddInt32(p.numActive, 1)
			return resource_pool.NewManagedHandle(p.location, idle.handle, p, p.options), nil
		}
	}

	highest := atomic.LoadInt32(p.activeHighWaterMark)
	for activeCount > highest &&
		!atomic.CompareAndSwapInt32(
			p.activeHighWaterMark,
			highest,
			activeCount) {

		highest = atomic.LoadInt32(p.activeHighWaterMark)
	}

	if h := p.getIdleHandle(); h != nil {
		return h, nil
	}

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if p.location == "" {
		atomic.AddInt32(p.numActive, -1)
		return nil, fmt.Errorf(
			"Resource location is not set for SimpleResourcePool")
	}

	if p.isLameDuck {
		atomic.AddInt32(p.numActive, -1)
		return nil, fmt.Errorf(
			"Lame duck resource pool cannot return handles to %s",
			p.location)
	}

	handle, err := p.options.Open(p.location)
	if err != nil {
		count := atomic.AddInt32(p.numActive, -1)
		return nil, fmt.Errorf(
			"Failed to open resource handle: %s, current active: %d, current idle: %v (%v)",
			p.location, count, len(p.idlesChannel), err)
	}
	return resource_pool.NewManagedHandle(p.location, handle, p, p.options), nil
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) Release(handle resource_pool.ManagedHandle) error {
	if pool, ok := handle.Owner().(*MeituResourcePool); !ok || pool != p {
		return errors.New(
			"Meitu resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		p.queueIdleHandles(h)
		atomic.AddInt32(p.numActive, -1)
	}

	return nil
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) Discard(handle resource_pool.ManagedHandle) error {
	if pool, ok := handle.Owner().(*MeituResourcePool); !ok || pool != p {
		return errors.New(
			"Meitu resource pool cannot take control of a handle owned " +
				"by another resource pool")
	}

	h := handle.ReleaseUnderlyingHandle()
	if h != nil {
		atomic.AddInt32(p.numActive, -1)
		if err := p.options.Close(h); err != nil {
			return fmt.Errorf("Failed to close resource handle: %v", err)
		}
	}
	return nil
}

// See ResourcePool for documentation.
func (p *MeituResourcePool) EnterLameDuckMode() {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	p.isLameDuck = true
	for idle := range p.idlesChannel {
		p.options.Close(idle.handle)
	}
	p.idlesChannel = make(chan *idleHandle, 0)
}

func (p *MeituResourcePool) getCurrentTime() time.Time {
	if p.options.NowFunc == nil {
		return time.Now()
	} else {
		return p.options.NowFunc()
	}
}

// This returns an idle resource, if there is one.
func (p *MeituResourcePool) getIdleHandle() resource_pool.ManagedHandle {
	now := p.getCurrentTime()
	for {
		select {
		case idle := <-p.idlesChannel:
			if idle.keepUntil == nil || now.Before(*idle.keepUntil) {
				return resource_pool.NewManagedHandle(p.location, idle.handle, p, p.options)
			}

			p.options.Close(idle.handle)
		default:
			return nil
		}
	}
}

// This adds an idle resource to the pool.
func (p *MeituResourcePool) queueIdleHandles(handle interface{}) {
	now := p.getCurrentTime()
	var keepUntil *time.Time
	if p.options.MaxIdleTime != nil {
		// NOTE: Assign to temp variable first to work around compiler bug
		x := now.Add(*p.options.MaxIdleTime)
		keepUntil = &x
	}

	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	if p.isLameDuck {
		p.options.Close(handle)
		return
	}

	select {
	case p.idlesChannel <- &idleHandle{handle: handle, keepUntil: keepUntil}:
		// Do Nothing
	default:
		// idle channel is full, close handle
		p.options.Close(handle)
	}
}
