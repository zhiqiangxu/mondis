package schema

import (
	"sync"
	"sync/atomic"
)

// Handle handles schema meta cache, including getting and setting.
type Handle struct {
	mu    sync.RWMutex
	value atomic.Value
}

// NewHandle is ctor for Handle
func NewHandle() *Handle {
	h := &Handle{}
	return h
}

// GetSnap gets schema meta cache atomically.
// this is called when txn starts
func (h *Handle) GetSnap() *MetaCache {
	v := h.value.Load()
	cache, _ := v.(*MetaCache)
	return cache
}

// GetLatest gets the latest schema meta cache.
// this is called when txn commits
func (h *Handle) GetLatest() *MetaCache {
	h.mu.RLock()
	v := h.value.Load()
	h.mu.RUnlock()
	cache, _ := v.(*MetaCache)
	return cache
}

// Update for update schema meta cache into Handle
func (h *Handle) Update(f func() (*MetaCache, error)) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cache, err := f()
	if err != nil {
		return
	}
	h.value.Store(cache)
	return
}
