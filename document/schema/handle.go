package schema

import (
	"context"
	"sync/atomic"

	"github.com/zhiqiangxu/util/mutex"
)

// Handle handles schema meta cache, including getting and setting.
type Handle struct {
	mu    mutex.CRWMutex
	value atomic.Value
}

// NewHandle is ctor for Handle
func NewHandle() *Handle {
	h := &Handle{}
	h.mu.Init()
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
func (h *Handle) GetLatest(ctx context.Context) (cache *MetaCache, err error) {
	err = h.mu.RLock(ctx)
	if err != nil {
		return
	}
	v := h.value.Load()
	h.mu.RUnlock()
	cache, _ = v.(*MetaCache)
	return
}

// Update for update schema meta cache into Handle
func (h *Handle) Update(ctx context.Context, f func() (*MetaCache, error)) (err error) {
	err = h.mu.Lock(ctx)
	if err != nil {
		return
	}
	defer h.mu.Unlock()

	cache, err := f()
	if err != nil {
		return
	}
	h.value.Store(cache)
	return
}
