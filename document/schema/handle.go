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

// Get schema meta cache atomically.
func (h *Handle) Get() *MetaCache {
	v := h.value.Load()
	cache, _ := v.(*MetaCache)
	return cache
}

// Check is called before a dml txn commit.
// this is called when txn commits
func (h *Handle) Check(ctx context.Context, startVersion int64, updatedCollections map[int64]struct{}) (checkOK bool, err error) {
	err = h.mu.RLock(ctx)
	if err != nil {
		return
	}
	v := h.value.Load()
	cache, _ := v.(*MetaCache)
	if cache != nil && cache.version > startVersion {
		for i := startVersion + 1; i <= cache.version; i++ {
			diffIdx := int(i - cache.diffStartVersion)
			if diffIdx < 0 {
				h.mu.RUnlock()
				return
			}
			if diffIdx >= len(cache.schemaDiffs) {
				h.mu.RUnlock()
				panic("bug: diffIdx >= len(cache.schemaDiffs)")
			}

			for _, collectionIDs := range cache.schemaDiffs[diffIdx] {
				if _, ok := updatedCollections[collectionIDs]; ok {
					return
				}
			}
		}
		checkOK = true
		h.mu.RUnlock()
		return
	}
	checkOK = true
	h.mu.RUnlock()
	return
}

// Update for update schema meta cache
func (h *Handle) Update(ctx context.Context, cache *MetaCache) (err error) {
	err = h.mu.Lock(ctx)
	if err != nil {
		return
	}

	h.value.Store(cache)

	h.mu.Unlock()
	return
}
