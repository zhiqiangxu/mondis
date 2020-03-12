package schema

import (
	"sync/atomic"
)

// Handle handles schema cache, including getting and setting.
type Handle struct {
	value atomic.Value
}

// NewHandle is ctor for Handle
func NewHandle() *Handle {
	h := &Handle{}
	return h
}

// Get gets schema meta cache from Handle.
func (h *Handle) Get() *MetaCache {
	v := h.value.Load()
	cache, _ := v.(*MetaCache)
	return cache
}

// Set for update schema into Handle
func (h *Handle) Set(cache *MetaCache) {
	h.value.Store(cache)
}
