package domain

import "sync"

// Collection model
type Collection struct {
	mu struct {
		sync.RWMutex
		indexes map[string]*Index
	}
}

// Index for find an index by name
func (collection *Collection) Index(name string) (idx *Index, err error) {
	collection.mu.RLock()
	idx = collection.mu.indexes[name]
	collection.mu.RUnlock()

	if idx == nil {
		err = ErrIndexNotExists
	}
	return
}
