package domain

import (
	"sync"

	"github.com/zhiqiangxu/mondis/document/meta/sequence"
)

// Collection model
type Collection struct {
	mu struct {
		sync.RWMutex
		indices map[string]*Index
	}
	cid         int64
	didSequence *sequence.Hash
}

// Index for find an index by name
func (collection *Collection) Index(name string) (idx *Index, err error) {
	collection.mu.RLock()
	idx = collection.mu.indices[name]
	collection.mu.RUnlock()

	if idx == nil {
		err = ErrIndexNotExists
	}
	return
}
