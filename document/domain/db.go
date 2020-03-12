package domain

import "sync"

// DB model
type DB struct {
	id int64
	mu struct {
		sync.RWMutex
		collections map[string]*Collection
	}
}

// Collection for find a collection by name
func (db *DB) Collection(name string) (collection *Collection, err error) {
	db.mu.RLock()
	collection = db.mu.collections[name]
	db.mu.RUnlock()
	if collection == nil {
		err = ErrCollectionNotExists
	}
	return
}
