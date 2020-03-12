package domain

import (
	"errors"
	"sync"

	"github.com/zhiqiangxu/mondis"
)

// Domain represents a storage space
type Domain struct {
	kvdb mondis.KVDB
	mu   struct {
		sync.RWMutex
		dbs map[string]*DB
	}
	ddl *DDL
}

// NewDomain is ctor for Domain
func NewDomain(kvdb mondis.KVDB) *Domain {
	do := &Domain{kvdb: kvdb}
	do.ddl = newDDL(do)
	return do
}

var (
	// ErrDBNotExists used by Domain
	ErrDBNotExists = errors.New("db not exists")
	// ErrCollectionNotExists used by Domain
	ErrCollectionNotExists = errors.New("collection not exists")
	// ErrIndexNotExists used by Domain
	ErrIndexNotExists = errors.New("index not exists")
)

// DB for find a db by name
func (do *Domain) DB(name string) (db *DB, err error) {
	do.mu.RLock()
	db = do.mu.dbs[name]
	do.mu.RUnlock()
	if db == nil {
		err = ErrDBNotExists
	}
	return
}

// DDL getter
func (do *Domain) DDL() *DDL {
	return do.ddl
}
