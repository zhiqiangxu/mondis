package domain

import (
	"errors"
	"sync"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// Domain represents a storage space
type Domain struct {
	handle *schema.Handle
	kvdb   mondis.KVDB
	mu     struct {
		sync.RWMutex
		dbs map[string]*DB
	}
	ddl *ddl.DDL
}

// NewDomain is ctor for Domain
func NewDomain(kvdb mondis.KVDB) *Domain {
	do := &Domain{
		handle: schema.NewHandle(),
		kvdb:   kvdb,
		ddl:    ddl.New(kvdb, ddl.Options{}),
	}
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
func (do *Domain) DDL() *ddl.DDL {
	return do.ddl
}

func (do *Domain) newCollection(dbID int64, info model.CollectionInfo) (collection *Collection, err error) {
	didSequence, err := meta.NewDocIDSequence(do.kvdb, dbID, info.ID, 0)
	if err != nil {
		return
	}
	collection = &Collection{didSequence: didSequence}
	return
}
