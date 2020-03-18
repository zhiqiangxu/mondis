package dml

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// DB model
type DB struct {
	Name string
	base
}

var (
	// ErrDBNotExists used by Domain
	ErrDBNotExists = errors.New("db not exists")
	// ErrCollectionNotExists used by Domain
	ErrCollectionNotExists = errors.New("collection not exists")
	// ErrIndexNotExists used by Domain
	ErrIndexNotExists = errors.New("index not exists")
)

// NewDB is ctor for DB
func NewDB(name string, kvdb mondis.KVDB, handle *schema.Handle) (db *DB, err error) {
	schemaCache := handle.Get()

	exists := schemaCache.CheckDBExists(name)
	if !exists {
		err = ErrDBNotExists
		return
	}

	db = &DB{Name: name, base: base{kvdb: kvdb, handle: handle}}
	return
}

// Collection for find a collection by name
func (db *DB) Collection(name string) (collection *Collection, err error) {

	schemaCache := db.handle.Get()

	if !schemaCache.CheckCollectionExists(db.Name, name) {
		err = ErrCollectionNotExists
		return
	}

	collection = newCollection(db.Name, name, db.kvdb, db.handle)
	return
}
