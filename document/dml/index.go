package dml

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// Index model
type Index struct {
	dbName         string
	collectionName string
	indexName      string
	base
}

func newIndex(dbName, collectionName, indexName string, kvdb mondis.KVDB, handle *schema.Handle) *Index {
	return &Index{dbName: dbName, collectionName: collectionName, indexName: indexName, base: base{kvdb: kvdb, handle: handle}}
}

// Lookup by index
func (idx *Index) Lookup() {

}
