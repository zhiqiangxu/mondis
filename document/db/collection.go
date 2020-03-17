package db

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/mondis/document/txn"
	"go.mongodb.org/mongo-driver/bson"
)

// Collection model
type Collection struct {
	dbName         string
	collectionName string
	base
}

func newCollection(dbName, collectionName string, kvdb mondis.KVDB, handle *schema.Handle) *Collection {
	return &Collection{dbName: dbName, collectionName: collectionName, base: base{kvdb: kvdb, handle: handle}}
}

// Index for find an index by name
func (collection *Collection) Index(name string) (idx *Index, err error) {
	schemaCache := collection.handle.Get()

	if !schemaCache.CheckIndexExists(collection.dbName, collection.collectionName, name) {
		err = ErrIndexNotExists
		return
	}

	idx = newIndex(collection.dbName, collection.collectionName, name, collection.kvdb, collection.handle)
	return
}

func (collection *Collection) checkExists() (err error) {
	if !collection.handle.Get().CheckCollectionExists(collection.dbName, collection.collectionName) {
		err = ErrCollectionNotExists
	}

	return
}

// InsertOne for insert a document into collection
func (collection *Collection) InsertOne(doc bson.M, t *txn.Txn) (did int64, err error) {

	err = collection.checkExists()
	if err != nil {
		return
	}

	insertFunc := func(t *txn.Txn) error {
		// did, err = t.InsertOne(collection.dbName, collection.collectionName, doc)
		return err
	}
	if t != nil {
		insertFunc(t)
	} else {
		collection.RunInNewUpdateTxn(insertFunc)
	}
	return
}
