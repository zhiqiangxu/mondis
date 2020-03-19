package dml

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/model"
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

var (
	// ErrDocNotFound used by Collection
	ErrDocNotFound = errors.New("document not found")
	// ErrDocExists used by Collection
	ErrDocExists = errors.New("document already exists")
)

func newCollection(dbName, collectionName string, kvdb mondis.KVDB, handle *schema.Handle) *Collection {
	return &Collection{dbName: dbName, collectionName: collectionName, base: base{kvdb: kvdb, handle: handle}}
}

// Index for find an index by name
func (c *Collection) Index(name string) (idx *Index, err error) {
	schemaCache := c.handle.Get()

	if !schemaCache.CheckIndexExists(c.dbName, c.collectionName, name) {
		err = ErrIndexNotExists
		return
	}

	idx = newIndex(c.dbName, c.collectionName, name, c.kvdb, c.handle)
	return
}

// InsertOne for insert a document into collection
func (c *Collection) InsertOne(doc bson.M, t *txn.Txn) (did int64, err error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return
	}

	insertFunc := func(t *txn.Txn) (ierr error) {

		ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
		if ci == nil {
			ierr = ErrCollectionNotExists
			return
		}

		seq := GetSequence(ci.ID)
		if seq == nil {
			ierr = ErrSequenceNotExists
			return
		}

		did, ierr = seq.Next()
		if ierr != nil {
			return
		}

		docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)

		ierr = t.Set(docKey, data, nil)
		if err != nil {
			return
		}

		t.AddCancelFunc(func() {
			seq.PutBack(did)
		})
		t.UpdatedCollections(ci.ID)
		return
	}
	if t != nil {
		err = insertFunc(t)
	} else {
		err = c.RunInNewUpdateTxn(insertFunc)
	}
	return
}

const (
	updateForUpdate int8 = iota
	updateForUpsert
	updateForInsert
)

func (c *Collection) updateOne(did int64, doc bson.M, ci *model.CollectionInfo, updateFor int8, t *txn.Txn) (existsForUpdate, isNewForUpsert bool, err error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return
	}

	docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)

	updateFunc := func(t *txn.Txn) (err error) {
		existsForUpdate, err = t.Exists(docKey)
		if err != nil {
			return
		}

		switch updateFor {
		case updateForUpdate:
			if !existsForUpdate {
				return
			}
		case updateForUpsert:
			isNewForUpsert = !existsForUpdate
		case updateForInsert:
			if existsForUpdate {
				err = ErrDocExists
				return
			}
		}

		err = t.Set(docKey, data, nil)
		if err != nil {
			return
		}

		return
	}

	if t == nil {
		err = c.RunInNewUpdateTxn(updateFunc)
	} else {
		err = updateFunc(t)
	}

	return
}
