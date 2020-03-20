package dml

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/mondis/document/txn"
	"github.com/zhiqiangxu/mondis/kv"
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

	origT := t

	insertFunc := func(t *txn.Txn) (ierr error) {
		ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
		if ci == nil {
			err = ErrCollectionNotExists
			return
		}
		if origT != nil {
			origT.ReferredCollections(ci.ID)
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
		return
	}

	if t != nil {
		err = insertFunc(t)
	} else {
		err = c.RunInNewUpdateTxn(insertFunc)
	}
	return
}

// InsertOneManaged for insert a new document with specified document id
func (c *Collection) InsertOneManaged(did int64, doc bson.M, t *txn.Txn) (err error) {

	_, _, err = c.updateOne(did, doc, updateForInsert, t)
	return
}

// UpdateOne for update an existing document in collection
func (c *Collection) UpdateOne(did int64, doc bson.M, t *txn.Txn) (exists bool, err error) {

	exists, _, err = c.updateOne(did, doc, updateForUpdate, t)
	return
}

// UpsertOne for upsert an existing document in collection
func (c *Collection) UpsertOne(did int64, doc bson.M, t *txn.Txn) (isNew bool, err error) {

	_, isNew, err = c.updateOne(did, doc, updateForUpsert, t)
	return
}

// DeleteOne for delete a document from collection
func (c *Collection) DeleteOne(did int64, t *txn.Txn) (err error) {

	origT := t

	deleteFunc := func(t *txn.Txn) (err error) {
		ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
		if ci == nil {
			err = ErrCollectionNotExists
			return
		}

		if origT != nil {
			origT.ReferredCollections(ci.ID)
		}

		docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)
		err = t.Delete(docKey)
		return
	}

	if t == nil {
		err = c.RunInNewUpdateTxn(deleteFunc)
	} else {
		err = deleteFunc(t)
	}
	return
}

const (
	updateForUpdate int8 = iota
	updateForUpsert
	updateForInsert
)

func (c *Collection) updateOne(did int64, doc bson.M, updateFor int8, t *txn.Txn) (existsForUpdate, isNewForUpsert bool, err error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return
	}

	origT := t

	updateFunc := func(t *txn.Txn) (err error) {
		ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
		if ci == nil {
			err = ErrCollectionNotExists
			return
		}

		if origT != nil {
			origT.ReferredCollections(ci.ID)
		}

		docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)

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

// GetOne for get a document by document id
func (c *Collection) GetOne(did int64, t *txn.Txn) (data bson.M, err error) {

	origT := t

	if t == nil {
		t = c.Txn(false)
		defer t.Discard()
	}

	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if origT != nil {
		origT.ReferredCollections(ci.ID)
	}

	docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)
	v, _, err := t.Get(docKey)
	if err == kv.ErrKeyNotFound {
		err = ErrDocNotFound
		return
	}
	if err != nil {
		return
	}

	err = bson.Unmarshal(v, &data)
	return
}

// GetMany for get many documents by document id list
func (c *Collection) GetMany(dids []int64, t *txn.Txn) (datas []bson.M, err error) {

	origT := t

	if t == nil {
		t = c.Txn(false)
		defer t.Discard()
	}

	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if origT != nil {
		origT.ReferredCollections(ci.ID)
	}

	var v []byte
	for _, did := range dids {
		docKey := EncodeCollectionDocumentKey(nil, ci.ID, did)
		v, _, err = t.Get(docKey)
		if err == kv.ErrKeyNotFound {
			err = ErrDocNotFound
			return
		}
		if err != nil {
			return
		}
		var data bson.M
		err = bson.Unmarshal(v, &data)
		if err != nil {
			return
		}

		datas = append(datas, data)
	}

	return
}

// GetDidRange return doc id range
func (c *Collection) GetDidRange(t *txn.Txn) (min, max int64, err error) {

	origT := t

	if t == nil {
		t = c.Txn(false)
		defer t.Discard()
	}

	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if origT != nil {
		origT.ReferredCollections(ci.ID)
	}

	collectionDocumentPrefix := AppendCollectionDocumentPrefix(nil, ci.ID)
	scanErr := t.Scan(mondis.ProviderScanOption{Offset: collectionDocumentPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		_, min, err = DecodeCollectionDocumentKey(key)
		return false
	})
	if err != nil {
		return
	}
	if scanErr != nil {
		err = scanErr
		return
	}
	scanErr = t.Scan(mondis.ProviderScanOption{Reverse: true, Offset: collectionDocumentPrefix.PrefixNext()}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		_, max, err = DecodeCollectionDocumentKey(key)
		return false
	})
	if err != nil {
		return
	}
	if scanErr != nil {
		err = scanErr
		return
	}

	return
}

// Count for total number of documents
func (c *Collection) Count(t *txn.Txn) (n int, err error) {

	origT := t

	if t == nil {
		t = c.Txn(false)
		defer t.Discard()
	}

	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if origT != nil {
		origT.ReferredCollections(ci.ID)
	}

	collectionDocumentPrefix := AppendCollectionDocumentPrefix(nil, ci.ID)
	err = t.Scan(mondis.ProviderScanOption{Prefix: collectionDocumentPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		n++
		return true
	})
	return
}

// DeleteAll for delete all documents of a collection
func (c *Collection) DeleteAll(t *txn.Txn) (n int, err error) {

	if t == nil {
		err = c.RunInNewUpdateTxn(func(t *txn.Txn) error {
			n, err = c.deleteAllWithTxn(t, false)
			return err
		})
	} else {
		n, err = c.deleteAllWithTxn(t, true)
	}

	return
}

func (c *Collection) deleteAllWithTxn(t *txn.Txn, mark bool) (n int, err error) {
	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if mark {
		t.ReferredCollections(ci.ID)
	}

	collectionDocumentPrefix := AppendCollectionDocumentPrefix(nil, ci.ID)
	scanErr := t.Scan(mondis.ProviderScanOption{Prefix: collectionDocumentPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		err = t.Delete(append([]byte(nil), key...))
		if err != nil {
			return false
		}
		n++
		return true
	})
	if err != nil {
		return
	}
	err = scanErr

	return
}

// GetIndices returns a copy of all indexes
func (c *Collection) GetIndices(t *txn.Txn) (iifs []*model.IndexInfo, err error) {
	origT := t

	if t == nil {
		t = c.Txn(false)
		defer t.Discard()
	}

	ci := t.StartMetaCache().CollectionInfo(c.dbName, c.collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}

	if origT != nil {
		origT.ReferredCollections(ci.ID)
	}

	iifs = make([]*model.IndexInfo, 0, len(ci.Indices))
	for _, iif := range ci.Indices {
		iifs = append(iifs, iif.Clone())
	}

	return
}
