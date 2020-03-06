package document

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/compact"
	tutil "github.com/zhiqiangxu/mondis/util"
	"github.com/zhiqiangxu/util"
	"github.com/zhiqiangxu/util/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

// Collection is like mongo collection
type Collection struct {
	db               *DB
	kvdb             mondis.KVDB
	cid              int64
	name             string
	documentSequence *Sequence
	mu               sync.RWMutex
	indexMap         map[string]IndexDefinition
}

func newCollection(db *DB, name string) (c *Collection, err error) {

	cid, err := db.getCollectionID(name)
	if err != nil {
		return
	}

	kvdb := db.kvdb
	documentSequence, err := NewSequence(kvdb, []byte(name), documentIDBandWidth)
	if err != nil {
		return
	}
	c = &Collection{
		db:               db,
		kvdb:             kvdb,
		cid:              cid,
		name:             name,
		documentSequence: documentSequence,
		indexMap:         make(map[string]IndexDefinition),
	}
	indexes, err := c.getIndexes(nil)
	if err != nil {
		return
	}
	for _, index := range indexes {
		c.indexMap[index.Name] = index
	}
	return
}

// InsertOne for insert a document into collection
func (c *Collection) InsertOne(doc bson.M, txn mondis.ProviderTxn) (did int64, err error) {

	data, err := bson.Marshal(doc)
	if err != nil {
		return
	}

	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	udid, err := c.documentSequence.Next()
	if err != nil {
		return
	}

	did = int64(udid)
	docKey := EncodeCollectionDocumentKey(nil, c.cid, did)

	insertFunc := func(txn mondis.ProviderTxn) error {
		return txn.Set(docKey, data, nil)
	}

	if txn == nil {
		err = tutil.RunInNewUpdateTxn(c.kvdb, insertFunc)
	} else {
		err = insertFunc(txn)
	}

	return
}

var (
	// ErrDocNotFound when document not found
	ErrDocNotFound = errors.New("document not found")
	// ErrIndexNameEmpty when index name is empty
	ErrIndexNameEmpty = errors.New("index name cannot be empty")
	// ErrIndexFieldsEmpty when index fields are empty
	ErrIndexFieldsEmpty = errors.New("index fields cannot be empty")
	// ErrDocIDExists when document withe specified id exists
	ErrDocIDExists = errors.New("document withe specified id exists")
)

// InsertOneManaged for insert a new document with specified document id
func (c *Collection) InsertOneManaged(did int64, doc bson.M, txn mondis.ProviderTxn) (err error) {
	_, _, err = c.updateOne(did, doc, updateForInsert, txn)
	return
}

// UpdateOne for update an existing document in collection
func (c *Collection) UpdateOne(did int64, doc bson.M, txn mondis.ProviderTxn) (exists bool, err error) {
	exists, _, err = c.updateOne(did, doc, updateForUpdate, txn)
	return
}

const (
	updateForUpdate int8 = iota
	updateForUpsert
	updateForInsert
)

func (c *Collection) updateOne(did int64, doc bson.M, updateFor int8, txn mondis.ProviderTxn) (existsForUpdate, isNewForUpsert bool, err error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return
	}

	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	docKey := EncodeCollectionDocumentKey(nil, c.cid, did)

	updateFunc := func(txn mondis.ProviderTxn) (err error) {
		existsForUpdate, err = txn.Exists(docKey)
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
				err = ErrDocIDExists
				return
			}
		}

		err = txn.Set(docKey, data, nil)
		if err != nil {
			return
		}

		return
	}

	if txn == nil {
		err = tutil.RunInNewUpdateTxn(c.kvdb, updateFunc)
	} else {
		err = updateFunc(txn)
	}

	return
}

// UpsertOne for upsert an existing document in collection
func (c *Collection) UpsertOne(did int64, doc bson.M, txn mondis.ProviderTxn) (isNew bool, err error) {
	_, isNew, err = c.updateOne(did, doc, updateForUpsert, txn)
	return
}

// DeleteOne for delete a document from collection
func (c *Collection) DeleteOne(did int64, txn mondis.ProviderTxn) (err error) {

	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	docKey := EncodeCollectionDocumentKey(nil, c.cid, did)

	deleteFunc := func(txn mondis.ProviderTxn) (err error) {
		err = txn.Delete(docKey)
		return
	}

	if txn == nil {
		err = tutil.RunInNewUpdateTxn(c.kvdb, deleteFunc)
	} else {
		err = deleteFunc(txn)
	}

	return
}

// GetOne for get a document by document id
func (c *Collection) GetOne(did int64, txn mondis.ProviderTxn) (data bson.M, err error) {
	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	docKey := EncodeCollectionDocumentKey(nil, c.cid, did)
	if txn == nil {
		txn = c.kvdb.NewTransaction(false)
		defer txn.Discard()
	}
	v, _, err := txn.Get(docKey)
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

// Count for total number of documents
func (c *Collection) Count(txn mondis.ProviderTxn) (n int, err error) {
	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	if txn == nil {
		txn = c.kvdb.NewTransaction(false)
		defer txn.Discard()
	}

	collectionDocumentPrefix := AppendCollectionDocumentPrefix(nil, c.cid)
	err = txn.Scan(mondis.ProviderScanOption{Prefix: collectionDocumentPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		n++
		return true
	})
	return
}

// DeleteAll for delete all documents of a collection
func (c *Collection) DeleteAll(txn mondis.ProviderTxn) (n int, err error) {
	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	if txn != nil {
		n, err = c.deleteAllWithTxn(txn)
		return
	}

	err = tutil.RunInNewTxn(c.kvdb, func(txn mondis.ProviderTxn) error {
		newN, err := c.deleteAllWithTxn(txn)
		n += newN
		return err
	})

	return
}

func (c *Collection) deleteAllWithTxn(txn mondis.ProviderTxn) (n int, err error) {
	// committed := 0
	collectionDocumentPrefix := AppendCollectionDocumentPrefix(nil, c.cid)
	scanErr := txn.Scan(mondis.ProviderScanOption{Prefix: collectionDocumentPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		err = txn.Delete(append([]byte(nil), key...))
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

// GetMany for get many documents by document id list
func (c *Collection) GetMany(dids []int64, txn mondis.ProviderTxn) (datas []bson.M, err error) {
	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	if txn == nil {
		txn = c.kvdb.NewTransaction(false)
		defer txn.Discard()
	}

	var v []byte
	for _, did := range dids {
		docKey := EncodeCollectionDocumentKey(nil, c.cid, did)
		v, _, err = txn.Get(docKey)
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

type (
	// IndexField for index field
	IndexField struct {
		Name string
		Desc bool
	}
	// IndexOption for index option
	IndexOption struct {
		Unique bool
	}
	// IndexDefinition for index definition
	IndexDefinition struct {
		Name   string
		Fields []IndexField
		Option IndexOption
	}
)

// Clone for deep copy
func (idef *IndexDefinition) Clone() (clone IndexDefinition) {
	clone = *idef
	clone.Fields = make([]IndexField, len(idef.Fields))
	for i, field := range idef.Fields {
		clone.Fields[i] = field
	}
	return
}

// CreateIndex for collection
func (c *Collection) CreateIndex(idef IndexDefinition) (iid int64, err error) {
	if idef.Name == "" {
		err = ErrIndexNameEmpty
		return
	}
	if len(idef.Fields) == 0 {
		err = ErrIndexFieldsEmpty
		return
	}

	idefBytes, err := bson.Marshal(idef)
	if err != nil {
		return
	}

	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	txn := c.kvdb.NewTransaction(true)
	defer txn.Discard()

	ciKey := EncodeCollectionColumnsIndexedKey(nil, c.cid, idef.Fields)

	v, _, err := txn.Get(ciKey)
	if v != nil {
		err = fmt.Errorf("there already exists index %s on the same columns", string(v))
		return
	}
	if err == kv.ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return
	}
	iid, err = c.db.nextIndexID()
	if err != nil {
		return
	}
	err = txn.Set(ciKey, util.Slice(idef.Name), nil)
	if err != nil {
		return
	}

	indexName2IDKey := EncodeCollectionIndexName2IDKey(nil, c.cid, idef.Name)
	txn.Set(indexName2IDKey, compact.EncodeVarint(nil, iid), nil)
	metaIndexKey := EncodeMetaIndexKey(nil, iid)
	err = txn.Set(metaIndexKey, idefBytes, nil)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}

	c.mu.Lock()
	c.indexMap[idef.Name] = idef
	c.mu.Unlock()
	return
}

func (c *Collection) getIndexes(txn mondis.ProviderTxn) (indexes []IndexDefinition, err error) {

	if txn == nil {
		txn = c.kvdb.NewTransaction(false)
		defer txn.Discard()
	}

	indexNamePrefix := AppendCollectionIndexNamePrefix(nil, c.cid)
	var (
		cid, iid            int64
		leftover, idefBytes []byte
	)
	scanErr := txn.Scan(mondis.ProviderScanOption{Prefix: indexNamePrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		cid, _, err = DecodeCollectionIndexName2IDKey(key)
		if err != nil || cid != c.cid {
			return false
		}

		leftover, iid, err = compact.DecodeVarint(value)
		if err != nil || len(leftover) > 0 {
			return false
		}

		indexKey := EncodeMetaIndexKey(nil, iid)
		idefBytes, _, err = txn.Get(indexKey)
		if err != nil {
			return false
		}

		var idef IndexDefinition
		err = bson.Unmarshal(idefBytes, &idef)
		if err != nil {
			return false
		}
		indexes = append(indexes, idef)
		return true
	})
	if err != nil {
		return
	}
	err = scanErr
	if err != nil {
		return
	}

	return
}

// GetIndexes returns a copy of all indexes
func (c *Collection) GetIndexes() []IndexDefinition {

	c.mu.RLock()
	ret := make([]IndexDefinition, 0, len(c.indexMap))
	for _, index := range c.indexMap {
		ret = append(ret, index.Clone())
	}
	c.mu.RUnlock()

	return ret
}

// DropIndex for collection
func (c *Collection) DropIndex(iname string) (exists bool, err error) {
	if iname == "" {
		err = ErrIndexNameEmpty
		return
	}

	// prologue start
	err = c.db.checkState()
	if err != nil {
		return
	}
	err = c.db.closer.Add(1)
	if err != nil {
		return
	}
	defer c.db.closer.Done()
	// prologue end

	txn := c.kvdb.NewTransaction(true)
	defer txn.Discard()

	indexName2IDKey := EncodeCollectionIndexName2IDKey(nil, c.cid, iname)
	iidv, _, err := txn.Get(indexName2IDKey)
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}

	_, iid, err := compact.DecodeVarint(iidv)
	if err != nil {
		return
	}

	metaIndexKey := EncodeMetaIndexKey(nil, iid)
	idefBytes, _, err := txn.Get(metaIndexKey)
	if err != nil {
		return
	}

	var idef IndexDefinition
	err = bson.Unmarshal(idefBytes, &idef)
	if err != nil {
		return
	}

	exists = true
	ciKey := EncodeCollectionColumnsIndexedKey(nil, c.cid, idef.Fields)
	err = txn.Delete(ciKey)
	if err != nil {
		return
	}
	err = txn.Delete(metaIndexKey)
	if err != nil {
		return
	}
	err = txn.Delete(indexName2IDKey)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}

	c.mu.Lock()
	delete(c.indexMap, idef.Name)
	c.mu.Unlock()
	return
}

func (c *Collection) close() {
	err := c.documentSequence.ReleaseRemaining()
	if err != nil {
		logger.Instance().Error("documentSequence.ReleaseRemaining", zap.Error(err))
	}
}
