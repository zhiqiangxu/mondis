package document

import (
	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

// Collection is like mongo collection
type Collection struct {
	db               *DB
	kvdb             kvrpc.KVDB
	cid              int64
	name             string
	documentSequence *Sequence
}

func newCollection(db *DB, cid int64, name string) (c *Collection) {

	kvdb := db.kvdb
	documentSequence, _ := NewSequence(kvdb, []byte(name), documentIDBandWidth)
	c = &Collection{db: db, kvdb: kvdb, cid: cid, name: name, documentSequence: documentSequence}
	return
}

// InsertOne for insert a document into collection
func (c *Collection) InsertOne(txn kvrpc.ProviderTxn, doc bson.M) (did int64, err error) {

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
	docKey := GetCollectionDocumentKey(c.cid, int64(udid))

	err = txn.Set(docKey, data, nil)
	if err != nil {
		return
	}

	did = int64(udid)
	return
}

// UpdateOne for update an existing document in collection
func (c *Collection) UpdateOne(txn kvrpc.ProviderTxn, did int64, doc bson.M) (err error) {
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

	return
}

func (c *Collection) close() {
	err := c.documentSequence.ReleaseRemaining()
	if err != nil {
		logger.Instance().Error("documentSequence.ReleaseRemaining", zap.Error(err))
	}
}
