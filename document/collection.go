package document

import (
	"github.com/zhiqiangxu/kvrpc"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	basePrefix                = "_kvrpc_"
	collectionPrefix          = basePrefix + "_c"
	documentPrefix            = "_r"
	indexPrefix               = "_i"
	collectionDocumentPrefix  = collectionPrefix + documentPrefix
	collectionIndexPrefix     = collectionPrefix + indexPrefix
	metaPrefix                = basePrefix + "m"
	sequencePrefix            = "_s"
	metaSequencePrefix        = metaPrefix + sequencePrefix
	reservedKeywordCollection = "collection"
	collectionIDBandWidth     = 50
	documentIDBandWidth       = 1000
)

// Collection is like mongo collection
type Collection struct {
	kvdb     kvrpc.KVDB
	cid      int64
	name     string
	sequence *Sequence
}

// NewCollection is ctor for Collection
func NewCollection(kvdb kvrpc.KVDB, name string) (c *Collection, err error) {

	sequence, _ := NewSequence(kvdb, []byte(name), documentIDBandWidth)
	c = &Collection{kvdb: kvdb, name: name, sequence: sequence}
	return
}

// InsertOne for insert a document into collection
func (c *Collection) InsertOne(doc interface{}) (id int64, err error) {
	bson.Marshal(doc)
	return
}
