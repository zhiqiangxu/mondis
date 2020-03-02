package document

import (
	"encoding/binary"
	"sync"

	"errors"
	"fmt"
	"sync/atomic"

	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/kvrpc/provider"
	"github.com/zhiqiangxu/util/closer"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// DB defines a column db
type DB struct {
	mu                 sync.RWMutex
	once               sync.Once
	state              uint32
	closer             *closer.Strict
	kvdb               kvrpc.KVDB
	collectionSequence *Sequence
	collections        map[string]*Collection
}

// NewDB is ctor for DB
func NewDB(kvdb kvrpc.KVDB) *DB {
	collectionSequence, _ := NewSequence(kvdb, []byte(reservedKeywordCollection), collectionIDBandWidth)
	return &DB{
		kvdb:               kvdb,
		collectionSequence: collectionSequence,
		collections:        make(map[string]*Collection),
		closer:             closer.NewStrict(),
	}
}

var (
	// ErrEmptyKeywordForSequence when sequence keyword is empty
	ErrEmptyKeywordForSequence = errors.New("sequence keyword cannot be empty")
	// ErrEmptyCollectionName when collection name is empty
	ErrEmptyCollectionName = errors.New("collection name cannot be empty")
	// ErrZeroBandwidth when bandwidth is zero
	ErrZeroBandwidth = errors.New("bandwidth must be greater than zero")
	// ErrCollectionNameForbiden when collection name is a reserved keyword
	ErrCollectionNameForbiden = errors.New("collection name is a reserved keyword")
)

var (
	reservedKeywordCollectionBytes = []byte(reservedKeywordCollection)
)

// Collection returns collection operator
func (db *DB) Collection(name string) (collection *Collection, err error) {
	if name == "" {
		err = ErrEmptyCollectionName
		return
	}
	if name == reservedKeywordCollection {
		err = ErrCollectionNameForbiden
		return
	}

	err = db.checkState()
	if err != nil {
		return
	}

	db.mu.RLock()
	collection = db.collections[name]
	if collection != nil {
		db.mu.RUnlock()
		return
	}
	db.mu.RUnlock()

	err = db.closer.Add(1)
	if err != nil {
		return
	}
	defer db.closer.Done()

	db.mu.Lock()
	collection = db.collections[name]
	if collection != nil {
		db.mu.Unlock()
		return
	}
	cid, err := db.getCollectionID(name)
	if err != nil {
		db.mu.Unlock()
		return
	}
	collection = newCollection(db, cid, name)
	db.collections[name] = collection
	db.mu.Unlock()

	return
}

const (
	open uint32 = iota
	closing
	closed
)

var (
	// ErrAlreadyClosed when document db already closed
	ErrAlreadyClosed = errors.New("document db already closed")
	// ErrAlreadyClosing when document db already closing
	ErrAlreadyClosing = errors.New("document db already closing")
)

func (db *DB) checkState() (err error) {
	state := atomic.LoadUint32(&db.state)
	switch state {
	case open:
	case closing:
		err = ErrAlreadyClosing
	case closed:
		err = ErrAlreadyClosed
	default:
		err = fmt.Errorf("unknown close state:%d", state)
	}
	return
}

// Close DB
func (db *DB) Close() {
	db.once.Do(func() {
		atomic.StoreUint32(&db.state, closing)

		db.closer.SignalAndWait()

		err := db.collectionSequence.ReleaseRemaining()
		if err != nil {
			logger.Instance().Error("collectionSequence.ReleaseRemaining", zap.Error(err))
		}

		for _, collection := range db.collections {
			collection.close()
		}

		atomic.StoreUint32(&db.state, closed)
	})

}

func (db *DB) getCollectionID(name string) (cid int64, err error) {

	cidKey := AppendCIDKey(nil, name)

	txn := db.kvdb.NewTransaction(true)
	defer txn.Discard()

	v, _, err := txn.Get(cidKey)
	if err != nil {
		if err == provider.ErrKeyNotFound {
			var ucid uint64
			ucid, err = db.collectionSequence.Next()
			if err != nil {
				return
			}

			var data [8]byte
			binary.BigEndian.PutUint64(data[:], ucid)
			err = txn.Set(cidKey, data[:], nil)
			if err != nil {
				return
			}
			err = txn.Commit()
			if err != nil {
				return
			}
			cid = int64(ucid)
		}
		return
	}

	cid = int64(binary.BigEndian.Uint64(v))
	return
}
