package provider

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv"
)

// LevelDB is mondis provider for LevelDB
type LevelDB struct {
	db *leveldb.DB
}

// NewLevelDB is ctor for LevelDB provider
func NewLevelDB() mondis.KVDB {
	return &LevelDB{}
}

// Open db
func (l *LevelDB) Open(option mondis.KVOption) (err error) {
	db, err := leveldb.OpenFile(option.Dir, nil)
	if err != nil {
		return
	}

	l.db = db
	return
}

// Close db
func (l *LevelDB) Close() (err error) {
	if l.db == nil {
		return
	}
	err = l.db.Close()
	return
}

// NewTransaction creates a transaction object
func (l *LevelDB) NewTransaction(update bool) mondis.ProviderTxn {
	panic("transaction not supported for leveldb")
}

// Set kv
func (l *LevelDB) Set(k, v []byte, meta *mondis.VMetaReq) (err error) {
	if meta != nil {
		err = fmt.Errorf("meta not supported for LevelDB")
		return
	}
	err = l.db.Put(k, v, nil)
	return
}

// Exists checks whether k exists
func (l *LevelDB) Exists(k []byte) (exists bool, err error) {
	exists, err = l.db.Has(k, nil)

	return
}

// Get v by k
func (l *LevelDB) Get(k []byte) (v []byte, meta mondis.VMetaResp, err error) {

	v, err = l.db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		err = kv.ErrKeyNotFound
	}

	// keep behaviour the same as badger
	if len(v) == 0 {
		v = nil
	}
	return
}

// Delete k
func (l *LevelDB) Delete(key []byte) (err error) {
	err = l.db.Delete(key, nil)
	return
}

var emptyMeta mondis.VMetaResp

// Scan over keys specified by option
func (l *LevelDB) Scan(option mondis.ProviderScanOption, fn func(key []byte, value []byte, meta mondis.VMetaResp) bool) (err error) {

	if option.Reverse {
		err = fmt.Errorf("Reverse scan not supported for LevelDB")
		return
	}

	var slice *util.Range
	if option.Prefix != nil {
		slice = util.BytesPrefix(option.Prefix)
	}
	iter := l.db.NewIterator(slice, nil)
	if option.Offset != nil {
		if !iter.Seek(option.Offset) {
			return
		}
		if !fn(iter.Key(), iter.Value(), emptyMeta) {
			return
		}
	}

	for {
		if !iter.Next() {
			break
		}
		if !fn(iter.Key(), iter.Value(), emptyMeta) {
			break
		}
	}

	return
}
