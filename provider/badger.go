package provider

import (
	"github.com/dgraph-io/badger"
	"github.com/zhiqiangxu/mondis"
)

// Badger is mondis provider for badger
type Badger struct {
	db *badger.DB
}

// NewBadger is ctor for Badger provider
func NewBadger() mondis.KVDB {
	return &Badger{}
}

// Open db
func (b *Badger) Open(option mondis.KVOption) (err error) {
	db, err := badger.Open(badger.DefaultOptions(option.Dir))
	if err != nil {
		return
	}

	b.db = db
	return
}

// Close db
func (b *Badger) Close() (err error) {
	if b.db == nil {
		return
	}
	err = b.db.Close()
	return
}

// NewTransaction creates a transaction object
func (b *Badger) NewTransaction(update bool) mondis.ProviderTxn {
	return (*Txn)(b.db.NewTransaction(update))
}

// Set kv
func (b *Badger) Set(k, v []byte, meta *mondis.VMetaReq) (err error) {
	txn := (*Txn)(b.db.NewTransaction(true))
	defer txn.Discard()

	err = txn.Set(k, v, meta)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// Exists checks whether k exists
func (b *Badger) Exists(k []byte) (exists bool, err error) {
	txn := (*Txn)(b.db.NewTransaction(false))
	defer txn.Discard()

	exists, err = txn.Exists(k)
	return
}

// Get v by k
func (b *Badger) Get(k []byte) (v []byte, meta mondis.VMetaResp, err error) {
	txn := (*Txn)(b.db.NewTransaction(false))
	defer txn.Discard()

	v, meta, err = txn.Get(k)
	return
}

// Delete k
func (b *Badger) Delete(key []byte) (err error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	err = txn.Delete(key)
	if err != nil {
		return
	}
	err = txn.Commit()
	return
}

// Scan over keys specified by option
func (b *Badger) Scan(option mondis.ProviderScanOption, fn func(key []byte, value []byte, meta mondis.VMetaResp) bool) (err error) {
	txn := (*Txn)(b.db.NewTransaction(false))
	defer txn.Discard()

	err = txn.Scan(option, fn)

	return
}

// WriteBatch creates a new mondis.ProviderWriteBatch
func (b *Badger) WriteBatch() mondis.ProviderWriteBatch {
	return (*badgerWB)(b.db.NewWriteBatch())
}

func scanByBadgerTxn(txn *badger.Txn, option mondis.ProviderScanOption, fn func(key []byte, value []byte, meta mondis.VMetaResp) bool) (err error) {
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Reverse = option.Reverse

	if len(option.Prefix) > 0 {
		iterOpts.Prefix = option.Prefix
	}

	iter := txn.NewIterator(iterOpts)
	defer iter.Close()

	if option.Offset != nil {
		iter.Seek(option.Offset)
	} else {
		iter.Rewind()
	}

	var goon bool
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()

		err = item.Value(func(val []byte) error {
			goon = fn(item.Key(), val, mondis.VMetaResp{ExpiresAt: item.ExpiresAt(), Tag: item.UserMeta()})
			return nil
		})
		if err != nil || !goon {
			break
		}
	}
	return
}
