package provider

import (
	"github.com/dgraph-io/badger"
	"github.com/zhiqiangxu/kvrpc"
)

// Badger is kvrpc provider for badger
type Badger struct {
	db *badger.DB
}

// NewBadger is ctor for Badger provider
func NewBadger() kvrpc.KVDB {
	return &Badger{}
}

// Open db
func (b *Badger) Open(option kvrpc.KVOption) (err error) {
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
func (b *Badger) NewTransaction(update bool) kvrpc.ProviderTxn {
	return (*Txn)(b.db.NewTransaction(update))
}

// Set kv
func (b *Badger) Set(k, v []byte, meta *kvrpc.VMetaReq) (err error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	if meta == nil {
		err = txn.Set(k, v)
	} else {
		entry := badger.NewEntry(k, v).WithTTL(meta.TTL).WithMeta(meta.Tag)
		err = txn.SetEntry(entry)
	}
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// Exists checks whether k exists
func (b *Badger) Exists(k []byte) (exists bool, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	_, err = txn.Get(k)
	if err == badger.ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	exists = true
	return
}

// Get v by k
func (b *Badger) Get(k []byte) (v []byte, meta kvrpc.VMetaResp, err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(k)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = ErrKeyNotFound
		}
		return
	}

	v, err = item.ValueCopy(nil)
	if err != nil {
		return
	}

	meta.Tag = item.UserMeta()
	meta.ExpiresAt = item.ExpiresAt()
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
func (b *Badger) Scan(option kvrpc.ProviderScanOption, fn func(key []byte, value []byte, meta kvrpc.VMetaResp) bool) (err error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	err = scanByBadgerTxn(txn, option, fn)

	return
}

func scanByBadgerTxn(txn *badger.Txn, option kvrpc.ProviderScanOption, fn func(key []byte, value []byte, meta kvrpc.VMetaResp) bool) (err error) {
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
			goon = fn(item.Key(), val, kvrpc.VMetaResp{ExpiresAt: item.ExpiresAt(), Tag: item.UserMeta()})
			return nil
		})
		if err != nil || !goon {
			break
		}
	}
	return
}
