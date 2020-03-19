package dml

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/mondis/document/txn"
)

type base struct {
	kvdb   mondis.KVDB
	handle *schema.Handle
}

// Txn to grab a Txn
func (b *base) Txn(update bool) *txn.Txn {
	return txn.NewTxn(b.handle, update, b.kvdb)
}

// RunInNewUpdateTxn for document db
func (b *base) RunInNewUpdateTxn(f func(*txn.Txn) error) (err error) {
	txn := b.Txn(true)
	defer txn.Discard()

	err = f(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// RunInNewTxn for document db
func (b *base) RunInNewTxn(f func(*txn.Txn) error) (err error) {
	txn := b.Txn(false)
	defer txn.Discard()

	err = f(txn)
	return
}
