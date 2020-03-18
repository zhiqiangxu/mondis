package txn

import (
	"context"
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// Txn for document db
type Txn struct {
	mondis.ProviderTxn
	handle             *schema.Handle
	startMetaCache     *schema.MetaCache
	update             bool
	updatedCollections map[int64]struct{}
}

// NewTxn is ctor for Txn
func NewTxn(startMetaCache *schema.MetaCache, update bool, kvdb mondis.KVDB) *Txn {
	t := kvdb.NewTransaction(update)
	return &Txn{startMetaCache: startMetaCache, update: update, ProviderTxn: t}
}

var (
	// ErrDDLConflict used by Txn
	ErrDDLConflict = errors.New("ddl conflict")
)

// Discard Txn
func (txn *Txn) Discard() {
	txn.ProviderTxn.Discard()
}

// Commit Txn
func (txn *Txn) Commit() (err error) {

	if !txn.update {
		return
	}

	ok, err := txn.handle.Check(context.Background(), txn.startMetaCache, txn.updatedCollections)
	if err != nil {
		return
	}

	if !ok {
		err = ErrDDLConflict
		return
	}

	err = txn.ProviderTxn.Commit()
	return
}

// StartMetaCache returns startMetaCache
func (txn *Txn) StartMetaCache() *schema.MetaCache {
	return txn.startMetaCache
}

// UpdatedCollections for storing updated collections before commit
func (txn *Txn) UpdatedCollections(collectionIDs ...int64) {
	if txn.updatedCollections == nil {
		txn.updatedCollections = make(map[int64]struct{})
	}

	for _, collectionID := range collectionIDs {
		txn.updatedCollections[collectionID] = struct{}{}
	}
}
