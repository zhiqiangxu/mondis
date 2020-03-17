package txn

import (
	"context"
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// Txn for document db
type Txn struct {
	handle             *schema.Handle
	startSchemaVersion int64
	update             bool
	t                  mondis.ProviderTxn
	updatedCollections map[int64]struct{}
}

// NewTxn is ctor for Txn
func NewTxn(schemaVersion int64, update bool, kvdb mondis.KVDB) *Txn {
	t := kvdb.NewTransaction(update)
	return &Txn{startSchemaVersion: schemaVersion, update: update, t: t}
}

var (
	// ErrDDLConflict used by Txn
	ErrDDLConflict = errors.New("ddl conflict")
)

// Discard Txn
func (txn *Txn) Discard() {
	txn.t.Discard()
}

// Commit Txn
func (txn *Txn) Commit() (err error) {

	if !txn.update {
		return
	}

	ok, err := txn.handle.Check(context.Background(), txn.startSchemaVersion, txn.updatedCollections)
	if err != nil {
		return
	}

	if !ok {
		err = ErrDDLConflict
		return
	}

	err = txn.t.Commit()
	return
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
