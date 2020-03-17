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
	t                  mondis.ProviderTxn
}

// NewTxn is ctor for Txn
func NewTxn(schemaVersion int64, update bool, kvdb mondis.KVDB) *Txn {
	t := kvdb.NewTransaction(update)
	return &Txn{startSchemaVersion: schemaVersion, t: t}
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

	ok, err := txn.handle.Check(context.Background(), txn.startSchemaVersion)
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
