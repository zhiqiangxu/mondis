package structure

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
)

var (
	// ErrInvalidHashDataKey used by TxStructure
	ErrInvalidHashDataKey = errors.New("invalid hash data key")
	// ErrKeyHasNoPrefix used by TxStructure
	ErrKeyHasNoPrefix = errors.New("key has no prefix")
	// ErrInvalidListMetaData used by TxStructure
	ErrInvalidListMetaData = errors.New("invalid list meta data")
	// ErrListIndexOutOfRange used by TxStructure
	ErrListIndexOutOfRange = errors.New("list index out of range")
)

// TxStructure supports some simple data structures like list
type TxStructure struct {
	txn    mondis.ProviderTxn
	prefix []byte
}

// New is ctor for TxStructure
func New(txn mondis.ProviderTxn, prefix []byte) *TxStructure {
	return &TxStructure{txn: txn, prefix: prefix}
}
