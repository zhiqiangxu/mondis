package document

import (
	"errors"
	"sync"

	"github.com/zhiqiangxu/kvrpc"
)

// Table for crud operations on table
type Table struct {
	sync.RWMutex
	tableSequence *Sequence
	tableIDs      map[string]uint64
}

var (
	// ErrEmptyKeywordForSequence when sequence keyword is empty
	ErrEmptyKeywordForSequence = errors.New("sequence keyword cannot be empty")
	// ErrEmptyTable when table is empty
	ErrEmptyTable = errors.New("table cannot be empty")
	// ErrZeroBandwidth when bandwidth is zero
	ErrZeroBandwidth = errors.New("bandwidth must be greater than zero")
	// ErrTableNameForbiden when table name is a reserved keyword
	ErrTableNameForbiden = errors.New("table name is a reserved keyword")
)

var (
	reservedKeywordCollectionBytes = []byte(reservedKeywordCollection)
)

// New is ctor for Table
func New(kvdb kvrpc.KVDB) (t *Table) {

	tableSequence, _ := NewSequence(kvdb, reservedKeywordCollectionBytes, collectionIDBandWidth)
	t = &Table{tableSequence: tableSequence, tableIDs: make(map[string]uint64)}
	return
}

// GetTableID get the id for table
func (t *Table) GetTableID(table string) (id uint64, err error) {
	t.RLock()
	id, ok := t.tableIDs[table]
	t.RUnlock()
	if ok {
		return
	}

	t.Lock()
	if id, ok = t.tableIDs[table]; ok {
		t.Unlock()
		return
	}

	id, err = t.tableSequence.Next()
	if err != nil {
		t.Unlock()
		return
	}
	t.tableIDs[table] = id

	t.Unlock()
	return
}

func (t *Table) recordKey(table string, pk uint64) (key []byte, err error) {
	if table == reservedKeywordCollection {
		err = ErrTableNameForbiden
		return
	}

	tableID, err := t.GetTableID(table)
	if err != nil {
		return
	}

	key = documentKey(tableID, pk)
	return
}

// Insert a row into table
func (t *Table) Insert(txn kvrpc.ProviderTxn, table string, pk uint64, column string, value []byte) (insertID uint64, err error) {
	return
}

// Update a row of table
func (t *Table) Update(txn kvrpc.ProviderTxn, table string, pk uint64, data []byte) (err error) {
	return
}

// Upsert a row into table
func (t *Table) Upsert(txn kvrpc.ProviderTxn, table string, pk uint64, data []byte) (err error) {
	return
}

// Delete a row from table
func (t *Table) Delete(txn kvrpc.ProviderTxn, table string, pk uint64) (err error) {
	return
}

// Get a row from table
func (t *Table) Get(txn kvrpc.ProviderTxn, table string, pk uint64) (data []byte, err error) {
	return
}

// Query for table rows
func (t *Table) Query(txn kvrpc.ProviderTxn, table string) (err error) {
	return
}

// Close for final housekeeping
func (t *Table) Close() (err error) {
	err = t.tableSequence.ReleaseRemaining()
	return
}
