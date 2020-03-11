package structure

import "github.com/zhiqiangxu/mondis/kv"

// Set the string value of the key.
func (t *TxStructure) Set(key []byte, value []byte) error {
	ek := t.encodeStringDataKey(key)
	return t.txn.Set(ek, value, nil)
}

// Get the string value of a key.
func (t *TxStructure) Get(key []byte) (value []byte, err error) {
	ek := t.encodeStringDataKey(key)
	value, _, err = t.txn.Get(ek)
	return
}

// Clear a string value key.
func (t *TxStructure) Clear(key []byte) (err error) {
	ek := t.encodeStringDataKey(key)
	err = t.txn.Delete(ek)
	return
}

// GetInt64 will try to parse value into int64.
func (t *TxStructure) GetInt64(key []byte) (n int64, err error) {
	ek := t.encodeStringDataKey(key)

	n, err = kv.GetInt64(t.txn, ek)
	return
}

// SetInt64 will set int64 value for key.
func (t *TxStructure) SetInt64(key []byte, value int64) (err error) {
	ek := t.encodeStringDataKey(key)

	err = kv.SetInt64(t.txn, ek, value)
	return
}

// Inc increments the integer value of a key by step, returns
// the value after the increment.
func (t *TxStructure) Inc(key []byte, step int64) (n int64, err error) {
	ek := t.encodeStringDataKey(key)
	// txn Inc will lock this key, so we don't lock it here.
	n, err = kv.IncInt64(t.txn, ek, step)
	return
}
