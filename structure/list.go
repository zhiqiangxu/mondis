package structure

import (
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/numeric"
)

type listMeta struct {
	LIndex int64
	RIndex int64
}

func (meta listMeta) Value() []byte {
	buf := make([]byte, 0, 16)
	buf = numeric.Encode2Binary(uint64(meta.LIndex), buf)
	buf = numeric.Encode2Binary(uint64(meta.RIndex), buf)
	return buf
}

func (meta listMeta) IsEmpty() bool {
	return meta.LIndex >= meta.RIndex
}

// This is a FILO list

// LPush prepends one or multiple values to a list.
func (t *TxStructure) LPush(key []byte, values ...[]byte) error {
	return t.listPush(key, true, values...)
}

// RPush appends one or multiple values to a list.
func (t *TxStructure) RPush(key []byte, values ...[]byte) error {
	return t.listPush(key, false, values...)
}

func (t *TxStructure) listPush(key []byte, left bool, values ...[]byte) (err error) {
	if len(values) == 0 {
		return
	}

	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil {
		return
	}

	var index int64
	for _, v := range values {
		if left {
			meta.LIndex--
			index = meta.LIndex
		} else {
			index = meta.RIndex
			meta.RIndex++
		}

		dataKey := t.encodeListDataKey(key, index)
		if err = t.txn.Set(dataKey, v, nil); err != nil {
			return
		}
	}

	err = t.txn.Set(metaKey, meta.Value(), nil)
	return
}

// LPop removes and gets the first element in a list.
func (t *TxStructure) LPop(key []byte) ([]byte, error) {
	return t.listPop(key, true)
}

// RPop removes and gets the last element in a list.
func (t *TxStructure) RPop(key []byte) ([]byte, error) {
	return t.listPop(key, false)
}

func (t *TxStructure) listPop(key []byte, left bool) (data []byte, err error) {
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)

	if err != nil || meta.IsEmpty() {
		return
	}

	var index int64
	if left {
		index = meta.LIndex
		meta.LIndex++
	} else {
		meta.RIndex--
		index = meta.RIndex
	}

	dataKey := t.encodeListDataKey(key, index)

	data, _, err = t.txn.Get(dataKey)
	if err != nil {
		return
	}

	if err = t.txn.Delete(dataKey); err != nil {
		return
	}

	if !meta.IsEmpty() {
		err = t.txn.Set(metaKey, meta.Value(), nil)
	} else {
		err = t.txn.Delete(metaKey)
	}

	return
}

// LLen gets the length of a list.
func (t *TxStructure) LLen(key []byte) (l int64, err error) {
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	l = meta.RIndex - meta.LIndex
	return
}

// LGetAll gets all elements of this list in order from right to left.
func (t *TxStructure) LGetAll(key []byte) (elements [][]byte, err error) {
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	length := int(meta.RIndex - meta.LIndex)
	elements = make([][]byte, 0, length)
	for index := meta.RIndex - 1; index >= meta.LIndex; index-- {
		e, _, getErr := t.txn.Get(t.encodeListDataKey(key, index))
		if getErr != nil {
			err = getErr
			return
		}
		elements = append(elements, e)
	}
	return
}

// LIndex gets an element from a list by its index.
func (t *TxStructure) LIndex(key []byte, index int64) (data []byte, err error) {
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	index = adjustIndex(index, meta.LIndex, meta.RIndex)

	if index >= meta.LIndex && index < meta.RIndex {
		data, _, err = t.txn.Get(t.encodeListDataKey(key, index))
	}
	return
}

// LSet updates an element in the list by its index.
func (t *TxStructure) LSet(key []byte, index int64, value []byte) (err error) {

	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	index = adjustIndex(index, meta.LIndex, meta.RIndex)

	if index >= meta.LIndex && index < meta.RIndex {
		err = t.txn.Set(t.encodeListDataKey(key, index), value, nil)
		return
	}
	err = ErrListIndexOutOfRange
	return
}

// LClear removes the list of the key.
func (t *TxStructure) LClear(key []byte) (err error) {
	metaKey := t.encodeListMetaKey(key)
	meta, err := t.loadListMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	for index := meta.LIndex; index < meta.RIndex; index++ {
		dataKey := t.encodeListDataKey(key, index)
		if err = t.txn.Delete(dataKey); err != nil {
			return
		}
	}

	err = t.txn.Delete(metaKey)
	return
}

func (t *TxStructure) loadListMeta(metaKey []byte) (m listMeta, err error) {
	v, _, err := t.txn.Get(metaKey)
	if err == kv.ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return
	}

	if v == nil {
		return
	}

	if len(v) != 16 {
		err = ErrInvalidListMetaData
		return
	}

	uLIndex, err := numeric.DecodeFromBinary(v[0:8])
	if err != nil {
		return
	}
	uRIndex, err := numeric.DecodeFromBinary(v[8:16])
	if err != nil {
		return
	}

	m.LIndex = int64(uLIndex)
	m.RIndex = int64(uRIndex)
	return
}

func adjustIndex(index int64, min, max int64) int64 {
	if index >= 0 {
		return index + min
	}

	return index + max
}
