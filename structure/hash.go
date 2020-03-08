package structure

import (
	"bytes"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/numeric"
)

// HashPair is the pair for (field, value) in a hash.
type HashPair struct {
	Field []byte
	Value []byte
}

type hashMeta struct {
	FieldCount int64
}

func (meta hashMeta) Value() []byte {
	return numeric.Encode2Binary(uint64(meta.FieldCount), nil)
}

func (meta hashMeta) IsEmpty() bool {
	return meta.FieldCount <= 0
}

// HSet sets the string value of a hash field.
func (t *TxStructure) HSet(key []byte, field []byte, value []byte) error {
	return t.updateHash(key, field, func([]byte) ([]byte, error) {
		return value, nil
	})
}

// HGet gets the value of a hash field.
func (t *TxStructure) HGet(key []byte, field []byte) (value []byte, err error) {
	dataKey := t.encodeHashDataKey(key, field)
	value, _, err = t.txn.Get(dataKey)
	return
}

func (t *TxStructure) hashFieldIntegerVal(val int64) []byte {
	return numeric.Encode2Human(val)
}

// HInc increments the integer value of a hash field, by step, returns
// the value after the increment.
func (t *TxStructure) HInc(key []byte, field []byte, step int64) (n int64, err error) {
	err = t.updateHash(key, field, func(oldValue []byte) ([]byte, error) {
		if oldValue != nil {
			var err error
			n, err = numeric.DecodeFromHuman(oldValue)
			if err != nil {
				return nil, err
			}
		}
		n += step
		return t.hashFieldIntegerVal(n), nil
	})

	return
}

// HGetInt64 gets int64 value of a hash field.
func (t *TxStructure) HGetInt64(key []byte, field []byte) (n int64, err error) {
	value, err := t.HGet(key, field)

	if err != nil {
		return
	}

	n, err = numeric.DecodeFromHuman(value)
	return
}

// HLen gets the number of fields in a hash.
func (t *TxStructure) HLen(key []byte) (l int64, err error) {
	metaKey := t.encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return
	}
	l = meta.FieldCount
	return
}

// HDel deletes one or more hash fields.
func (t *TxStructure) HDel(key []byte, fields ...[]byte) (err error) {
	metaKey := t.encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	for _, field := range fields {
		dataKey := t.encodeHashDataKey(key, field)

		_, err = t.loadHashValue(dataKey)
		if err == kv.ErrKeyNotFound {
			err = nil
			continue
		}
		if err != nil {
			return
		}

		if err = t.txn.Delete(dataKey); err != nil {
			return
		}

		meta.FieldCount--

	}

	if meta.IsEmpty() {
		err = t.txn.Delete(metaKey)
	} else {
		err = t.txn.Set(metaKey, meta.Value(), nil)
	}
	return
}

// HKeys gets all the fields in a hash.
func (t *TxStructure) HKeys(key []byte) (keys [][]byte, err error) {
	err = t.iterateHash(key, func(field []byte, value []byte) bool {
		keys = append(keys, append([]byte{}, field...))
		return true
	})
	return
}

// HGetN gets N fields and values in hash in ascending order.
func (t *TxStructure) HGetN(key []byte, n int) (res []HashPair, err error) {
	if n <= 0 {
		return
	}

	err = t.iterateHash(key, func(field []byte, value []byte) bool {
		pair := HashPair{
			Field: append([]byte{}, field...),
			Value: append([]byte{}, value...),
		}
		res = append(res, pair)
		return len(res) < n
	})
	return
}

// HGetAll gets all the fields and values in a hash.
func (t *TxStructure) HGetAll(key []byte) (res []HashPair, err error) {
	err = t.iterateHash(key, func(field []byte, value []byte) bool {
		pair := HashPair{
			Field: append([]byte{}, field...),
			Value: append([]byte{}, value...),
		}
		res = append(res, pair)
		return true
	})

	return
}

// HGetNDesc gets N fields and values in hash in descending order.
func (t *TxStructure) HGetNDesc(key []byte, n int) (res []HashPair, err error) {
	if n <= 0 {
		return
	}

	err = t.iterateReverseHash(key, func(field []byte, value []byte) bool {
		pair := HashPair{
			Field: append([]byte{}, field...),
			Value: append([]byte{}, value...),
		}
		res = append(res, pair)
		return len(res) < n
	})
	return
}

// HClear removes the hash value of the key.
func (t *TxStructure) HClear(key []byte) (err error) {
	metaKey := t.encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil || meta.IsEmpty() {
		return
	}

	itErr := t.iterateHash(key, func(field []byte, value []byte) bool {
		k := t.encodeHashDataKey(key, field)
		err = t.txn.Delete(k)
		return err == nil
	})
	if err != nil {
		return
	}
	if itErr != nil {
		err = itErr
		return
	}

	err = t.txn.Delete(metaKey)
	return
}

func (t *TxStructure) iterateReverseHash(key []byte, fn func(k []byte, v []byte) bool) (err error) {
	dataPrefix := t.hashDataKeyPrefix(key)

	var field []byte
	scanErr := t.txn.Scan(mondis.ProviderScanOption{Reverse: true, Offset: dataPrefix.PrefixNext()}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		if !bytes.HasPrefix(key, dataPrefix) {
			return false
		}

		_, field, err = t.decodeHashDataKey(key)
		if err != nil {
			return false
		}

		if !fn(field, value) {
			return false
		}

		return true
	})
	if err == nil {
		err = scanErr
	}
	return
}

func (t *TxStructure) iterateHash(key []byte, fn func(k []byte, v []byte) bool) (err error) {
	dataPrefix := t.hashDataKeyPrefix(key)

	var field []byte
	scanErr := t.txn.Scan(mondis.ProviderScanOption{Prefix: dataPrefix}, func(key []byte, value []byte, _ mondis.VMetaResp) bool {
		if !bytes.HasPrefix(key, dataPrefix) {
			return false
		}

		_, field, err = t.decodeHashDataKey(key)
		if err != nil {
			return false
		}

		if !fn(field, value) {
			return false
		}

		return true
	})
	if err == nil {
		err = scanErr
	}
	return
}

func (t *TxStructure) updateHash(key []byte, field []byte, fn func(oldValue []byte) ([]byte, error)) (err error) {
	dataKey := t.encodeHashDataKey(key, field)
	oldValue, err := t.loadHashValue(dataKey)

	var isNew bool
	if err == kv.ErrKeyNotFound {
		isNew = true
		err = nil
	}
	if err != nil {
		return
	}

	newValue, err := fn(oldValue)
	if err != nil {
		return
	}

	// Check if new value is equal to old value.
	if bytes.Equal(oldValue, newValue) {
		return nil
	}

	if err = t.txn.Set(dataKey, newValue, nil); err != nil {
		return
	}

	if !isNew {
		return
	}

	metaKey := t.encodeHashMetaKey(key)
	meta, err := t.loadHashMeta(metaKey)
	if err != nil {
		return
	}

	meta.FieldCount++
	if err = t.txn.Set(metaKey, meta.Value(), nil); err != nil {
		return
	}

	return
}

func (t *TxStructure) loadHashMeta(metaKey []byte) (m hashMeta, err error) {
	v, _, err := t.txn.Get(metaKey)
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	uFieldCount, err := numeric.DecodeFromBinary(v)
	if err != nil {
		return
	}
	m.FieldCount = int64(uFieldCount)
	return
}

func (t *TxStructure) loadHashValue(dataKey []byte) (v []byte, err error) {
	v, _, err = t.txn.Get(dataKey)

	return
}
