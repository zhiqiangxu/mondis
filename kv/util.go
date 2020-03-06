package kv

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv/numeric"
)

// IncInt64 increases the value for key k in kv store by step.
func IncInt64(txn mondis.ProviderTxn, k Key, step int64) (n int64, err error) {
	v, _, err := txn.Get(k)
	if err == ErrKeyNotFound {
		err = txn.Set(k, numeric.Encode2Human(step), nil)
		if err != nil {
			return
		}
		n = step
		return
	}
	if err != nil {
		return
	}

	n, err = numeric.DecodeFromHuman(v)
	if err != nil {
		return
	}

	n += step
	if err = txn.Set(k, numeric.Encode2Human(n), nil); err != nil {
		return
	}
	return
}

// GetInt64 get int64 value which created by IncInt64 method.
func GetInt64(txn mondis.ProviderTxn, k Key) (n int64, err error) {
	v, _, err := txn.Get(k)
	if err != nil {
		return
	}

	n, err = numeric.DecodeFromHuman(v)
	return
}
