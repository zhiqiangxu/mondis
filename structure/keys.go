package structure

import (
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/memcomparable"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag byte

const (
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func (t *TxStructure) encodeListMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+24)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint64(ek, uint64(ListMeta))
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+len(key)+36)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	ek = memcomparable.EncodeUint64(ek, uint64(ListData))
	return memcomparable.EncodeInt64(ek, index)
}
