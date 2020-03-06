package structure

import (
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/memcomparable"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag uint8

const (
	// HashMeta is the flag for hash meta.
	HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func (t *TxStructure) encodeHashMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(key)+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(HashMeta))
}

func (t *TxStructure) hashDataKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(key)+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(HashData))
}

func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(key)+1+memcomparable.EncodedBytesLength(field))
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	ek = memcomparable.EncodeUint8(ek, uint8(HashData))
	return memcomparable.EncodeBytes(ek, field)
}

func (t *TxStructure) decodeHashDataKey(dk kv.Key) (key, field []byte, err error) {
	if !dk.HasPrefix(t.prefix) {
		err = ErrKeyHasNoPrefix
		return
	}

	dk = dk[len(t.prefix):]

	dk, key, err = memcomparable.DecodeBytes(dk, nil)
	if err != nil {
		return
	}
	dk, tp, err := memcomparable.DecodeUint8(dk)
	if err != nil {
		return
	}

	if tp != uint8(HashData) {
		err = ErrInvalidHashDataKey
		return
	}

	_, field, err = memcomparable.DecodeBytes(dk, nil)
	return
}

func (t *TxStructure) encodeListMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(key)+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(ListMeta))
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(key)+9)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	ek = memcomparable.EncodeUint8(ek, uint8(ListData))
	return memcomparable.EncodeInt64(ek, index)
}
