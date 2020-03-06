package structure

import (
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/memcomparable"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag uint8

const (
	// StringData is the flag for string data.
	StringData TypeFlag = 's'
	// HashMeta is the flag for hash meta.
	HashMeta TypeFlag = 'H'
	// HashData is the flag for hash data.
	HashData TypeFlag = 'h'
	// ListMeta is the flag for list meta.
	ListMeta TypeFlag = 'L'
	// ListData is the flag for list data.
	ListData TypeFlag = 'l'
)

func (t *TxStructure) encodeStringDataKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(StringData))
}

func (t *TxStructure) encodeHashMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(HashMeta))
}

func (t *TxStructure) hashDataKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(HashData))
}

func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+1+memcomparable.EncodedBytesLength(len(field)))
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	ek = memcomparable.EncodeUint8(ek, uint8(HashData))
	return memcomparable.EncodeBytes(ek, field)
}

func (t *TxStructure) decodeHashDataKey(ek kv.Key) (key, field []byte, err error) {
	if !ek.HasPrefix(t.prefix) {
		err = ErrKeyHasNoPrefix
		return
	}

	ek = ek[len(t.prefix):]

	ek, key, err = memcomparable.DecodeBytes(ek, nil)
	if err != nil {
		return
	}
	ek, tp, err := memcomparable.DecodeUint8(ek)
	if err != nil {
		return
	}

	if tp != uint8(HashData) {
		err = ErrInvalidHashDataKey
		return
	}

	_, field, err = memcomparable.DecodeBytes(ek, nil)
	return
}

func (t *TxStructure) encodeListMetaKey(key []byte) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+1)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	return memcomparable.EncodeUint8(ek, uint8(ListMeta))
}

func (t *TxStructure) encodeListDataKey(key []byte, index int64) kv.Key {
	ek := make([]byte, 0, len(t.prefix)+memcomparable.EncodedBytesLength(len(key))+9)
	ek = append(ek, t.prefix...)
	ek = memcomparable.EncodeBytes(ek, key)
	ek = memcomparable.EncodeUint8(ek, uint8(ListData))
	return memcomparable.EncodeInt64(ek, index)
}
