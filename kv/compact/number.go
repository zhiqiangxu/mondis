package compact

import (
	"encoding/binary"
	"errors"
)

// EncodeVarint for encoding an int64 in a compact way
func EncodeVarint(b []byte, v int64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeVarint decodes value encoded by EncodeVarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeVarint(b []byte) ([]byte, int64, error) {
	v, n := binary.Varint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}

// EncodeUvarint for encoding a uint64 in a compact way
func EncodeUvarint(b []byte, v uint64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(data[:], v)
	return append(b, data[:n]...)
}

// DecodeUvarint decodes value encoded by EncodeUvarint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUvarint(b []byte) ([]byte, uint64, error) {
	v, n := binary.Uvarint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}
