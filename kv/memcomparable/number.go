package memcomparable

import (
	"encoding/binary"
	"errors"
)

const signMask uint64 = 0x8000000000000000

// EncodeInt64ToUint64 make int64 v to comparable uint64 type
func EncodeInt64ToUint64(v int64) uint64 {
	return uint64(v) ^ signMask
}

// DecodeUint64ToInt64 decodes the u that encoded by EncodeInt64ToUint64
func DecodeUint64ToInt64(u uint64) int64 {
	return int64(u ^ signMask)
}

// EncodeInt64 for convert int64 to memcomparable-format
func EncodeInt64(b []byte, v int64) []byte {
	u := EncodeInt64ToUint64(v)
	return EncodeUint64(b, u)
}

// DecodeInt64 is reverse for EncodeInt64
func DecodeInt64(b []byte) (leftover []byte, v int64, err error) {
	leftover, u, err := DecodeUint64(b)
	if err == nil {
		v = DecodeUint64ToInt64(u)
	}

	return
}

// EncodeInt64Desc for encoding int64 in descending order
func EncodeInt64Desc(b []byte, v int64) []byte {
	u := EncodeInt64ToUint64(v)
	return EncodeUint64(b, ^u)
}

// DecodeInt64Desc is reverse for EncodeInt64Desc
func DecodeInt64Desc(b []byte) (leftover []byte, v int64, err error) {
	leftover, u, err := DecodeUint64(b)
	if err == nil {
		v = DecodeUint64ToInt64(^u)
	}
	return
}

// EncodeUint64 for convert uint64 to memcomparable-format
func EncodeUint64(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], v)
	return append(b, data[:]...)
}

// EncodeUint64Desc bitwise reverses v before EncodeUint64
func EncodeUint64Desc(b []byte, v uint64) []byte {
	return EncodeUint64(b, ^v)
}

// EncodeUint8 for convert uint8 to memcomparable-format
func EncodeUint8(b []byte, v uint8) []byte {
	return append(b, v)
}

// EncodeUint8Desc bitwise reverses v before EncodeUint8
func EncodeUint8Desc(b []byte, v uint8) []byte {
	return EncodeUint8(b, ^v)
}

// DecodeUint8 is reverse for EncodeUint8
func DecodeUint8(b []byte) (leftover []byte, v uint8, err error) {
	if len(b) < 1 {
		err = ErrInsufficientBytesToDecode
		return
	}

	v = uint8(b[0])
	leftover = b[1:]
	return
}

// DecodeUint8Desc is reverse for EncodeUint8Desc
func DecodeUint8Desc(b []byte) (leftover []byte, v uint8, err error) {
	leftover, v, err = DecodeUint8(b)
	if err == nil {
		v = ^v
	}
	return
}

var (
	// ErrInsufficientBytesToDecode when insufficient bytes to decode value
	ErrInsufficientBytesToDecode = errors.New("insufficient bytes to decode value")
)

// DecodeUint64 is reverse for EncodeUint64
func DecodeUint64(b []byte) (leftover []byte, v uint64, err error) {
	if len(b) < 8 {
		err = ErrInsufficientBytesToDecode
		return
	}

	v = binary.BigEndian.Uint64(b[:8])
	leftover = b[8:]
	return
}

// DecodeUint64Desc is reverse for EncodeUint64Desc
func DecodeUint64Desc(b []byte) (leftover []byte, v uint64, err error) {
	leftover, v, err = DecodeUint64(b)
	if err == nil {
		v = ^v
	}
	return
}
