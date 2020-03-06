package numeric

import (
	"encoding/binary"
	"errors"
)

var (
	errInvalidValueForBinary = errors.New("invalid value for binary")
)

// Encode2Binary will encode uint64 to binary
func Encode2Binary(v uint64, buf []byte) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

// DecodeFromBinary is reverse for Encode2Binary
func DecodeFromBinary(b []byte) (v uint64, err error) {
	if len(b) != 8 {
		err = errInvalidValueForBinary
		return
	}

	v = binary.BigEndian.Uint64(b)

	return
}
