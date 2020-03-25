package compact

import (
	"encoding/binary"
	"fmt"

	"github.com/zhiqiangxu/util/bytes"
)

// EncodeBytes for encode []byte in a compact way
func EncodeBytes(b []byte, data []byte) []byte {
	b = bytes.Realloc(b, binary.MaxVarintLen64+len(data))
	b = EncodeVarint(b, int64(len(data)))
	return append(b, data...)
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before.
func DecodeBytes(b []byte) ([]byte, []byte, error) {
	b, n, err := DecodeVarint(b)
	if err != nil {
		return nil, nil, err
	}
	if int64(len(b)) < n {
		return nil, nil, fmt.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return b[n:], b[:n], nil
}
