package memcomparable

import "math"

func encodeFloat64ToUint64(f float64) uint64 {
	// FYI: https://stackoverflow.com/questions/37758267/golang-float64bits
	//		https://segmentfault.com/a/1190000017919244
	u := math.Float64bits(f)
	if f >= 0 {
		// 对于正浮点数，0EM，相对大小同EM作为整型的相对大小
		u |= signMask
	} else {
		// 对于负浮点数，1EM，相对大小同EM作为整型的相对大小相反
		u = ^u
	}
	return u
}

func decodeUint64ToFloat64(u uint64) float64 {
	if u&signMask > 0 {
		u &= ^signMask
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

// EncodeFloat64 for convert float64 to memcomparable-format
func EncodeFloat64(b []byte, v float64) []byte {
	u := encodeFloat64ToUint64(v)
	return EncodeUint64(b, u)
}

// DecodeFloat64 decodes a float64 from a byte slice generated with EncodeFloat64 before.
func DecodeFloat64(b []byte) ([]byte, float64, error) {
	b, u, err := DecodeUint64(b)
	return b, decodeUint64ToFloat64(u), err
}

// EncodeFloat64Desc for encoding float64 in descending order
func EncodeFloat64Desc(b []byte, v float64) []byte {
	u := encodeFloat64ToUint64(v)
	return EncodeUint64Desc(b, u)
}

// DecodeFloat64Desc decodes a float64 from a byte slice generated with EncodeFloat64Desc before.
func DecodeFloat64Desc(b []byte) ([]byte, float64, error) {
	b, u, err := DecodeUint64Desc(b)
	return b, decodeUint64ToFloat64(u), err
}
