package memcomparable

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/zhiqiangxu/util/bytes"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var (
	pads = make([]byte, encGroupSize)
)

// EncodeBytes for convert []byte to memcomparable-format
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes slice which is padding with 0.
//  marker is `0xFF - padding 0 count`
// For example:
//   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
func EncodeBytes(b []byte, data []byte) []byte {
	dLen := len(data)
	reallocSize := (dLen/encGroupSize + 1) * (encGroupSize + 1)
	result := bytes.Realloc(b, reallocSize)
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}

	return result
}

// EncodedBytesLength returns the length of data after encoded
func EncodedBytesLength(dataLen int) int {
	mod := dataLen % encGroupSize
	padCount := encGroupSize - mod
	return dataLen + padCount + 1 + dataLen/encGroupSize
}

func decodeBytes(b []byte, buf []byte, reverse bool) (leftover []byte, data []byte, err error) {
	if buf == nil {
		buf = make([]byte, 0, len(b))
	}
	buf = buf[:0]
	for {
		if len(b) < encGroupSize+1 {
			err = ErrInsufficientBytesToDecode
			return
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		if padCount > encGroupSize {
			err = fmt.Errorf("invalid marker byte, group bytes %q", groupBytes)
			return
		}

		realGroupSize := encGroupSize - padCount
		buf = append(buf, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			var padByte = encPad
			if reverse {
				padByte = encMarker
			}
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					err = fmt.Errorf("invalid padding byte, group bytes %q", groupBytes)
					return
				}
			}
			break
		}
	}
	if reverse {
		reverseBytes(buf)
	}

	leftover = b
	data = buf
	return
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before
// returns the leftover bytes and decoded value if no error.
func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf, false)
}

// EncodeBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
func EncodeBytesDesc(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytes(b, data)
	reverseBytes(b[n:])
	return b
}

// DecodeBytesDesc decodes bytes which is encoded by EncodeBytesDesc before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytesDesc(b []byte, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf, true)
}

// See https://golang.org/src/crypto/cipher/xor.go
const wordSize = int(unsafe.Sizeof(uintptr(0)))
const supportsUnaligned = runtime.GOARCH == "386" || runtime.GOARCH == "amd64"

func fastReverseBytes(b []byte) {
	n := len(b)
	w := n / wordSize
	if w > 0 {
		bw := *(*[]uintptr)(unsafe.Pointer(&b))
		for i := 0; i < w; i++ {
			bw[i] = ^bw[i]
		}
	}

	for i := w * wordSize; i < n; i++ {
		b[i] = ^b[i]
	}
}

func safeReverseBytes(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}

func reverseBytes(b []byte) {
	if supportsUnaligned {
		fastReverseBytes(b)
		return
	}

	safeReverseBytes(b)
}
