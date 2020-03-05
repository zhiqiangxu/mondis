package kv

import (
	"bytes"
	"testing"

	"github.com/zhiqiangxu/mondis/kv/memcomparable"
)

func TestNext(t *testing.T) {

	keyA := memcomparable.EncodeBytes(memcomparable.EncodeBytes(nil, []byte("abc")), []byte("def"))
	keyB := memcomparable.EncodeBytes(memcomparable.EncodeBytes(nil, []byte("abca")), []byte("def"))

	seekKey := memcomparable.EncodeBytes(nil, []byte("abc"))
	// Use next key to seek to the first key equal to "abc"
	nextKey := Key(seekKey).Next()
	cmp := bytes.Compare(nextKey, keyA)
	if cmp != -1 {
		t.Fail()
	}

	// Use next partial key, we can skip all index keys with first column value equal to "abc".
	nextPartialKey := Key(seekKey).PrefixNext()
	if !bytes.Equal(nextPartialKey, []byte{'a', 'b', 'c', 0, 0, 0, 0, 0, 251}) {
		t.Fail()
	}
	cmp = bytes.Compare(nextPartialKey, keyA)
	if cmp != 1 {
		t.Fail()
	}

	if !bytes.Equal(Key([]byte("abc")).PrefixNext(), []byte("abd")) {
		t.Fail()
	}
	// this is actually comparing [a, b, c, 0, 0, 0, 0, 0, 251] and [a, b, c, a, 0, 0, 0, 0, 251]
	cmp = bytes.Compare(nextPartialKey, keyB)
	if cmp != -1 {
		t.Fail()
	}
}

func TestIsPoint(t *testing.T) {
	if bytes.Compare([]byte("abd"), []byte("abca")) <= 0 {
		t.Fail()
	}
	tests := []struct {
		start   []byte
		end     []byte
		isPoint bool
	}{
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey2"),
			isPoint: true,
		},
		{
			start:   Key("abc"),
			end:     Key("abd"),
			isPoint: true,
		},
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey3"),
			isPoint: false,
		},
		{
			start:   Key(""),
			end:     []byte{0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 1},
			isPoint: false,
		},
		{
			start:   []byte{123, 123},
			end:     []byte{123, 123, 0},
			isPoint: true,
		},
		{
			start:   []byte{255},
			end:     []byte{0},
			isPoint: false,
		},
	}
	for _, tt := range tests {
		kr := KeyRange{
			StartKey: tt.start,
			EndKey:   tt.end,
		}
		if kr.IsPoint() != tt.isPoint {
			t.Fail()
		}
	}
}
