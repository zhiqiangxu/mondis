package memcomparable

import (
	"bytes"
	"testing"
)

func TestBytes(t *testing.T) {
	abc := []byte("abc")
	def := []byte("def")

	{
		leftover, data, err := DecodeBytes(EncodeBytes(nil, abc), nil)
		if len(leftover) != 0 || bytes.Compare(data, abc) != 0 || err != nil {
			t.Fail()
		}
	}

	{
		leftover, data, err := DecodeBytesDesc(EncodeBytesDesc(nil, abc), nil)
		if len(leftover) != 0 || bytes.Compare(data, abc) != 0 || err != nil {
			t.Fail()
		}
	}

	if bytes.Compare(EncodeBytes(nil, abc), EncodeBytes(nil, def)) >= 0 {
		t.Fail()
	}

	if bytes.Compare(EncodeBytesDesc(nil, abc), EncodeBytesDesc(nil, def)) <= 0 {
		t.Fail()
	}
}
