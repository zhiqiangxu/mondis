package memcomparable

import (
	"bytes"
	"testing"
)

func TestFloat(t *testing.T) {
	if encodeFloat64ToUint64(1.1) < encodeFloat64ToUint64(-1.1) {
		t.Fail()
	}

	if bytes.Compare(EncodeFloat64(nil, 1.1), EncodeFloat64(nil, -1.1)) <= 0 {
		t.Fail()
	}

	if bytes.Compare(EncodeFloat64Desc(nil, 1.1), EncodeFloat64Desc(nil, -1.1)) >= 0 {
		t.Fail()
	}

	if decodeUint64ToFloat64(encodeFloat64ToUint64(1.5)) != 1.5 {
		t.Fail()
	}
	if decodeUint64ToFloat64(encodeFloat64ToUint64(-1.5)) != -1.5 {
		t.Fail()
	}

	{
		leftover, v, err := DecodeFloat64(EncodeFloat64(nil, -1))
		if len(leftover) != 0 || v != -1 || err != nil {
			t.Fail()
		}
	}

	{
		leftover, v, err := DecodeFloat64Desc(EncodeFloat64Desc(nil, -1))
		if len(leftover) != 0 || v != -1 || err != nil {
			t.Fail()
		}
	}

}
