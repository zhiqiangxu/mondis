package memcomparable

import (
	"bytes"
	"testing"
)

func TestNumber(t *testing.T) {
	if EncodeInt64ToUint64(1) < EncodeInt64ToUint64(-1) {
		t.Fail()
	}

	if bytes.Compare(EncodeInt64(nil, 1), EncodeInt64(nil, -1)) <= 0 {
		t.Fail()
	}

	if bytes.Compare(EncodeUint64(nil, 2), EncodeUint64(nil, 1)) <= 0 {
		t.Fail()
	}

	if bytes.Compare(EncodeInt64Desc(nil, 1), EncodeInt64Desc(nil, -1)) >= 0 {
		t.Fail()
	}

	if bytes.Compare(EncodeUint64Desc(nil, 2), EncodeUint64Desc(nil, 1)) >= 0 {
		t.Fail()
	}

	if DecodeUint64ToInt64(EncodeInt64ToUint64(1)) != 1 {
		t.Fail()
	}
	if DecodeUint64ToInt64(EncodeInt64ToUint64(-1)) != -1 {
		t.Fail()
	}

	{
		leftover, v, err := DecodeInt64(EncodeInt64(nil, -1))
		if len(leftover) != 0 || v != -1 || err != nil {
			t.Fail()
		}
	}

	{
		leftover, v, err := DecodeInt64Desc(EncodeInt64Desc(nil, -1))
		if len(leftover) != 0 || v != -1 || err != nil {
			t.Fail()
		}
	}

	{
		leftover, v, err := DecodeUint64(EncodeUint64(nil, 1))
		if len(leftover) != 0 || v != 1 || err != nil {
			t.Fail()
		}
	}

	{
		leftover, v, err := DecodeUint64Desc(EncodeUint64Desc(nil, 1))
		if len(leftover) != 0 || v != 1 || err != nil {
			t.Fail()
		}
	}

}
