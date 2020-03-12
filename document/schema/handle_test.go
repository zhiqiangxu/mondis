package schema

import "testing"

func TestHandle(t *testing.T) {
	h := NewHandle()
	mc := h.Get()
	if mc != nil {
		t.FailNow()
	}
}
