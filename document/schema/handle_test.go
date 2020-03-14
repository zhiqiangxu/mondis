package schema

import "testing"

func TestHandle(t *testing.T) {
	h := NewHandle()
	mc := h.GetSnap()
	if mc != nil {
		t.FailNow()
	}
}
