package document

import "testing"

func TestSlice(t *testing.T) {

	c := 9
	c2 := 2
	s := make([]byte, c)
	s2 := s[0 : c2+1]
	if cap(s) != c && cap(s2) != c2 {
		t.Fatal("bug")
	}

}
