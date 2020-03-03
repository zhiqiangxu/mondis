package provider

import (
	"testing"

	"github.com/zhiqiangxu/mondis"
)

func TestBadger(t *testing.T) {
	b := NewBadger()
	err := b.Open(mondis.KVOption{Dir: "/tmp/mondis"})
	if err != nil {
		t.Fatal("b.Open")
	}

	key1 := []byte("key1")
	{
		// badger will return nil for both nil and empty value

		// test Set nil
		err = b.Set(key1, nil, nil)
		if err != nil {
			t.Fatal("Set key1 nil")
		}
		// test Get nil value
		v, _, err := b.Get(key1)
		if err != nil || v != nil {
			t.Fatal("Get key1 nil")
		}

		// test Set empty
		empty := []byte("")
		err = b.Set(key1, empty, nil)
		if err != nil {
			t.Fatal("Set key1 empty")
		}
		// test Get empty value
		v, _, err = b.Get(key1)
		if err != nil || v != nil {
			t.Fatal("Get key1 nil")
		}
	}

}
