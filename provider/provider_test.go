package provider

import (
	"testing"

	"github.com/zhiqiangxu/mondis"
)

func TestProvider(t *testing.T) {
	for i := 0; i < 2; i++ {

		var b mondis.KVDB
		switch i {
		case 0:
			b = NewBadger()
		case 1:
			b = NewLevelDB()
		}

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
				t.Fatal("Set1")
			}
			// test Get nil value
			v, _, err := b.Get(key1)
			if err != nil || v != nil {
				t.Fatal("Get1")
			}

			exists, err := b.Exists(key1)
			if err != nil || !exists {
				t.Fatal("Exists1")
			}

			// test Set empty
			empty := []byte("")
			err = b.Set(key1, empty, nil)
			if err != nil {
				t.Fatal("Set2")
			}
			// test Get empty value
			v, _, err = b.Get(key1)
			if err != nil || v != nil {
				t.Fatal("Get2")
			}
			exists, err = b.Exists(key1)
			if err != nil || !exists {
				t.Fatal("Exists2")
			}
		}

		err = b.Close()
		if err != nil {
			t.Fatal("Close", err)
		}

	}

}
