package provider

import (
	"os"
	"testing"

	"github.com/zhiqiangxu/mondis"
	"gotest.tools/assert"
)

const dataDir = "/tmp/mondis"

func TestProvider(t *testing.T) {

	providers := []func() mondis.KVDB{
		NewBadger, NewLevelDB,
	}

	for _, provider := range providers {
		os.RemoveAll(dataDir)

		b := provider()

		err := b.Open(mondis.KVOption{Dir: dataDir})
		assert.Assert(t, err == nil)

		key1 := []byte("key1")
		{
			// badger will return nil for both nil and empty value

			// test Set nil
			err = b.Set(key1, nil, nil)
			assert.Assert(t, err == nil)
			// test Get nil value
			v, _, err := b.Get(key1)
			assert.Assert(t, err == nil && v == nil)

			exists, err := b.Exists(key1)
			assert.Assert(t, err == nil && exists)

			// test Set empty
			empty := []byte("")
			err = b.Set(key1, empty, nil)
			assert.Assert(t, err == nil)

			// test Get empty value
			v, _, err = b.Get(key1)
			assert.Assert(t, err == nil && v == nil)

			exists, err = b.Exists(key1)
			assert.Assert(t, err == nil && exists)
		}

		err = b.Close()
		assert.Assert(t, err == nil)

	}

}
