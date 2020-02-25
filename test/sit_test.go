package test

import (
	"bytes"
	"testing"
	"time"

	"github.com/zhiqiangxu/kvrpc"
	"github.com/zhiqiangxu/kvrpc/client"
	"github.com/zhiqiangxu/kvrpc/provider"
	"github.com/zhiqiangxu/kvrpc/server"
)

const (
	addr    = "localhost:8099"
	dataDir = "/tmp/kvrpc"
)

func TestBadger(t *testing.T) {
	// server side
	{
		kvdb := provider.NewBadger()
		s := server.New(addr, kvdb, server.Option{}, kvrpc.KVOption{Dir: dataDir})
		go s.Start()

		time.Sleep(time.Millisecond * 500)

		defer s.Stop()
	}

	// client side
	{
		c := client.New(addr, client.Option{})

		key1 := []byte("key1")
		value1 := []byte("value1")
		err := c.Set(key1, value1, nil)
		if err != nil {
			t.Fatal("Set ng")
		}

		v, _, err := c.Get(key1)
		if err != nil || !bytes.Equal(v, value1) {
			t.Fatal("Get ng")
		}

		err = c.Delete(key1)
		if err != nil {
			t.Fatal("Delete ng", err)
		}

		_, _, err = c.Get(key1)
		if err != provider.ErrKeyNotFound {
			t.Fatal("Get ng")
		}

		key2 := []byte("key2")
		value2 := []byte("value2")
		err = c.Update(func(txn kvrpc.Txn) error {
			err := txn.Set(key2, value2, nil)
			if err != nil {
				t.Fatal("Update.Set ng")
			}

			v, _, err := txn.Get(key2)
			if err != nil || !bytes.Equal(v, value2) {
				t.Fatal("Update.Get ng")
			}
			return nil
		})
		if err != nil {
			t.Fatal("Update ng", err)
		}
	}
}
