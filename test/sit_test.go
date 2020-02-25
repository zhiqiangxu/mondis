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
		// use badger provider
		kvdb := provider.NewBadger()
		s := server.New(addr, kvdb, server.Option{}, kvrpc.KVOption{Dir: dataDir})
		go s.Start()

		time.Sleep(time.Millisecond * 500)

		defer s.Stop()
	}

	// client side
	{
		c := client.New(addr, client.Option{})

		{
			// test Set
			key1 := []byte("key1")
			value1 := []byte("value1")
			err := c.Set(key1, value1, nil)
			if err != nil {
				t.Fatal("Set ng")
			}

			// test Get
			v, _, err := c.Get(key1)
			if err != nil || !bytes.Equal(v, value1) {
				t.Fatal("Get ng")
			}

			// test Delete
			err = c.Delete(key1)
			if err != nil {
				t.Fatal("Delete ng", err)
			}

			// test Get when key not exists
			_, _, err = c.Get(key1)
			if err != provider.ErrKeyNotFound {
				t.Fatal("Get ng")
			}
		}

		{
			// test Update transaction
			key2 := []byte("key2")
			value2 := []byte("value2")
			err := c.Update(func(txn kvrpc.Txn) error {
				err := txn.Set(key2, value2, nil)
				if err != nil {
					t.Fatal("Update.Set ng")
				}

				v, _, err := txn.Get(key2)
				if err != nil || !bytes.Equal(v, value2) {
					t.Fatal("Update.Get ng")
				}

				err = txn.Delete(key2)
				if err != nil {
					t.Fatal("Update.Delete ng")
				}

				_, _, err = txn.Get(key2)
				if err != provider.ErrKeyNotFound {
					t.Fatal("Update.Get ng2")
				}
				return nil
			})
			if err != nil {
				t.Fatal("Update ng", err)
			}
		}

		{
			// test Read transaction
			key3 := []byte("key3")
			value3 := []byte("value3")
			err := c.Set(key3, value3, nil)
			if err != nil {
				t.Fatal("Set ng2", err)
			}
			err = c.View(func(txn kvrpc.Txn) error {
				v, _, err := txn.Get(key3)
				if err != nil || !bytes.Equal(v, value3) {
					t.Fatal("View Get ng", err)
				}
				return nil
			})
			if err != nil {
				t.Fatal("View ng", err)
			}
			err = c.Delete(key3)
			if err != nil {
				t.Fatal("Delete ng2", err)
			}
		}

	}
}
