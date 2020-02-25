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
				t.Fatal("Set key1")
			}

			// test Get
			v, _, err := c.Get(key1)
			if err != nil || !bytes.Equal(v, value1) {
				t.Fatal("Get key1")
			}

			// test Delete
			err = c.Delete(key1)
			if err != nil {
				t.Fatal("Delete key1", err)
			}

			// test Get when key not exists
			_, _, err = c.Get(key1)
			if err != provider.ErrKeyNotFound {
				t.Fatal("Get key1")
			}
		}

		{
			// test Update transaction
			key2 := []byte("key2")
			value2 := []byte("value2")
			err := c.Update(func(txn kvrpc.Txn) error {
				err := txn.Set(key2, value2, nil)
				if err != nil {
					t.Fatal("Update.Set key2")
				}

				v, _, err := txn.Get(key2)
				if err != nil || !bytes.Equal(v, value2) {
					t.Fatal("Update.Get key2")
				}

				err = txn.Delete(key2)
				if err != nil {
					t.Fatal("Update.Delete key2")
				}

				_, _, err = txn.Get(key2)
				if err != provider.ErrKeyNotFound {
					t.Fatal("Update.Get key2")
				}
				return nil
			})
			if err != nil {
				t.Fatal("Update", err)
			}
		}

		{
			// test Read transaction
			key3 := []byte("key3")
			value3 := []byte("value3")
			err := c.Set(key3, value3, nil)
			if err != nil {
				t.Fatal("Set key3", err)
			}
			err = c.View(func(txn kvrpc.Txn) error {
				v, _, err := txn.Get(key3)
				if err != nil || !bytes.Equal(v, value3) {
					t.Fatal("View Get key3", err)
				}
				return nil
			})
			if err != nil {
				t.Fatal("View", err)
			}
			err = c.Delete(key3)
			if err != nil {
				t.Fatal("Delete key3", err)
			}
		}

	}

}
