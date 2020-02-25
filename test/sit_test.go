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
			t.Fatalf("Set ng")
		}

		v, _, err := c.Get(key1)
		if err != nil || !bytes.Equal(v, value1) {
			t.Fatalf("Get ng")
		}
	}
}
