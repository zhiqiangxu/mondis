# kvrpc ≈ redis + mongodb

`kvrpc` aimed to be a universal rpc layer for any key value database, but as it evolves, `kvrpc` now also aims to bridge the gap between a document-oriented database and a key value database!

## Features:

1. complete key value database api
    1. `Set`
    2. `Exists`
    3. `Get`
    4. `Delete`
    5. `Scan`
    6. `Update` (writable transaction)
    7. `View`   (readonly transaction)
2. document-oriented database api like mongodb (in progress)

Refer to [`kvrpc.Client`](https://github.com/zhiqiangxu/kvrpc/blob/master/kvrpc.go#L6) or [`test cases`](https://github.com/zhiqiangxu/kvrpc/blob/master/test/sit_test.go) for details.

`kvrpc` is based on [`qrpc`](https://github.com/zhiqiangxu/qrpc).

## Demo

This is how to start a server for badger provider:

```golang
package main

import (
    "github.com/zhiqiangxu/kvrpc/provider"
    "github.com/zhiqiangxu/kvrpc/server"
)

const (
	addr    = "localhost:8099"
	dataDir = "/tmp/kvrpc"
)

func main() {
    // use badger provider
    kvdb := provider.NewBadger()
    // create a kvrpc server
    s := server.New(addr, kvdb, server.Option{}, kvrpc.KVOption{Dir: dataDir})
    // start the server
    s.Start()
}

```

This is how to start the client:

```golang
package main

import (
    "github.com/zhiqiangxu/kvrpc/client"
    "bytes"
)

func main() {
    // create a kvrpc client
    c := client.New(addr, client.Option{})

    // test Set
    key1 := []byte("key1")
    value1 := []byte("value1")
    err := c.Set(key1, value1, nil)
    if err != nil {
        panic("Set key1")
    }

    // test Get
    v, _, err := c.Get(key1)
    if err != nil || !bytes.Equal(v, value1) {
        panic("Get key1")
    }

    // test Delete
    err = c.Delete(key1)
    if err != nil {
        panic("Delete key1", err)
    }

    // test Get when key not exists
    _, _, err = c.Get(key1)
    if err != provider.ErrKeyNotFound {
        panic("Get key1")
    }

    // test Update transaction
    key2 := []byte("key2")
    value2 := []byte("value2")
    err = c.Update(func(txn kvrpc.Txn) error {
        err := txn.Set(key2, value2, nil)
        if err != nil {
            panic("Update.Set key2")
        }

        v, _, err := txn.Get(key2)
        if err != nil || !bytes.Equal(v, value2) {
            panic("Update.Get key2")
        }

        err = txn.Delete(key2)
        if err != nil {
            panic("Update.Delete key2")
        }
        return nil
    })
    if err != nil {
        panic("Update")
    }
    
    // test Read transaction
    key3 := []byte("key3")
    value3 := []byte("value3")
    err = c.Set(key3, value3, nil)
    if err != nil {
        panic("Set key3")
    }
    err = c.View(func(txn kvrpc.Txn) error {
        v, _, err := txn.Get(key3)
        if err != nil || !bytes.Equal(v, value3) {
            panic("View Get key3")
        }
        return nil
    })

}


```

See how you can make use of different key value databases in a universal way? Enjoy it!