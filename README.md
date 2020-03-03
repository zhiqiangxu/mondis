# mondis ≈ mongodb + redis

`mondis` started as `kvrpc` to be a universal rpc layer for any key value database, but as it evolves, `mondis` now also aims to bridge the gap between a document-oriented database and a key value database!

## Features:

1. full `key value database api`
    1. `Set`
    2. `Exists`
    3. `Get`
    4. `Delete`
    5. `Scan`
    6. `Update` (writable transaction)
    7. `View`   (readonly transaction)
2. `document-oriented database api` like mongodb (in progress)

Refer to [`mondis.Client`](https://github.com/zhiqiangxu/mondis/blob/master/mondis.go#L6) or [`test cases`](https://github.com/zhiqiangxu/mondis/blob/master/test/sit_test.go) for details.

`mondis` is based on [`qrpc`](https://github.com/zhiqiangxu/qrpc).

## Demo

This is how to start a server with a badger provider:

```golang
package main

import (
    "github.com/zhiqiangxu/mondis/provider"
    "github.com/zhiqiangxu/mondis/server"
)

const (
	addr    = "localhost:8099"
	dataDir = "/tmp/mondis"
)

func main() {
    // use badger provider
    kvdb := provider.NewBadger()
    // create a mondis server
    s := server.New(addr, kvdb, server.Option{}, mondis.KVOption{Dir: dataDir})
    // start the server
    s.Start()
}

```

This is how to request the server from a client:

```golang
package main

import (
    "github.com/zhiqiangxu/mondis/client"
    "bytes"
)

func main() {
    // create a mondis client
    c := client.New(addr, client.Option{})

    // test Set
    key1 := []byte("key1")
    value1 := []byte("value1")
    _ = c.Set(key1, value1, nil)
    

    // test Get
    v, _, _ := c.Get(key1)
    if !bytes.Equal(v, value1) {
        panic("return value not expected")
    }

    // test Delete
    err = c.Delete(key1)

    // test Get when key not exists
    _, _, err = c.Get(key1)
    if err != provider.ErrKeyNotFound {
        panic("shoud got provider.ErrKeyNotFound if key not exists")
    }

    // test Update transaction
    key2 := []byte("key2")
    value2 := []byte("value2")
    _ = c.Update(func(txn mondis.Txn) error {
        _ = txn.Set(key2, value2, nil)

        v, _, _ := txn.Get(key2)
        if !bytes.Equal(v, value2) {
            panic("return value not expected")
        }

        _ = txn.Delete(key2)
        return nil
    })
    
    
    // test Read transaction
    key3 := []byte("key3")
    value3 := []byte("value3")
    err = c.Set(key3, value3, nil)
    err = c.View(func(txn mondis.Txn) error {
        v, _, _ := txn.Get(key3)
        if !bytes.Equal(v, value3) {
            panic("return value not expected")
        }
        return nil
    })

}


```

The above shows how to use the `key value database api`.

The `document-oriented database api` is under active development, so keep tuned!