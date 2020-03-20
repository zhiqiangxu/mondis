package test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/client"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/dml"
	"github.com/zhiqiangxu/mondis/document/domain"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/provider"
	"github.com/zhiqiangxu/mondis/server"
	"github.com/zhiqiangxu/mondis/structure"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	addr    = "localhost:8099"
	dataDir = "/tmp/mondis"
)

func TestBadger(t *testing.T) {
	// server side
	{
		// use badger provider
		kvdb := provider.NewBadger()
		s := server.New(addr, kvdb, server.Option{}, mondis.KVOption{Dir: dataDir})
		go s.Start()

		time.Sleep(time.Millisecond * 500)

		defer s.Stop()
	}

	// client side
	{
		c := client.New(addr, client.Option{})
		nonExistingKey := []byte("nonExistingKey")

		{
			// test Exists
			exists, err := c.Exists(nonExistingKey)
			if err != nil || exists {
				t.Fatal("Exists nonExistingKey1")
			}
			// test Set
			key1 := []byte("key1")
			value1 := []byte("value1")
			err = c.Set(key1, value1, nil)
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
			if err != kv.ErrKeyNotFound {
				t.Fatal("Get key1")
			}
		}

		{
			// test Update transaction
			key2 := []byte("key2")
			value2 := []byte("value2")
			err := c.Update(func(txn mondis.Txn) error {
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
				if err != kv.ErrKeyNotFound {
					t.Fatal("Update.Get key2")
				}

				// test Exists
				exists, err := txn.Exists(nonExistingKey)
				if err != nil || exists {
					t.Fatal("Exists nonExistingKey2")
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
			err = c.View(func(txn mondis.Txn) error {
				v, _, err := txn.Get(key3)
				if err != nil || !bytes.Equal(v, value3) {
					t.Fatal("View Get key3", err)
				}

				// test Exists
				exists, err := txn.Exists(nonExistingKey)
				if err != nil || exists {
					t.Fatal("Exists nonExistingKey3")
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

		{
			// test Scan
			prefix := "unique_prefix"
			n := 10
			for i := 0; i < n; i++ {
				err := c.Set([]byte(fmt.Sprintf("%s:%d", prefix, i)), []byte{(byte(i))}, nil)
				if err != nil {
					t.Fatal("Set", err)
				}
			}

			var (
				entries []mondis.Entry
				err     error
			)
			scanOption := mondis.ScanOption{Limit: n - 1, ProviderScanOption: mondis.ProviderScanOption{Prefix: []byte(prefix)}}
			for j := 0; j < 2; j++ {
				switch j {
				case 0:
					entries, err = c.Scan(scanOption)
				case 1:
					err = c.Update(func(txn mondis.Txn) error {
						entries, err = txn.Scan(scanOption)
						return err
					})
				case 2:
					err = c.View(func(txn mondis.Txn) error {
						entries, err = txn.Scan(scanOption)
						return err
					})
				}

				if err != nil {
					t.Fatal("Scan", err)
				}
				if len(entries) != (n - 1) {
					t.Fatal("entry count not expected")
				}

				// scan result should be from low to high
				for i, entry := range entries {
					if !bytes.Equal(entry.Key, []byte(fmt.Sprintf("%s:%d", prefix, i))) {
						t.Fatal("entry not expected", i)
					}
				}
			}

		}

	}

}

func TestDocument(t *testing.T) {
	os.RemoveAll(dataDir)
	kvdb := provider.NewBadger()
	err := kvdb.Open(mondis.KVOption{Dir: dataDir})
	if err != nil {
		t.Fatal("kvdb.Open", err)
	}

	do := domain.NewDomain(kvdb)
	_, err = do.DDL().CreateSchema(context.Background(), ddl.CreateSchemaInput{DB: "db", Collections: []string{"c"}})
	if err != nil {
		t.Fatal("CreateSchema", err)
	}
	db, err := do.DB("db")
	if err != nil {
		t.Fatal("DB", err)
	}

	c, err := db.Collection("c")
	if err != nil {
		t.Fatal("db.Collection", err)
	}

	// test GetDidRange before DeleteAll
	{
		key := "key range"
		did1, err := c.InsertOne(bson.M{key: "value"}, nil)
		if err != nil {
			t.Fatal("c.InsertOne", err)
		}
		did2, err := c.InsertOne(bson.M{key: "value"}, nil)
		if err != nil {
			t.Fatal("c.InsertOne", err)
		}

		min, max, err := c.GetDidRange(nil)
		if err != nil {
			t.Fatal("c.GetDidRange", err)
		}

		if min != did1 || max != did2 {
			t.Fatal("GetDidRange result not expected")
		}
	}

	n, err := c.DeleteAll(nil)
	if err != nil {
		t.Fatal("c.DeleteAll", err, n)
	}

	key := "key"
	did, err := c.InsertOne(bson.M{key: "value"}, nil)
	if err != nil {
		t.Fatal("c.InsertOne", err)
	}

	data, err := c.GetOne(did, nil)
	if err != nil {
		t.Fatal("c.GetOne", err)
	}
	if data[key] != "value" {
		t.Fatal("data[key] != \"value\"")
	}

	updated, err := c.UpdateOne(did, bson.M{key: "value2"}, nil)
	if err != nil || !updated {
		t.Fatal("c.InsertOne", err, updated)
	}

	data, err = c.GetOne(did, nil)
	if err != nil {
		t.Fatal("c.GetOne", err)
	}
	if data[key] != "value2" {
		t.Fatal("data[key] != \"value2\"")
	}

	n, err = c.Count(nil)
	if err != nil || n != 1 {
		t.Fatal("c.Count")
	}

	err = c.DeleteOne(did, nil)
	if err != nil {
		t.Fatal("c.DeleteOne", err)
	}

	err = c.InsertOneManaged(1000, bson.M{key: "value"}, nil)
	if err != nil {
		t.Fatal("c.InsertOneManaged", err)
	}
	n, err = c.Count(nil)
	if err != nil || n != 1 {
		t.Fatal("c.Count")
	}
	err = c.DeleteOne(1000, nil)
	if err != nil {
		t.Fatal("c.DeleteOne", err)
	}

	_, err = c.GetOne(did, nil)
	if err != dml.ErrDocNotFound {
		t.Fatal("err != dml.ErrDocNotFound", err)
	}

	// {
	// 	// test index
	// 	c, err := db.Collection("i")
	// 	if err != nil {
	// 		t.Fatal("db.Collection", err)
	// 	}
	// 	idxName := "test_idx"
	// 	idef := document.IndexDefinition{
	// 		Name: idxName,
	// 		Fields: []document.IndexField{
	// 			document.IndexField{Name: "f1"},
	// 		},
	// 	}
	// 	iid, err := c.CreateIndex(idef)
	// 	if err != nil {
	// 		t.Fatal("c.CreateIndex", err)
	// 	}
	// 	if iid <= 0 {
	// 		t.Fatal("iid <=0", iid)
	// 	}

	// 	allIndexes := c.GetIndexes()
	// 	if len(allIndexes) != 1 {
	// 		t.Fatal("len(allIndexes)!=1")
	// 	}

	// 	exists, err := c.DropIndex(idxName)
	// 	if err != nil {
	// 		t.Fatal("c.DropIndex", err)
	// 	}
	// 	if !exists {
	// 		t.Fatal()
	// 	}
	// }
	// db.Close()

	// err = c.DeleteOne(did, nil)
	// if err != document.ErrAlreadyClosed {
	// 	t.Fatal("err != document.ErrAlreadyClosed")
	// }

}

func TestList(t *testing.T) {
	kvdb := provider.NewBadger()
	err := kvdb.Open(mondis.KVOption{Dir: dataDir})
	if err != nil {
		t.Fatal("kvdb.Open", err)
	}
	defer kvdb.Close()

	txn := kvdb.NewTransaction(true)
	txStruct := structure.New(txn, []byte("l"))
	list1 := []byte("list1")
	l, err := txStruct.LLen(list1)
	if err != nil || l != 0 {
		t.Fatal(err, l)
	}

	err = txStruct.LPush(list1, []byte("item1"), []byte("item2"))
	if err != nil {
		t.FailNow()
	}
	l, err = txStruct.LLen(list1)
	if err != nil || l != 2 {
		t.Fatal(err, l)
	}

	item, err := txStruct.LPop(list1)
	if err != nil {
		t.FailNow()
	}
	if !bytes.Equal(item, []byte("item2")) {
		t.FailNow()
	}
	item, err = txStruct.LPop(list1)
	if err != nil {
		t.FailNow()
	}
	if !bytes.Equal(item, []byte("item1")) {
		t.FailNow()
	}

	err = txStruct.RPush(list1, []byte("item1"), []byte("item2"))
	if err != nil {
		t.FailNow()
	}
	l, err = txStruct.LLen(list1)
	if err != nil || l != 2 {
		t.Fatal(err, l)
	}

	item, err = txStruct.LPop(list1)
	if err != nil {
		t.FailNow()
	}
	if !bytes.Equal(item, []byte("item1")) {
		t.FailNow()
	}
	item, err = txStruct.LPop(list1)
	if err != nil {
		t.FailNow()
	}
	if !bytes.Equal(item, []byte("item2")) {
		t.FailNow()
	}

	l, err = txStruct.LLen(list1)
	if err != nil || l != 0 {
		t.Fatal(err, l)
	}

	err = txn.Commit()
	if err != nil {
		t.FailNow()
	}
}

func TestHash(t *testing.T) {
	kvdb := provider.NewBadger()
	err := kvdb.Open(mondis.KVOption{Dir: dataDir})
	if err != nil {
		t.Fatal("kvdb.Open", err)
	}
	defer kvdb.Close()

	txn := kvdb.NewTransaction(true)
	txStruct := structure.New(txn, []byte("l"))
	hash1 := []byte("hash1")

	err = txStruct.HClear(hash1)
	if err != nil {
		t.FailNow()
	}

	f1 := []byte("f1")
	v1 := []byte("v1")
	f2 := []byte("f2")
	v2 := []byte("v2")
	f3 := []byte("f3")
	v3 := []byte("v3")
	err = txStruct.HSet(hash1, f1, v1)
	if err != nil {
		t.FailNow()
	}
	err = txStruct.HSet(hash1, f2, v2)
	if err != nil {
		t.FailNow()
	}
	err = txStruct.HSet(hash1, f3, v3)
	if err != nil {
		t.FailNow()
	}

	n, err := txStruct.HLen(hash1)
	if err != nil || n != 3 {
		t.FailNow()
	}

	err = txStruct.HDel(hash1, f1)
	if err != nil {
		t.FailNow()
	}
	n, err = txStruct.HLen(hash1)
	if err != nil || n != 2 {
		t.FailNow()
	}
	err = txStruct.HSet(hash1, f1, v1)
	if err != nil {
		t.FailNow()
	}

	err = txn.Commit()
	if err != nil {
		t.FailNow()
	}

	txn = kvdb.NewTransaction(true)
	txStruct = structure.New(txn, []byte("l"))
	pairs, err := txStruct.HGetN(hash1, 3)
	if err != nil {
		t.FailNow()
	}
	pairsDesc, err := txStruct.HGetNDesc(hash1, 3)
	if err != nil {
		t.FailNow()
	}

	if len(pairs) != 3 || len(pairsDesc) != 3 {
		t.FailNow()
	}

	for i, p := range pairs {
		pdesc := pairsDesc[2-i]
		if !(bytes.Equal(p.Field, pdesc.Field) && bytes.Equal(p.Value, pdesc.Value)) {
			t.FailNow()
		}
	}
}
