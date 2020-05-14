package test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"reflect"

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
	"gotest.tools/assert"
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
			assert.Assert(t, err == nil && !exists)

			// test Set
			key1 := []byte("key1")
			value1 := []byte("value1")
			err = c.Set(key1, value1, nil)
			assert.Assert(t, err == nil)

			// test Get
			v, _, err := c.Get(key1)
			assert.Assert(t, err == nil && bytes.Equal(v, value1))

			// test Delete
			err = c.Delete(key1)
			assert.Assert(t, err == nil)

			// test Get when key not exists
			_, _, err = c.Get(key1)
			assert.Assert(t, err == kv.ErrKeyNotFound)
		}

		{
			// test Update transaction
			key2 := []byte("key2")
			value2 := []byte("value2")
			err := c.Update(func(txn mondis.Txn) error {
				err := txn.Set(key2, value2, nil)
				assert.Assert(t, err == nil)

				v, _, err := txn.Get(key2)
				assert.Assert(t, err == nil && bytes.Equal(v, value2))

				err = txn.Delete(key2)
				assert.Assert(t, err == nil)

				_, _, err = txn.Get(key2)
				assert.Assert(t, err == kv.ErrKeyNotFound)

				// test Exists
				exists, err := txn.Exists(nonExistingKey)
				assert.Assert(t, err == nil && !exists)
				return nil
			})
			assert.Assert(t, err == nil)
		}

		{
			// test Read transaction
			key3 := []byte("key3")
			value3 := []byte("value3")
			err := c.Set(key3, value3, nil)
			assert.Assert(t, err == nil)
			err = c.View(func(txn mondis.Txn) error {
				v, _, err := txn.Get(key3)
				assert.Assert(t, err == nil && bytes.Equal(v, value3))

				// test Exists
				exists, err := txn.Exists(nonExistingKey)
				assert.Assert(t, err == nil && !exists)
				return nil
			})
			assert.Assert(t, err == nil)
			err = c.Delete(key3)
			assert.Assert(t, err == nil)
		}

		{
			// test Scan
			prefix := "unique_prefix"
			n := 10
			for i := 0; i < n; i++ {
				err := c.Set([]byte(fmt.Sprintf("%s:%d", prefix, i)), []byte{(byte(i))}, nil)
				assert.Assert(t, err == nil)
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

				assert.Assert(t, err == nil && len(entries) == (n-1))

				// scan result should be from low to high
				for i, entry := range entries {
					assert.Assert(t, bytes.Equal(entry.Key, []byte(fmt.Sprintf("%s:%d", prefix, i))))
				}
			}

		}

	}

}

func TestDocument(t *testing.T) {
	os.RemoveAll(dataDir)
	kvdb := provider.NewBadger()
	err := kvdb.Open(mondis.KVOption{Dir: dataDir})
	assert.Assert(t, err == nil)

	do := domain.NewDomain(kvdb)
	assert.Assert(t, do.Init() == nil)
	_, err = do.DDL().CreateSchema(context.Background(), ddl.CreateSchemaInput{DB: "db", Collections: []string{"c"}})
	assert.Assert(t, err == nil)
	db, err := do.DB("db")
	assert.Assert(t, err == nil)

	c, err := db.Collection("c")
	assert.Assert(t, err == nil)

	// test GetDidRange before DeleteAll
	{
		key := "key range"
		did1, err := c.InsertOne(bson.M{key: "value"}, nil)
		assert.Assert(t, err == nil)
		did2, err := c.InsertOne(bson.M{key: "value"}, nil)
		assert.Assert(t, err == nil)

		min, max, err := c.GetDidRange(nil)
		assert.Assert(t, err == nil && min == did1 && max == did2)

		n, err := c.Count(nil)
		assert.Assert(t, err == nil && n == 2)

		var result []bson.M
		err = c.GetMany([]int64{did1, did2}, &result, nil)
		assert.Assert(t, err == nil && len(result) == 2)
		result = nil
		err = c.GetAll(&result, nil)
		assert.Assert(t, err == nil && len(result) == 2)
	}

	n, err := c.DeleteAll(nil)
	assert.Assert(t, err == nil)

	key := "key"
	did, err := c.InsertOne(bson.M{key: "value"}, nil)
	assert.Assert(t, err == nil)

	var data bson.M
	err = c.GetOne(did, &data, nil)
	assert.Assert(t, err == nil && data[key] == "value")

	updated, err := c.UpdateOne(did, bson.M{key: "value2"}, nil)
	assert.Assert(t, err == nil && updated)

	err = c.GetOne(did, &data, nil)
	assert.Assert(t, err == nil && data[key] == "value2")

	n, err = c.Count(nil)
	assert.Assert(t, err == nil && n == 1)

	err = c.DeleteOne(did, nil)
	assert.Assert(t, err == nil)

	err = c.InsertOneManaged(1000, bson.M{key: "value"}, nil)
	assert.Assert(t, err == nil)
	n, err = c.Count(nil)
	assert.Assert(t, err == nil && n == 1)
	err = c.DeleteOne(1000, nil)
	assert.Assert(t, err == nil)

	err = c.GetOne(did, nil, nil)
	assert.Assert(t, err == dml.ErrDocNotFound)

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
	assert.Assert(t, err == nil)
	defer kvdb.Close()

	txn := kvdb.NewTransaction(true)
	txStruct := structure.New(txn, []byte("l"))
	list1 := []byte("list1")
	l, err := txStruct.LLen(list1)
	assert.Assert(t, err == nil && l == 0)

	err = txStruct.LPush(list1, []byte("item1"), []byte("item2"))
	assert.Assert(t, err == nil)
	l, err = txStruct.LLen(list1)
	assert.Assert(t, err == nil && l == 2)

	item, err := txStruct.LPop(list1)
	assert.Assert(t, err == nil && bytes.Equal(item, []byte("item2")))

	item, err = txStruct.LPop(list1)
	assert.Assert(t, err == nil && bytes.Equal(item, []byte("item1")))

	err = txStruct.RPush(list1, []byte("item1"), []byte("item2"))
	assert.Assert(t, err == nil)
	l, err = txStruct.LLen(list1)
	assert.Assert(t, err == nil && l == 2)

	item, err = txStruct.LPop(list1)
	assert.Assert(t, err == nil && bytes.Equal(item, []byte("item1")))

	item, err = txStruct.LPop(list1)
	assert.Assert(t, err == nil && bytes.Equal(item, []byte("item2")))

	l, err = txStruct.LLen(list1)
	assert.Assert(t, err == nil && l == 0)

	err = txn.Commit()
	assert.Assert(t, err == nil)
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

func TestWB(t *testing.T) {

	providers := []func() mondis.KVDB{
		provider.NewBadger, provider.NewLevelDB,
	}

	for _, provider := range providers {
		{
			os.RemoveAll(dataDir)
			kvdb := provider()
			err := kvdb.Open(mondis.KVOption{Dir: dataDir})
			assert.Assert(t, err == nil)

			b := kvdb.WriteBatch()
			k := []byte("k")
			v := []byte("v")
			err = b.Set(k, v)
			assert.Assert(t, err == nil)
			err = b.Commit()
			assert.Assert(t, err == nil)

			vget, _, err := kvdb.Get(k)
			assert.Assert(t, err == nil && reflect.DeepEqual(vget, v))

			assert.Assert(t, kvdb.Close() == nil)
		}
	}

}
