package provider

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type leveldbWB struct {
	batch *leveldb.Batch
	db    *leveldb.DB
}

func (wb *leveldbWB) Set(k, v []byte) error {
	wb.batch.Put(k, v)
	return nil
}

func (wb *leveldbWB) Delete(key []byte) error {
	wb.batch.Delete(key)
	return nil
}

func (wb *leveldbWB) Commit() error {
	return wb.db.Write(wb.batch, nil)
}

func (wb *leveldbWB) Discard() {

}
