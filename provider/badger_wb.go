package provider

import "github.com/dgraph-io/badger"

type badgerWB badger.WriteBatch

func (wb *badgerWB) Set(k, v []byte) error {
	return (*badger.WriteBatch)(wb).Set(k, v)
}

func (wb *badgerWB) Delete(key []byte) error {
	return (*badger.WriteBatch)(wb).Delete(key)
}

func (wb *badgerWB) Commit() error {
	return (*badger.WriteBatch)(wb).Flush()
}

func (wb *badgerWB) Discard() {
	(*badger.WriteBatch)(wb).Cancel()
}
