package kvrpc

import "time"

type (
	// KVOption for KVDB
	KVOption struct {
		Dir string
	}
	// KVDB is the common interface that a key value database needs to implement to integrate into kvrpc
	KVDB interface {
		KVOP
		Open(option KVOption) error
		Close() error
		NewTransaction(update bool) Txn
	}

	// VMetaReq for set value meta
	VMetaReq struct {
		TTL time.Duration
		Tag byte
	}
	// VMetaResp is what you get back
	VMetaResp struct {
		ExpiresAt uint64
		Tag       byte
	}
	// KVOP for operations on kv
	KVOP interface {
		Set(k, v []byte, meta *VMetaReq) error
		Get(k []byte) ([]byte, VMetaResp, error)
		Delete(key []byte) error
	}

	// Txn for transaction
	Txn interface {
		KVOP
		Commit() error
		Discard()
	}
)
