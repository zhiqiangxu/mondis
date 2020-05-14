package mondis

import "time"

type (

	// KVDB is the single interface that a key value database needs to implement to integrate into mondis
	KVDB interface {
		ProviderKVOP
		Open(option KVOption) error
		Close() error
		WriteBatch() ProviderWriteBatch
		NewTransaction(update bool) ProviderTxn
	}

	// ProviderKVOP is KVOP for provider
	ProviderKVOP interface {
		CommonKVOP
		// key and value is only valid before fn returns
		Scan(option ProviderScanOption, fn func(key []byte, value []byte, meta VMetaResp) bool) error
	}

	// ProviderWriteBatch is WriteBatch for provider
	ProviderWriteBatch interface {
		Set(k, v []byte) error
		Delete(key []byte) error
		Commit() error
		Discard()
	}

	// ProviderTxn is Txn for provider
	ProviderTxn interface {
		ProviderKVOP
		StartTS() uint64 // not used yet
		Commit() error
		Discard()
	}

	// CommonKVOP for common operations on kv
	CommonKVOP interface {
		Set(k, v []byte, meta *VMetaReq) error
		Exists(k []byte) (bool, error) // maybe cheaper than Get
		Get(k []byte) ([]byte, VMetaResp, error)
		Delete(key []byte) error
	}

	// KVOption for KVDB
	KVOption struct {
		Dir string
	}

	// ProviderScanOption is scan options for provider
	ProviderScanOption struct {
		// Direction of iteration. False is forward, true is backward.
		Reverse bool
		// Only iterate over this given prefix.
		Prefix []byte
		// Seek would seek to the provided key if present. If absent, it would seek to the next
		// smallest key greater than the provided key if iterating in the forward direction.
		// Behavior would be reversed if iterating backwards.
		Offset []byte
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
)
