package mondis

import "time"

type (

	// KVDB is the single interface that a key value database needs to implement to integrate into mondis
	KVDB interface {
		ProviderKVOP
		Open(option KVOption) error
		Close() error
		NewTransaction(update bool) ProviderTxn
	}

	// ProviderKVOP is KVOP for provider
	ProviderKVOP interface {
		CommonKVOP
		// key and value is only valid before fn returns
		Scan(option ProviderScanOption, fn func(key []byte, value []byte, meta VMetaResp) bool) error
	}

	// ProviderTxn for Txn for provider
	ProviderTxn interface {
		ProviderKVOP
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
