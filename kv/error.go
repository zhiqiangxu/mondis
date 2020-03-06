package kv

import "errors"

var (
	// ErrTxnTooBig when transaction too big
	ErrTxnTooBig = errors.New("transaction too big")
	// ErrKeyNotFound when key not found
	ErrKeyNotFound = errors.New("key not found")
)
