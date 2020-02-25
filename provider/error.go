package provider

import "errors"

var (
	// ErrTxnTooBig when transaction too big
	ErrTxnTooBig = errors.New("transaction too big")
)
