package util

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/provider"
)

const (
	maxTry = 100
)

// TryCommitWhenTxnTooBig handles ErrTxnTooBig
func TryCommitWhenTxnTooBig(kvdb mondis.KVDB, txn mondis.ProviderTxn, f func(txn mondis.ProviderTxn) error) (finalTxn mondis.ProviderTxn, err error) {
	finalTxn = txn
	tried := 1
	for {
		err = f(finalTxn)
		if err != provider.ErrTxnTooBig {
			return
		}
		if tried >= maxTry {
			return
		}
		tried++
		err = finalTxn.Commit()
		if err != nil {
			return
		}
		finalTxn = kvdb.NewTransaction(true)
	}
}

// RunInNewUpdateTxn for run f in a new update transaction
func RunInNewUpdateTxn(kvdb mondis.KVDB, f func(mondis.ProviderTxn) error) (err error) {
	txn := kvdb.NewTransaction(true)
	defer txn.Discard()

	txn, err = TryCommitWhenTxnTooBig(kvdb, txn, f)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

// RunInNewTxn for run f in a new read-only transaction
func RunInNewTxn(kvdb mondis.KVDB, f func(mondis.ProviderTxn) error) (err error) {
	txn := kvdb.NewTransaction(false)
	defer txn.Discard()

	err = f(txn)

	return
}
