package util

import (
	"time"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/kv"
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
		if err != kv.ErrTxnTooBig {
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

// RunInNewUpdateTxnWithCancel will call cancelFunc when Commit failed
func RunInNewUpdateTxnWithCancel(kvdb mondis.KVDB, f func(mondis.ProviderTxn) error, cancelFunc func()) (err error) {
	txn := kvdb.NewTransaction(true)
	defer txn.Discard()

	err = f(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		cancelFunc()
	}
	return
}

// RunInNewUpdateTxnWithCallback will call afterCommitFunc after Commit succeeded
func RunInNewUpdateTxnWithCallback(kvdb mondis.KVDB, f func(mondis.ProviderTxn) error, afterCommitFunc func()) (err error) {
	txn := kvdb.NewTransaction(true)
	defer txn.Discard()

	err = f(txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err == nil && afterCommitFunc != nil {
		afterCommitFunc()
	}
	return
}

// RunInNewUpdateTxn for run f in a new update transaction
func RunInNewUpdateTxn(kvdb mondis.KVDB, f func(mondis.ProviderTxn) error) (err error) {
	txn := kvdb.NewTransaction(true)
	defer txn.Discard()

	err = f(txn)
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

// ChooseTime for choosing between base and max
func ChooseTime(base, max time.Duration) time.Duration {
	if base == 0 || base > max {
		return max
	}
	return base
}
