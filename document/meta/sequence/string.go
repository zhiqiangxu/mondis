package sequence

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/keyspace"
	"github.com/zhiqiangxu/mondis/structure"
)

// String for allocating auto incrementing pk using string value
type String struct {
	Sequence
}

// NewString is ctor for String
func NewString(kvdb mondis.KVDB, keyword []byte, bandwidth int64) (s *String, err error) {
	if len(keyword) == 0 {
		err = ErrEmptyKeywordForStringSequence
		return
	}

	if bandwidth == 0 {
		err = ErrZeroBandwidth
		return
	}

	s = &String{Sequence{bandwidth: bandwidth}}
	s.renewLeaseFunc = func(step int64) (err error) {
		if step == 0 {
			step = s.bandwidth
		}
		txn := kvdb.NewTransaction(true)
		defer txn.Discard()

		txStruct := structure.New(txn, keyspace.MetaPrefixBytes)
		leased, err := txStruct.Inc(keyword, step)
		if err != nil {
			return
		}
		err = txn.Commit()
		if err != nil {
			return
		}

		s.leased = leased
		s.next = leased - step
		return
	}
	s.clearFunc = func() (err error) {
		txn := kvdb.NewTransaction(true)
		defer txn.Discard()

		txStruct := structure.New(txn, keyspace.MetaPrefixBytes)
		err = txStruct.Clear(keyword)
		if err != nil {
			return
		}

		err = txn.Commit()
		if err != nil {
			return
		}

		s.next = 0
		s.leased = 0
		return
	}
	s.updateLeasedFunc = func() (err error) {
		txn := kvdb.NewTransaction(true)
		defer txn.Discard()

		txStruct := structure.New(txn, keyspace.MetaPrefixBytes)
		err = txStruct.SetInt64(keyword, s.next)
		if err != nil {
			return
		}

		err = txn.Commit()
		if err != nil {
			return
		}

		s.leased = s.next
		return
	}

	err = s.renewLeaseFunc(0)

	return
}
