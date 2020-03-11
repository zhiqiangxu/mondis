package sequence

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/keyspace"
	"github.com/zhiqiangxu/mondis/structure"
)

// Hash for allocating auto incrementing pk using hash value
type Hash struct {
	Sequence
}

// NewHash is ctor for Hash
func NewHash(kvdb mondis.KVDB, key, field []byte, bandwidth int64) (s *Hash, err error) {
	if len(key) == 0 {
		err = ErrEmptyKeyForHashSequence
		return
	}
	if len(field) == 0 {
		err = ErrEmptyFieldForHashSequence
		return
	}

	if bandwidth == 0 {
		err = ErrZeroBandwidth
		return
	}

	s = &Hash{Sequence{bandwidth: bandwidth}}
	s.renewLeaseFunc = func(step int64) (err error) {
		if step == 0 {
			step = s.bandwidth
		}
		txn := kvdb.NewTransaction(true)
		defer txn.Discard()

		txStruct := structure.New(txn, keyspace.MetaPrefixBytes)
		leased, err := txStruct.HInc(key, field, step)
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
		err = txStruct.HClear(key)
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
		err = txStruct.HSetInt64(key, field, s.next)
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
