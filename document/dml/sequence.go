package dml

import (
	"errors"
	"sync"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/config"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/meta/sequence"
)

var (
	sequenceMap sync.Map
	// ErrSequenceNotExists when sequence not exists
	ErrSequenceNotExists = errors.New("sequence not exists")
	// ErrSequenceAlreadyExists used by CreateSequence
	ErrSequenceAlreadyExists = errors.New("sequence already exists")
)

// GetSequence for collection, thread safe
func GetSequence(cid int64) *sequence.Hash {
	v, _ := sequenceMap.Load(cid)

	ret, _ := v.(*sequence.Hash)
	return ret
}

// CreateSequence by cid, non thread safe
func CreateSequence(kvdb mondis.KVDB, dbID, cid, bandwidth int64) (err error) {
	_, exists := sequenceMap.Load(cid)
	if exists {
		err = ErrSequenceAlreadyExists
		return
	}

	seq, err := meta.NewDocIDSequence(kvdb, dbID, cid, bandwidth)
	if err != nil {
		return
	}

	_, loaded := sequenceMap.LoadOrStore(cid, seq)
	if loaded {
		err = ErrSequenceAlreadyExists
		return
	}
	return
}

// DropSequence by cid, non thread safe
func DropSequence(cid int64) (err error) {
	v, exists := sequenceMap.Load(cid)
	if !exists {
		err = ErrSequenceNotExists
		return
	}

	err = v.(*sequence.Hash).Close(config.Load().Lease == 0)
	return
}

// DropSequenceIfExists do nothing if sequence not exists
func DropSequenceIfExists(cid int64) (err error) {
	v, exists := sequenceMap.Load(cid)
	if !exists {
		return
	}

	err = v.(*sequence.Hash).Close(config.Load().Lease == 0)
	return
}
