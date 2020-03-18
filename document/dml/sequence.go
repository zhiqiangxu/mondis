package dml

import (
	"sync"

	"github.com/zhiqiangxu/mondis/document/meta/sequence"
)

var sequenceMap sync.Map

// GetSequence for collection, thread safe
func GetSequence(cid int64) *sequence.Hash {
	v, _ := sequenceMap.Load(cid)

	ret, _ := v.(*sequence.Hash)
	return ret
}

// CreateSequence by cid, non thread safe
func CreateSequence(cid int64) (err error) {
	return
}

// DropSequence by cid, non thread safe
func DropSequence(cid int64) (err error) {
	return
}
