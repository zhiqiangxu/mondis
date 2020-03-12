package meta

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta/sequence"
)

const defaultDIDBandWidth = 1000

// NewDocIDSequence creates a sequence for collection documents
func NewDocIDSequence(kvdb mondis.KVDB, dbID, cid, bandwidth int64) (*sequence.Hash, error) {
	if bandwidth <= 0 {
		bandwidth = defaultDIDBandWidth
	}

	dbKey := dbKeyByID(dbID)
	didSequenceKey := didSequenceKeyByID(cid)

	return sequence.NewHash(kvdb, dbKey, didSequenceKey, bandwidth)
}
