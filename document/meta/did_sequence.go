package meta

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta/sequence"
)

const defaultDIDBandWidth = 1000

// NewDocumentIDSequence creates a sequence for collection documents
func NewDocumentIDSequence(kvdb mondis.KVDB, bandwidth, dbID, cid int64) (*sequence.Hash, error) {
	if bandwidth <= 0 {
		bandwidth = defaultDIDBandWidth
	}

	dbKey := dbKeyByID(dbID)
	didSequenceKey := didSequenceKeyByID(cid)

	return sequence.NewHash(kvdb, dbKey, didSequenceKey, bandwidth)
}
