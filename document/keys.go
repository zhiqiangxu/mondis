package document

import "github.com/zhiqiangxu/kvrpc/document/memcomparable"

func sequenceKey(keyword []byte) []byte {
	return append([]byte(metaSequencePrefix), keyword...)
}

func documentKey(cid uint64, pk uint64) []byte {
	return memcomparable.EncodeUint64(memcomparable.EncodeUint64([]byte(collectionDocumentPrefix), cid), pk)
}
