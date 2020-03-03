package document

import (
	"github.com/zhiqiangxu/mondis/document/memcomparable"
)

// Key for document database
type Key []byte

const (
	basePrefix                = "_md_"
	collectionPrefix          = basePrefix + "c"
	documentPrefix            = "_d" // stores all collection documents
	indexPrefix               = "_i" // stores all collection indexes
	metaPrefix                = basePrefix + "m"
	sequencePrefix            = "_s" // stores latest sequence id of all keywords
	metaSequencePrefix        = metaPrefix + sequencePrefix
	cidPrefix                 = "_cid" // stores cid of collections
	metaCIDPrefix             = metaPrefix + cidPrefix
	reservedKeywordCollection = "collection"
	collectionIDBandWidth     = 50
	documentIDBandWidth       = 1000
)

// AppendCollectionDocumentPrefix appends c[cid]_d to buf
func AppendCollectionDocumentPrefix(buf []byte, cid int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(documentPrefix))
	}
	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, documentPrefix...)
	return buf
}

// GetCollectionDocumentKey returns c[cid]_d[did]
func GetCollectionDocumentKey(cid, did int64) Key {
	buf := make([]byte, 0, len(collectionPrefix)+8+len(documentPrefix)+8)
	buf = AppendCollectionDocumentPrefix(buf, cid)
	buf = memcomparable.EncodeInt64(buf, did)
	return buf
}

// AppendCollectionIndexPrefix appends c[cid]_i to buf
func AppendCollectionIndexPrefix(buf []byte, cid int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(indexPrefix))
	}
	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexPrefix...)
	return buf
}

// AppendSequenceKey appends m_s[keyword] to buf
func AppendSequenceKey(buf, keyword []byte) Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaSequencePrefix)+len(keyword))
	}
	buf = append(buf, metaSequencePrefix...)
	buf = append(buf, keyword...)
	return buf
}

// AppendCIDKey appends m_cid[cname] to buf
func AppendCIDKey(buf []byte, cname string) Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaCIDPrefix)+len(cname))
	}
	buf = append(buf, metaCIDPrefix...)
	buf = append(buf, cname...)
	return buf
}
