package dml

import (
	"bytes"
	"fmt"

	"github.com/zhiqiangxu/mondis/document/keyspace"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/memcomparable"
	"github.com/zhiqiangxu/util"
)

const (
	collectionPrefixLen       = len(keyspace.CollectionPrefix)
	documentPrefix            = "_d" // stores all collection documents
	documentPrefixLen         = len(documentPrefix)
	indexDataPrefix           = "_id" // stores all collection index data
	columnsIndexedPrefix      = "_ci" // stores all columns with index
	indexNamePrefix           = "_in" // stores index name => index id
	indexNamePrefixLen        = len(indexNamePrefix)
	sequencePrefix            = "_s" // stores latest sequence id of all keywords
	metaSequencePrefix        = keyspace.MetaPrefix + sequencePrefix
	cName2IDPrefix            = "_cn2id" // stores collection name => collection id
	metaCName2IDPrefix        = keyspace.MetaPrefix + cName2IDPrefix
	cID2NamePrefix            = "_cid2n" // stores collection id => collection name
	metaCID2NamePrefix        = keyspace.MetaPrefix + cID2NamePrefix
	indexPrefix               = "_i" // stores index id => index definition
	metaIndexPrefix           = keyspace.MetaPrefix + indexPrefix
	reservedKeywordCollection = "collection"
	reservedKeywordIndex      = "index"
	collectionIDBandWidth     = 50
	indexIDBandWidth          = 50
	documentIDBandWidth       = 1000
)

var (
	reservedKeywordCollectionBytes = []byte(reservedKeywordCollection)
	reservedKeywordIndexBytes      = []byte(reservedKeywordIndex)
	indexNamePrefixBytes           = []byte(indexNamePrefix)
	documentPrefixBytes            = []byte(documentPrefix)
)

// AppendCollectionDocumentPrefix appends c[cid]_d to buf
func AppendCollectionDocumentPrefix(buf []byte, cid int64) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, collectionPrefixLen+8+len(documentPrefix))
	}
	buf = append(buf, keyspace.CollectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, documentPrefix...)
	return buf
}

// EncodeCollectionDocumentKey returns c[cid]_d[did]
func EncodeCollectionDocumentKey(buf []byte, cid, did int64) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, collectionPrefixLen+8+len(documentPrefix)+8)
	}

	buf = AppendCollectionDocumentPrefix(buf, cid)
	buf = memcomparable.EncodeInt64(buf, did)
	return buf
}

// AppendCollectionIndexDataPrefix appends c[cid]_id to buf
func AppendCollectionIndexDataPrefix(buf []byte, cid int64) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, collectionPrefixLen+8+len(indexDataPrefix))
	}
	buf = append(buf, keyspace.CollectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexDataPrefix...)
	return buf
}

// EncodeMetaSequenceKey returns m_s[keyword]
func EncodeMetaSequenceKey(buf, keyword []byte) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaSequencePrefix)+len(keyword))
	}
	buf = append(buf, metaSequencePrefix...)
	buf = append(buf, keyword...)
	return buf
}

// EncodeMetaCollectionName2IDKey returns m_cn2id[cname] to buf
func EncodeMetaCollectionName2IDKey(buf []byte, cname string) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaCName2IDPrefix)+memcomparable.EncodedBytesLength(len(cname)))
	}
	buf = append(buf, metaCName2IDPrefix...)
	buf = memcomparable.EncodeBytes(buf, util.Slice(cname))
	return buf
}

// EncodeMetaIndexKey returns m_i[iid]
func EncodeMetaIndexKey(buf []byte, iid int64) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaIndexPrefix)+8)
	}
	buf = append(buf, metaIndexPrefix...)
	buf = memcomparable.EncodeInt64(buf, iid)
	return buf
}

// AppendCollectionIndexNamePrefix appends c_in[iname] to buf
func AppendCollectionIndexNamePrefix(buf []byte, cid int64) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, collectionPrefixLen+8+len(indexNamePrefix))
	}
	buf = append(buf, keyspace.CollectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexNamePrefix...)
	return buf
}

// EncodeCollectionIndexName2IDKey return c_in[iname]
func EncodeCollectionIndexName2IDKey(buf []byte, cid int64, iname string) kv.Key {
	if buf == nil {
		buf = make([]byte, 0, collectionPrefixLen+8+len(indexNamePrefix)+memcomparable.EncodedBytesLength(len(iname)))
	}
	buf = append(buf, keyspace.CollectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexNamePrefix...)
	buf = memcomparable.EncodeBytes(buf, util.Slice(iname))
	return buf
}

func hasCollectionPrefix(key kv.Key) bool {
	return bytes.HasPrefix(key, keyspace.CollectionPrefixBytes)
}

func hasIndexNamePrefix(key kv.Key) bool {
	return bytes.HasPrefix(key, indexNamePrefixBytes)
}

func hasDocumentPrefix(key kv.Key) bool {
	return bytes.HasPrefix(key, documentPrefixBytes)
}

// DecodeCollectionDocumentKey is reverse of EncodeCollectionDocumentKey
func DecodeCollectionDocumentKey(key kv.Key) (cid, did int64, err error) {
	if len(key) < collectionPrefixLen+8+len(documentPrefix)+8 {
		err = fmt.Errorf("invalid collection document key - %q", key)
		return
	}

	if !hasCollectionPrefix(key) {
		err = fmt.Errorf("invalid collection document key - %q", key)
		return
	}

	key = key[collectionPrefixLen:]
	key, cid, err = memcomparable.DecodeInt64(key)
	if err != nil {
		return
	}

	if !hasDocumentPrefix(key) {
		err = fmt.Errorf("invalid collection document key - %q", key)
		return
	}

	key = key[documentPrefixLen:]
	key, did, err = memcomparable.DecodeInt64(key)
	if err != nil {
		err = fmt.Errorf("invalid collection name to id key - %q", key)
		return
	}
	return
}

// DecodeCollectionIndexName2IDKey is reverse for EncodeCollectionIndexName2IDKey
func DecodeCollectionIndexName2IDKey(key kv.Key) (cid int64, iname []byte, err error) {
	if len(key) <= collectionPrefixLen+8+len(indexNamePrefix) {
		err = fmt.Errorf("invalid collection name to id key - %q", key)
		return
	}

	k := key

	if !hasCollectionPrefix(key) {
		err = fmt.Errorf("invalid collection name to id key - %q", k)
		return
	}

	key = key[collectionPrefixLen:]
	key, cid, err = memcomparable.DecodeInt64(key)
	if err != nil {
		return
	}

	if !hasIndexNamePrefix(key) {
		err = fmt.Errorf("invalid collection name to id key - %q", k)
		return
	}

	key = key[indexNamePrefixLen:]
	key, iname, err = memcomparable.DecodeBytes(key, nil)
	if err != nil {
		err = fmt.Errorf("invalid collection name to id key - %q", k)
		return
	}

	if len(key) > 0 {
		err = fmt.Errorf("invalid collection name to id key - %q", k)
		return
	}

	return
}
