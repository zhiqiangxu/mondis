package document

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/zhiqiangxu/mondis/document/compact"
	"github.com/zhiqiangxu/mondis/document/memcomparable"
	"github.com/zhiqiangxu/util"
)

// Key for document database
type Key []byte

const (
	basePrefix                = "_md_"
	collectionPrefix          = basePrefix + "c"
	collectionPrefixLen       = len(collectionPrefix)
	documentPrefix            = "_d"  // stores all collection documents
	indexDataPrefix           = "_id" // stores all collection index data
	columnsIndexedPrefix      = "_ci" // stores all columns with index
	indexNamePrefix           = "_in" // stores index name => index id
	indexNamePrefixLen        = len(indexNamePrefix)
	metaPrefix                = basePrefix + "m"
	sequencePrefix            = "_s" // stores latest sequence id of all keywords
	metaSequencePrefix        = metaPrefix + sequencePrefix
	cName2IDPrefix            = "_cn2id" // stores collection name => collection id
	metaCName2IDPrefix        = metaPrefix + cName2IDPrefix
	cID2NamePrefix            = "_cid2n" // stores collection id => collection name
	metaCID2NamePrefix        = metaPrefix + cID2NamePrefix
	indexPrefix               = "_i" // stores index id => index definition
	metaIndexPrefix           = metaPrefix + indexPrefix
	reservedKeywordCollection = "collection"
	reservedKeywordIndex      = "index"
	collectionIDBandWidth     = 50
	indexIDBandWidth          = 50
	documentIDBandWidth       = 1000
)

var (
	reservedKeywordCollectionBytes = []byte(reservedKeywordCollection)
	reservedKeywordIndexBytes      = []byte(reservedKeywordIndex)
	collectionPrefixBytes          = []byte(collectionPrefix)
	indexNamePrefixBytes           = []byte(indexNamePrefix)
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

// EncodeCollectionDocumentKey returns c[cid]_d[did]
func EncodeCollectionDocumentKey(buf []byte, cid, did int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(documentPrefix)+8)
	}

	buf = AppendCollectionDocumentPrefix(buf, cid)
	buf = memcomparable.EncodeInt64(buf, did)
	return buf
}

// AppendCollectionIndexDataPrefix appends c[cid]_id to buf
func AppendCollectionIndexDataPrefix(buf []byte, cid int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(indexDataPrefix))
	}
	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexDataPrefix...)
	return buf
}

// EncodeCollectionColumnsIndexedKey return c[cid]_ci[sorted fields]
func EncodeCollectionColumnsIndexedKey(buf []byte, cid int64, fields []IndexField) Key {

	if buf == nil {
		bufLen := len(collectionPrefix) + 8 + len(columnsIndexedPrefix)
		for _, field := range fields {
			bufLen += binary.MaxVarintLen64 + binary.MaxVarintLen64 + len(field.Name)
		}
		buf = make([]byte, 0, bufLen)
	}

	if len(fields) == 1 {
		fields[0].Desc = false
	} else {
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Name < fields[j].Name
		})
	}

	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, columnsIndexedPrefix...)

	var desc uint64
	for _, field := range fields {
		if field.Desc {
			desc = 1
		} else {
			desc = 0
		}
		buf = compact.EncodeUvarint(buf, desc)
		buf = compact.EncodeBytes(buf, util.Slice(field.Name))
	}
	return buf
}

// EncodeMetaSequenceKey returns m_s[keyword]
func EncodeMetaSequenceKey(buf, keyword []byte) Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaSequencePrefix)+len(keyword))
	}
	buf = append(buf, metaSequencePrefix...)
	buf = append(buf, keyword...)
	return buf
}

// EncodeMetaCollectionName2IDKey returns m_cn2id[cname] to buf
func EncodeMetaCollectionName2IDKey(buf []byte, cname string) Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaCName2IDPrefix)+memcomparable.EncodedBytesLength(util.Slice(cname)))
	}
	buf = append(buf, metaCName2IDPrefix...)
	buf = memcomparable.EncodeBytes(buf, util.Slice(cname))
	return buf
}

// EncodeMetaIndexKey returns m_i[iid]
func EncodeMetaIndexKey(buf []byte, iid int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(metaIndexPrefix)+8)
	}
	buf = append(buf, metaIndexPrefix...)
	buf = memcomparable.EncodeInt64(buf, iid)
	return buf
}

// AppendCollectionIndexNamePrefix appends c_in[iname] to buf
func AppendCollectionIndexNamePrefix(buf []byte, cid int64) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(indexNamePrefix))
	}
	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexNamePrefix...)
	return buf
}

// EncodeCollectionIndexName2IDKey return c_in[iname]
func EncodeCollectionIndexName2IDKey(buf []byte, cid int64, iname string) Key {
	if buf == nil {
		buf = make([]byte, 0, len(collectionPrefix)+8+len(indexNamePrefix)+memcomparable.EncodedBytesLength(util.Slice(iname)))
	}
	buf = append(buf, collectionPrefix...)
	buf = memcomparable.EncodeInt64(buf, cid)
	buf = append(buf, indexNamePrefix...)
	buf = memcomparable.EncodeBytes(buf, util.Slice(iname))
	return buf
}

func hasCollectionPrefix(key Key) bool {
	return bytes.HasPrefix(key, collectionPrefixBytes)
}

func hasIndexNamePrefix(key Key) bool {
	return bytes.HasPrefix(key, indexNamePrefixBytes)
}

// DecodeCollectionIndexName2IDKey is reverse for EncodeCollectionIndexName2IDKey
func DecodeCollectionIndexName2IDKey(key Key) (cid int64, iname []byte, err error) {
	if len(key) <= len(collectionPrefix)+8+len(indexNamePrefix) {
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
