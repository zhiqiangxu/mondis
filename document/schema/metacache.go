package schema

import (
	"fmt"

	"github.com/zhiqiangxu/mondis/document/model"
)

const (
	maxDiffs = 100
)

// MetaCache is cache for document db meta
type MetaCache struct {
	version          int64
	dbs              map[string]*model.DBInfo
	schemaDiffs      [][]int64
	diffStartVersion int64
}

// NewMetaCache is ctor for MetaCache
func NewMetaCache(version int64, dbInfos []*model.DBInfo) *MetaCache {
	c := &MetaCache{version: version, diffStartVersion: version + 1}
	for _, dbInfo := range dbInfos {
		c.dbs[dbInfo.Name] = dbInfo.Clone()
	}
	return c
}

// Version getter
func (c *MetaCache) Version() int64 {
	return c.version
}

// Clone for deep copy
func (c *MetaCache) Clone() *MetaCache {
	if c == nil {
		return &MetaCache{diffStartVersion: 1}
	}
	clone := *c
	clone.dbs = make(map[string]*model.DBInfo)
	clone.schemaDiffs = make([][]int64, len(c.schemaDiffs))
	for dbName, dbInfo := range c.dbs {
		clone.dbs[dbName] = dbInfo.Clone()
	}
	for i := 0; i < len(c.schemaDiffs); i++ {
		diff := c.schemaDiffs[i]
		clone.schemaDiffs[i] = append(diff[:0:0], diff...)
	}
	return &clone
}

// ApplyDiffs for apply SchemaDiffs to MetaCache
func (c *MetaCache) ApplyDiffs(diffs []*model.SchemaDiff) (err error) {

	err = c.validateDiffs(diffs)
	if err != nil {
		return
	}

	for _, diff := range diffs {
		collectionIDs := append(diff.CollectionIDs[:0:0], diff.CollectionIDs...)
		c.schemaDiffs = append(c.schemaDiffs, collectionIDs)

		switch diff.Type {
		case model.ActionCreateSchema:

			err = c.onCreateSchema(diff)
			if err != nil {
				return
			}
		default:
			err = fmt.Errorf("can not apply diff type %d", diff.Type)
			return
		}
	}

	if len(c.schemaDiffs) > maxDiffs {
		rmCount := len(c.schemaDiffs) - maxDiffs
		c.schemaDiffs = c.schemaDiffs[rmCount:]
		c.diffStartVersion += int64(rmCount)
	}

	return
}

func (c *MetaCache) validateDiffs(diffs []*model.SchemaDiff) (err error) {
	// diffs should be consecutive
	for i := 0; i < len(diffs)-1; i++ {
		if diffs[i].Version+1 != diffs[i+1].Version {
			err = fmt.Errorf("diff not consecutive")
			return
		}
	}

	if diffs[0].Version != c.diffStartVersion+int64(len(c.schemaDiffs)) {
		err = fmt.Errorf("diff0 not consecutive")
		return
	}

	return
}

func (c *MetaCache) onCreateSchema(diff *model.SchemaDiff) (err error) {
	var dbInfo model.DBInfo
	err = diff.DecodeArg(&dbInfo)
	if err != nil {
		return
	}

	if c.dbs[dbInfo.Name] != nil {
		err = fmt.Errorf("db %s exists in meta cache", dbInfo.Name)
		return
	}

	c.version = diff.Version

	c.dbs[dbInfo.Name] = &dbInfo
	return
}
