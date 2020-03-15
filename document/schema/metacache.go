package schema

import (
	"fmt"

	"github.com/zhiqiangxu/mondis/document/model"
)

// MetaCache is cache for document db meta
type MetaCache struct {
	version int64
	dbs     map[string]*model.DBInfo
}

// Version getter
func (c *MetaCache) Version() int64 {
	return c.version
}

// Clone for deep copy
func (c *MetaCache) Clone() *MetaCache {
	if c == nil {
		return &MetaCache{}
	}
	clone := &MetaCache{version: c.version, dbs: make(map[string]*model.DBInfo)}
	for dbName, dbInfo := range c.dbs {
		clone.dbs[dbName] = dbInfo.Clone()
	}
	return clone
}

// ApplyDiff for calculating MetaCache by SchemaDiff
func (c *MetaCache) ApplyDiff(diff *model.SchemaDiff) {
	return
}

func (c *MetaCache) addSchema(version int64, dbInfo *model.DBInfo) (nc *MetaCache, err error) {
	if c.dbs[dbInfo.Name] != nil {
		err = fmt.Errorf("db %s exists in meta cache", dbInfo.Name)
		return
	}

	nc.version = version

	nc.dbs[dbInfo.Name] = dbInfo
	return
}
