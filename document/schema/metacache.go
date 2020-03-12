package schema

import "github.com/zhiqiangxu/mondis/document/model"

// MetaCache is cache for document db meta
type MetaCache struct {
	version int64
	dbs     map[string]*model.DBInfo
}
