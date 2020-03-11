package domain

import (
	"sync"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/model"
)

// Domain represents a storage space
type Domain struct {
	kvdb mondis.KVDB
	mu   struct {
		sync.RWMutex
		dbs map[int64]*model.DBInfo
	}
	ddl *ddl.DDL
}
