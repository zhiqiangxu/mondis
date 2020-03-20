package ddl

import (
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
)

func checkDBNameNotExists(m *meta.Meta, dbName string) (exists bool, err error) {
	dbi, err := getDbInfo(m, dbName)
	if err != nil {
		return
	}

	if dbi != nil {
		err = ErrDBAlreadyExists
		return
	}
	return
}

func getDbInfo(m *meta.Meta, dbName string) (dbInfo *model.DBInfo, err error) {
	dbInfos, err := m.ListDatabases()
	if err != nil {
		return
	}

	for _, dbi := range dbInfos {
		if dbi.Name == dbName {
			dbInfo = dbi
			return
		}
	}
	return
}

func checkIndexNameNotExists(m *meta.Meta, dbName, collectionName, indexName string) (exists bool, err error) {
	dbi, err := getDbInfo(m, dbName)
	if err != nil {
		return
	}

	if dbi == nil {
		err = ErrDBNotExists
		return
	}

	ci := dbi.CollectionInfo(collectionName)
	if ci == nil {
		err = ErrCollectionNotExists
		return
	}
	exists = ci.IndexExists(indexName)
	return
}
