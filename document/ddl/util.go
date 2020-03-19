package ddl

import "github.com/zhiqiangxu/mondis/document/meta"

func checkDBNameNotExists(m *meta.Meta, dbName string) (exists bool, err error) {
	dbInfos, err := m.ListDatabases()
	if err != nil {
		return
	}

	for _, dbInfo := range dbInfos {
		if dbInfo.Name == dbName {
			exists = true
			return
		}
	}
	return
}
