package ddl

import "github.com/zhiqiangxu/mondis/document/model"

// CreateSchemaInput for CreateSchema
type CreateSchemaInput struct {
	DB          string
	Collections []string
	Indexes     map[string][]*model.IndexInfo
}

// DropSchemaInput for DropSchema
type DropSchemaInput struct {
	DB string
}
