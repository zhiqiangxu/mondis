package ddl

import "github.com/zhiqiangxu/mondis/document/model"

// CreateSchemaInput for CreateSchema
type CreateSchemaInput struct {
	DB          string
	Collections []string
	Indexes     map[string][]IndexInfo
}

// DropSchemaInput for DropSchema
type DropSchemaInput struct {
	DB string
}

// CreateCollectionInput for CreateCollection
type CreateCollectionInput struct {
	DB         string
	Collection string
	Indexes    []IndexInfo
}

// DropCollectionInput for DropCollection
type DropCollectionInput struct {
	DB         string
	Collection string
}

// AddIndexInput for AddIndex
type AddIndexInput struct {
	DB         string
	Collection string
	IndexInfo  IndexInfo
}

// DropIndexInput for DropIndex
type DropIndexInput struct {
	DB         string
	Collection string
	IndexName  string
}

// IndexInfo for ddl input
// basically model.IndexInfo minus state
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
	Primary bool
}

// ToModel converts IndexInfo to *model.IndexInfo
func (ii *IndexInfo) ToModel() *model.IndexInfo {
	mii := &model.IndexInfo{
		Name:    ii.Name,
		Unique:  ii.Unique,
		Primary: ii.Primary,
	}
	if len(ii.Columns) > 0 {
		mii.Columns = make([]string, len(ii.Columns))
		for i, column := range ii.Columns {
			mii.Columns[i] = column
		}
	}

	return mii
}
