package ddl

import (
	"fmt"

	"github.com/zhiqiangxu/mondis/document/model"
)

// CreateSchemaInput for CreateSchema
type CreateSchemaInput struct {
	DB          string
	Collections []string
	Indices     map[string][]IndexInfo
}

// Validate CreateSchemaInput
func (in *CreateSchemaInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	for _, cn := range in.Collections {
		if cn == "" {
			err = fmt.Errorf("collection empty")
			return
		}
	}
	for _, indexInfos := range in.Indices {
		for _, indexInfo := range indexInfos {
			err = indexInfo.Validate()
			if err != nil {
				return
			}
		}
	}
	return
}

// DropSchemaInput for DropSchema
type DropSchemaInput struct {
	DB string
}

// Validate DropSchemaInput
func (in *DropSchemaInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	return
}

// CreateCollectionInput for CreateCollection
type CreateCollectionInput struct {
	DB         string
	Collection string
	Indices    []IndexInfo
}

// Validate CreateCollectionInput
func (in *CreateCollectionInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	if in.Collection == "" {
		err = fmt.Errorf("collection empty")
		return
	}

	for _, indexInfo := range in.Indices {
		err = indexInfo.Validate()
		if err != nil {
			return
		}
	}
	return
}

// DropCollectionInput for DropCollection
type DropCollectionInput struct {
	DB         string
	Collection string
}

// Validate DropCollectionInput
func (in *DropCollectionInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	if in.Collection == "" {
		err = fmt.Errorf("collection empty")
		return
	}
	return
}

// AddIndexInput for AddIndex
type AddIndexInput struct {
	DB         string
	Collection string
	IndexInfo  IndexInfo
}

// Validate AddIndexInput
func (in *AddIndexInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	if in.Collection == "" {
		err = fmt.Errorf("collection empty")
		return
	}
	err = in.IndexInfo.Validate()
	return
}

// DropIndexInput for DropIndex
type DropIndexInput struct {
	DB         string
	Collection string
	IndexName  string
}

// Validate DropIndexInput
func (in *DropIndexInput) Validate() (err error) {
	if in.DB == "" {
		err = fmt.Errorf("db empty")
		return
	}
	if in.Collection == "" {
		err = fmt.Errorf("collection empty")
		return
	}
	if in.IndexName == "" {
		err = fmt.Errorf("index name empty")
		return
	}
	return
}

// IndexInfo for ddl input
// basically model.IndexInfo minus state
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
}

// Validate IndexInfo
func (ii *IndexInfo) Validate() (err error) {
	if ii.Name == "" {
		err = fmt.Errorf("index name empty")
		return
	}
	if len(ii.Columns) == 0 {
		err = fmt.Errorf("index columns empty")
		return
	}
	return
}

// ToModel converts IndexInfo to *model.IndexInfo
func (ii *IndexInfo) ToModel() *model.IndexInfo {
	mii := &model.IndexInfo{
		Name:   ii.Name,
		Unique: ii.Unique,
	}
	if len(ii.Columns) > 0 {
		mii.Columns = make([]string, len(ii.Columns))
		for i, column := range ii.Columns {
			mii.Columns[i] = column
		}
	}

	return mii
}
