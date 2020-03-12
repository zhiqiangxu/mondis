package ddl

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
