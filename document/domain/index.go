package domain

// Index model
type Index struct {
	dbName         string
	collectionName string
	indexName      string
	do             *Domain
}

func newIndex(dbName, collectionName, indexName string, do *Domain) *Index {
	return &Index{dbName: dbName, collectionName: collectionName, indexName: indexName, do: do}
}

// Lookup by index
func (idx *Index) Lookup() {

}
