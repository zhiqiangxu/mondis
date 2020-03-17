package domain

// Collection model
type Collection struct {
	dbName         string
	collectionName string
	do             *Domain
}

func newCollection(dbName, collectionName string, do *Domain) *Collection {
	return &Collection{dbName: dbName, collectionName: collectionName, do: do}
}

// Index for find an index by name
func (collection *Collection) Index(name string) (idx *Index, err error) {
	schemaCache := collection.do.handle.Get()
	if schemaCache == nil {
		err = ErrIndexNotExists
		return
	}

	if !schemaCache.CheckIndexExists(collection.dbName, collection.collectionName, name) {
		err = ErrIndexNotExists
		return
	}

	idx = newIndex(collection.dbName, collection.collectionName, name, collection.do)
	return
}
