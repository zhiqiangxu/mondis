package domain

// DB model
type DB struct {
	Name string
	do   *Domain
}

func newDB(name string, do *Domain) *DB {
	return &DB{Name: name, do: do}
}

// Collection for find a collection by name
func (db *DB) Collection(name string) (collection *Collection, err error) {

	schemaCache := db.do.handle.Get()
	if schemaCache == nil {
		err = ErrCollectionNotExists
		return
	}

	if !schemaCache.CheckCollectionExists(db.Name, name) {
		err = ErrCollectionNotExists
		return
	}

	collection = newCollection(db.Name, name, db.do)
	return
}
