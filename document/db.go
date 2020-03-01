package document

// DB defines a column db
type DB struct {
}

// Collection returns collection operator
func (d *DB) Collection(name string) *Collection {
	return nil
}
