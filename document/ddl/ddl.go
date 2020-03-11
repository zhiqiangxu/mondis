package ddl

// DDL is responsible for updating schema in data store and maintaining in-memory schema cache.
type DDL struct {
}

// CreateSchema for create db
func (d *DDL) CreateSchema(input CreateSchemaInput) (err error) {
	return
}

// DropSchema for drop db
func (d *DDL) DropSchema() (err error) {
	return
}
