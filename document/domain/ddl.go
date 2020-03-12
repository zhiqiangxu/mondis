package domain

import "github.com/zhiqiangxu/mondis/document/domain/ddl"

// DDL is responsible for updating schema in data store and maintaining in-memory schema cache.
type DDL struct {
	do *Domain
}

func newDDL(do *Domain) *DDL {
	return &DDL{do: do}
}

// CreateSchema for create db
func (d *DDL) CreateSchema(input ddl.CreateSchemaInput) (err error) {
	return
}

// DropSchema for drop db
func (d *DDL) DropSchema() (err error) {
	return
}
