package ddl

import (
	"context"
	"errors"

	"github.com/zhiqiangxu/mondis"
)

const (
	maxDDLJobsInQueue = 100
)

var (
	// ErrDDLJobsInQueueExceeded used by DDL
	ErrDDLJobsInQueueExceeded = errors.New("jobs in queue exceeded")
)

// DDL is responsible for updating schema in data store and maintaining in-memory schema cache.
type DDL struct {
	kvdb    mondis.KVDB
	options Options
}

// New is ctor for DDL
func New(kvdb mondis.KVDB, options Options) *DDL {
	return &DDL{kvdb: kvdb, options: options}
}

// CreateSchema for create db
func (d *DDL) CreateSchema(ctx context.Context, input CreateSchemaInput) error {
	return d.onCreateSchema(ctx, input)
}

// DropSchema for drop db
func (d *DDL) DropSchema() (err error) {
	return
}
