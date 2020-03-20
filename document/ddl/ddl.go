package ddl

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
)

const (
	maxJobsInQueue = 100
)

var (
	// ErrJobsInQueueExceeded used by DDL
	ErrJobsInQueueExceeded = errors.New("ddl jobs in queue exceeded")
	// ErrDBAlreadyExists used by DDL
	ErrDBAlreadyExists = errors.New("db already exists")
	// ErrCollectionNotExists used by DDL
	ErrCollectionNotExists = errors.New("collection not exists")
	// ErrDBNotExists used by DDL
	ErrDBNotExists = errors.New("db not exists")
	// ErrIndexAlreadyExists used by DDL
	ErrIndexAlreadyExists = errors.New("index already exists")
	// ErrIndexNotExists used by DDL
	ErrIndexNotExists = errors.New("index not exists")
	// ErrInvalidDDLState used by DDL
	ErrInvalidDDLState = errors.New("invalid ddl state")
)

// DDL is responsible for updating schema in data store and maintaining in-memory schema cache.
type DDL struct {
	kvdb    mondis.KVDB
	options Options
	workers map[workerType]*worker
}

// New is ctor for DDL
func New(kvdb mondis.KVDB, options Options) *DDL {
	ddl := &DDL{
		kvdb:    kvdb,
		options: options,
		workers: make(map[workerType]*worker),
	}

	ddl.start()

	return ddl
}

func (d *DDL) start() {
	d.workers[defaultWorkerType] = newWorker(defaultWorkerType, d)

	for _, w := range d.workers {
		go w.start()
	}
}
