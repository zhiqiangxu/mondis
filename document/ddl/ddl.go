package ddl

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/dml"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/util"
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

	return ddl
}

func (d *DDL) start() {
	d.workers[defaultWorkerType] = newWorker(defaultWorkerType, d)

	for _, w := range d.workers {
		go w.start()
	}
}

// Init DDL
func (d *DDL) Init() (err error) {
	err = util.RunInNewUpdateTxn(d.kvdb, func(txn mondis.ProviderTxn) (err error) {
		m := meta.NewMeta(txn)
		dbs, err := m.ListDatabases()
		if err != nil {
			return
		}

		for _, db := range dbs {
			for _, ci := range db.Collections {
				err = dml.CreateSequence(d.kvdb, db.ID, ci.ID, 0)
				if err != nil {
					return
				}
			}
		}
		return
	})

	if err != nil {
		return
	}
	d.start()
	return
}
