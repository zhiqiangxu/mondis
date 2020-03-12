package ddl

import (
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/util"
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
	kvdb mondis.KVDB
}

// New is ctor for DDL
func New(kvdb mondis.KVDB, options Options) *DDL {
	return &DDL{kvdb: kvdb}
}

// CreateSchema for create db
func (d *DDL) CreateSchema(input CreateSchemaInput) (err error) {
	n := 2 + len(input.Collections)
	err = util.RunInNewUpdateTxn(d.kvdb, func(txn mondis.ProviderTxn) (err error) {
		m := meta.NewMeta(txn)
		queueLength, err := m.DDLJobQueueLen()
		if err != nil {
			return
		}
		if queueLength > maxDDLJobsInQueue {
			err = ErrDDLJobsInQueueExceeded
			return
		}
		start, _, err := m.GenGlobalIDs(n)
		if err != nil {
			return
		}

		jobID := start + 1
		schemaID := start + 2
		nextID := schemaID
		dbInfo := model.DBInfo{
			ID:          schemaID,
			Name:        input.DB,
			Collections: make(map[string]*model.CollectionInfo),
		}
		for _, cn := range input.Collections {
			collectInfo := dbInfo.Collections[cn]
			if collectInfo == nil {
				collectInfo = &model.CollectionInfo{
					ID:   nextID + 1,
					Name: cn,
				}
				dbInfo.Collections[cn] = collectInfo
				nextID++
			}
			dbInfo.CollectionOrder = append(dbInfo.CollectionOrder, cn)
			if len(input.Indexes[cn]) > 0 {
				for _, indexInfo := range input.Indexes[cn] {
					collectInfo.Indices[indexInfo.Name] = indexInfo.ToModel()
					collectInfo.IndexOrder = append(collectInfo.IndexOrder, indexInfo.Name)
				}
			}
		}
		job := &model.Job{
			ID:   jobID,
			Type: model.ActionCreateSchema,
			Arg:  dbInfo,
		}

		err = m.EnQueueDDLJob(job)

		return
	})
	return
}

// DropSchema for drop db
func (d *DDL) DropSchema() (err error) {
	return
}
