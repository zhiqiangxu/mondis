package ddl

import (
	"context"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/util"
)

// CreateSchema for create db
func (d *DDL) CreateSchema(ctx context.Context, input CreateSchemaInput) (job *model.Job, err error) {
	err = input.Validate()
	if err != nil {
		return
	}

	n := 2 + len(input.Collections) + len(input.Indices)
	err = util.RunInNewUpdateTxn(d.kvdb, func(txn mondis.ProviderTxn) (err error) {
		m := meta.NewMeta(txn)
		queueLength, err := m.DDLJobQueueLen()
		if err != nil {
			return
		}
		if queueLength > maxJobsInQueue {
			err = ErrJobsInQueueExceeded
			return
		}

		exists, err := checkDBNameNotExists(m, input.DB)
		if err != nil {
			return
		}
		if exists {
			err = ErrDBAlreadyExists
			return
		}

		start, _, err := m.GenGlobalIDs(n)
		if err != nil {
			return
		}

		schemaID := start + 1
		nextID := schemaID
		dbInfo := &model.DBInfo{
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
				nextID++
				dbInfo.Collections[cn] = collectInfo
			}
			dbInfo.CollectionOrder = append(dbInfo.CollectionOrder, cn)
			if len(input.Indices[cn]) > 0 {
				for _, indexInfo := range input.Indices[cn] {
					iif := indexInfo.ToModel()
					iif.ID = nextID + 1
					nextID++
					collectInfo.Indices[indexInfo.Name] = iif
					collectInfo.IndexOrder = append(collectInfo.IndexOrder, indexInfo.Name)
				}
			}
		}

		job = &model.Job{
			ID:   nextID + 1,
			Type: model.ActionCreateSchema,
			Arg:  dbInfo,
		}

		err = m.EnQueueDDLJob(job)

		return
	})

	if err != nil {
		return
	}

	d.notifyWorker(job.Type)

	err = d.checkJob(ctx, job)
	return
}

// AddIndex for add index
func (d *DDL) AddIndex(ctx context.Context, input AddIndexInput) (job *model.Job, err error) {
	err = util.RunInNewUpdateTxn(d.kvdb, func(txn mondis.ProviderTxn) (err error) {
		m := meta.NewMeta(txn)

		exists, err := checkIndexNameNotExists(m, input.DB, input.Collection, input.IndexInfo.Name)
		if err != nil {
			return
		}
		if exists {
			err = ErrIndexAlreadyExists
			return
		}

		start, _, err := m.GenGlobalIDs(2)
		if err != nil {
			return
		}

		iif := input.IndexInfo.ToModel()
		iif.ID = start + 1
		iif.JobRedundant = &model.IndexInfoRedundant{
			DB:         input.DB,
			Collection: input.Collection,
		}
		job = &model.Job{
			ID:   start + 2,
			Type: model.ActionAddIndex,
			Arg:  iif,
		}

		err = m.EnQueueDDLJob(job)

		return
	})

	if err != nil {
		return
	}

	d.notifyWorker(job.Type)

	err = d.checkJob(ctx, job)
	return
}

// DropSchema for drop db
func (d *DDL) DropSchema(ctx context.Context, input DropSchemaInput) (err error) {
	return
}

// GetHistoryJob get a history job info by id
func (d *DDL) GetHistoryJob(jobID int64) (job *model.Job, err error) {

	err = util.RunInNewUpdateTxn(d.kvdb, func(txn mondis.ProviderTxn) (err error) {
		m := meta.NewMeta(txn)
		job, err = m.GetHistoryDDLJob(jobID)
		return
	})

	return
}
