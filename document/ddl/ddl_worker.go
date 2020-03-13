package ddl

import (
	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/mondis/util"
	"github.com/zhiqiangxu/util/osc"
)

func (d *DDL) onCreateSchema(input CreateSchemaInput) (err error) {
	return d.lockAndUpdateMetaCache(func() (metacache *schema.MetaCache, err error) {
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

			err = m.CreateDatabase(dbInfo)
			if err != nil {
				return
			}
			for _, collection := range dbInfo.Collections {
				err = m.CreateCollection(dbInfo.ID, collection)
				if err != nil {
					return
				}
			}
			job := &model.Job{
				ID:   nextID + 1,
				Type: model.ActionCreateSchema,
				Arg:  dbInfo,
			}

			schemaVersion, err := d.updateSchemaVersion(m, job)
			if err != nil {
				return
			}
			job.FinishDBJob(model.JobStateDone, osc.StatePublic, schemaVersion, dbInfo)

			err = m.AddHistoryDDLJob(job)

			return
		})
		return
	})

}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func (d *DDL) updateSchemaVersion(m *meta.Meta, job *model.Job) (schemaVersion int64, err error) {
	schemaVersion, err = m.GenSchemaVersion()
	if err != nil {
		return
	}

	diff := &model.SchemaDiff{
		Version:       schemaVersion,
		Type:          job.Type,
		CollectionIDs: job2CollectionIDs(job),
	}

	err = m.SetSchemaDiff(diff)
	return
}

func (d *DDL) lockAndUpdateMetaCache(f func() (*schema.MetaCache, error)) error {
	return d.options.MetaCacheHandle.Update(f)
}

func job2CollectionIDs(job *model.Job) (collectionIDs []int64) {
	switch job.Type {
	case model.ActionCreateSchema:
		dbInfo := job.Arg.(*model.DBInfo)
		for _, c := range dbInfo.Collections {
			collectionIDs = append(collectionIDs, c.ID)
		}
	default:
	}
	return
}
