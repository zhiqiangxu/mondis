package model

import (
	"encoding/json"

	"github.com/zhiqiangxu/util/osc"
)

type (
	// DBInfo for db
	DBInfo struct {
		ID              int64
		Name            string
		Collections     map[string]*CollectionInfo
		CollectionOrder []string
		State           osc.SchemaState
	}
	// CollectionInfo for collection
	CollectionInfo struct {
		ID         int64
		Name       string
		Indices    map[string]*IndexInfo
		IndexOrder []string
		State      osc.SchemaState
	}
	// IndexInfo for index
	IndexInfo struct {
		Name    string
		Columns []string
		Unique  bool
		State   osc.SchemaState
	}
	// Job for a DDL operation
	Job struct {
		ID          int64
		Type        ActionType
		State       JobState
		Error       error
		ErrorCount  int64
		Arg         interface{} `json:"-"`
		RawArg      json.RawMessage
		SchemaState osc.SchemaState
		StartTS     uint64 `json:"start_ts"`
		// DependencyID is the job's ID that the current job depends on.
		DependencyID int64
	}
	// SchemaDiff contains the schema modification at a particular schema version.
	SchemaDiff struct {
		Version       int64      `json:"version"`
		Type          ActionType `json:"type"`
		CollectionIDs []int64
		Arg           interface{} `json:"-"`
		RawArg        json.RawMessage
	}
)

// ActionType is the type for DDL action.
type ActionType byte

// List DDL actions.
const (
	ActionNone ActionType = iota
	ActionCreateSchema
	ActionDropSchema
	ActionCreateCollection
	ActionDropCollection
	ActionAddIndex
	ActionDropIndex
	ActionTruncateCollection
	ActionRenameCollection
)

var actionMap = map[ActionType]string{
	ActionCreateSchema:       "create schema",
	ActionDropSchema:         "drop schema",
	ActionCreateCollection:   "create collection",
	ActionDropCollection:     "drop collection",
	ActionAddIndex:           "add index",
	ActionDropIndex:          "drop index",
	ActionTruncateCollection: "truncate collection",
	ActionRenameCollection:   "rename collection",
}

// String return current ddl action in string
func (action ActionType) String() string {
	if v, ok := actionMap[action]; ok {
		return v
	}
	return "none"
}

// JobState is for job state.
type JobState byte

// List job states.
const (
	JobStateNone    JobState = 0
	JobStateRunning JobState = 1
	// When DDL encountered an unrecoverable error at reorganization state,
	// some keys has been added already, we need to remove them.
	// JobStateRollingback is the state to do the rolling back job.
	JobStateRollingback  JobState = 2
	JobStateRollbackDone JobState = 3
	JobStateDone         JobState = 4
	JobStateCancelled    JobState = 5
	// JobStateSynced is used to mark the information about the completion of this job
	// has been synchronized to all servers.
	JobStateSynced JobState = 6
	// JobStateCancelling is used to mark the DDL job is cancelled by the client, but the DDL work hasn't handle it.
	JobStateCancelling JobState = 7
)

// String implements fmt.Stringer interface.
func (s JobState) String() string {
	switch s {
	case JobStateRunning:
		return "running"
	case JobStateRollingback:
		return "rollingback"
	case JobStateRollbackDone:
		return "rollback done"
	case JobStateDone:
		return "done"
	case JobStateCancelled:
		return "cancelled"
	case JobStateCancelling:
		return "cancelling"
	case JobStateSynced:
		return "synced"
	default:
		return "none"
	}
}

// CollectionExists check whether collection exists
func (db *DBInfo) CollectionExists(collectionName string) bool {
	return db.Collections[collectionName] != nil
}

// CollectionInfo finds CollectionInfo by name
func (db *DBInfo) CollectionInfo(collectionName string) *CollectionInfo {
	return db.Collections[collectionName]
}

// Clone DBInfo
func (db *DBInfo) Clone() *DBInfo {
	clone := *db
	clone.Collections = make(map[string]*CollectionInfo)
	clone.CollectionOrder = make([]string, len(db.CollectionOrder))
	for cn, ci := range db.Collections {
		clone.Collections[cn] = ci.Clone()
	}
	for i, cn := range db.CollectionOrder {
		clone.CollectionOrder[i] = cn
	}
	return &clone
}

// Clone CollectionInfo
func (c *CollectionInfo) Clone() *CollectionInfo {
	clone := *c
	clone.Indices = make(map[string]*IndexInfo)
	clone.IndexOrder = make([]string, len(c.IndexOrder))
	for in, ii := range c.Indices {
		clone.Indices[in] = ii.Clone()
	}
	for i, in := range c.IndexOrder {
		clone.IndexOrder[i] = in
	}
	return &clone
}

// IndexExists checks whether index exists
func (c *CollectionInfo) IndexExists(indexName string) bool {
	return c.Indices[indexName] != nil
}

// Clone IndexInfo
func (ii *IndexInfo) Clone() *IndexInfo {
	clone := *ii
	clone.Columns = make([]string, len(ii.Columns))
	for i, name := range ii.Columns {
		clone.Columns[i] = name
	}
	return &clone
}

// Encode encodes job with json format.
func (job *Job) Encode() (b []byte, err error) {
	if len(job.RawArg) == 0 {
		job.RawArg, err = json.Marshal(job.Arg)
		if err != nil {
			return
		}
	}

	b, err = json.Marshal(job)

	return
}

// Decode decodes job from the json buffer, we must use DecodeArg later to
// decode special arg for this job.
func (job *Job) Decode(b []byte) (err error) {
	err = json.Unmarshal(b, job)
	return
}

// DecodeArg decodes job arg.
func (job *Job) DecodeArg(arg interface{}) (err error) {
	err = json.Unmarshal(job.RawArg, arg)
	job.Arg = arg
	return
}

// IsSynced returns whether the DDL modification is synced among all servers.
func (job *Job) IsSynced() bool {
	return job.State == JobStateSynced
}

// IsDone returns whether job is done.
func (job *Job) IsDone() bool {
	return job.State == JobStateDone
}

// IsCancelled returns whether job is canceled.
func (job *Job) IsCancelled() bool {
	return job.State == JobStateCancelled
}

// IsFinished returns whether job is finished or not.
// If the job state is Done or Cancelled, it is finished.
func (job *Job) IsFinished() bool {
	return job.State == JobStateDone || job.State == JobStateRollbackDone || job.State == JobStateCancelled
}

// IsRollbackDone returns whether the job is rolled back or not.
func (job *Job) IsRollbackDone() bool {
	return job.State == JobStateRollbackDone
}

// FinishCollectionJob is called when a collection job is finished.
func (job *Job) FinishCollectionJob(jobState JobState, schemaState osc.SchemaState, schemaVersion int64, collectionInfo *CollectionInfo) {
	job.State = jobState
	job.SchemaState = schemaState
}

// FinishDBJob is called when a db job is finished.
func (job *Job) FinishDBJob(jobState JobState, schemaState osc.SchemaState, schemaVersion int64, dbInfo *DBInfo) {
	job.State = jobState
	job.SchemaState = schemaState
}

// Encode SchemaDiff
func (sd *SchemaDiff) Encode() (b []byte, err error) {
	if len(sd.RawArg) == 0 {
		sd.RawArg, err = json.Marshal(sd.Arg)
		if err != nil {
			return
		}
	}

	b, err = json.Marshal(sd)
	return
}

// Decode decodes schema diff from the json buffer, we must use DecodeArg later to
// decode special arg for this schema diff.
func (sd *SchemaDiff) Decode(b []byte) (err error) {
	err = json.Unmarshal(b, sd)
	return
}

// DecodeArg decodes schema diff arg.
func (sd *SchemaDiff) DecodeArg(arg interface{}) (err error) {
	err = json.Unmarshal(sd.RawArg, arg)
	sd.Arg = arg
	return
}
