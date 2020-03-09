package model

import (
	"encoding/json"
	"fmt"
)

type (
	// DBInfo for db
	DBInfo struct {
		ID          int64
		Name        string
		Collections map[int64]*CollectionInfo
		State       SchemaState
	}
	// CollectionInfo for collection
	CollectionInfo struct {
		ID      int64
		Name    string
		Indices []*IndexInfo
		State   SchemaState
	}
	// IndexInfo for index
	IndexInfo struct {
		ID           int64
		Name         string
		CollectionID int64
		Columns      []*IndexColumn
		Unique       bool
		Primary      bool
		State        SchemaState
	}
	// IndexColumn for index column
	IndexColumn struct {
		Name   string
		Offset int
	}
	// Job for a DDL operation
	Job struct {
		ID           int64
		Type         ActionType
		SchemaID     int64
		CollectionID int64
		SchemaName   string
		State        JobState
		Error        error
		ErrorCount   int64
		Args         []interface{} `json:"-"`
		// RawArgs : We must use json raw message to delay parsing special args.
		RawArgs     json.RawMessage
		SchemaState SchemaState
		StartTS     uint64 `json:"start_ts"`
		// DependencyID is the job's ID that the current job depends on.
		DependencyID int64
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

// SchemaState is the state for schema elements.
type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizing whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizing whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

// String implements fmt.Stringer interface.
func (s SchemaState) String() string {
	switch s {
	case StateDeleteOnly:
		return "delete only"
	case StateWriteOnly:
		return "write only"
	case StateWriteReorganization:
		return "write reorganization"
	case StateDeleteReorganization:
		return "delete reorganization"
	case StatePublic:
		return "public"
	default:
		return "none"
	}
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

// Encode encodes job with json format.
// updateRawArgs is used to determine whether to update the raw args.
func (job *Job) Encode(updateRawArgs bool) (b []byte, err error) {
	if updateRawArgs {
		job.RawArgs, err = json.Marshal(job.Args)
		if err != nil {
			return
		}
	}

	b, err = json.Marshal(job)

	return
}

// Decode decodes job from the json buffer, we must use DecodeArgs later to
// decode special args for this job.
func (job *Job) Decode(b []byte) (err error) {
	err = json.Unmarshal(b, job)
	return
}

// DecodeArgs decodes job args.
func (job *Job) DecodeArgs(args ...interface{}) (err error) {
	job.Args = args
	err = json.Unmarshal(job.RawArgs, &job.Args)
	return
}

// String implements fmt.Stringer interface.
func (job *Job) String() string {
	return fmt.Sprintf("ID:%d, Type:%s, State:%s, SchemaState:%s, SchemaID:%d, CollectionID:%d, ArgLen:%d, start time: %v, Err:%v, ErrCount:%d",
		job.ID, job.Type, job.State, job.SchemaState, job.SchemaID, job.CollectionID, len(job.Args), job.StartTS, job.Error, job.ErrorCount)
}
