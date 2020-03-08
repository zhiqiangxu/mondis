package model

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
)

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
