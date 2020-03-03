package mondis

type (

	// Client is the universal interface implemneted by mondis client
	Client interface {
		KVOP
		Update(func(t Txn) error) error
		View(func(t Txn) error) error
	}

	// Txn is for transaction
	Txn interface {
		KVOP
		Commit() error
		Discard()
	}

	// Entry for all data of key
	Entry struct {
		Key   []byte
		Value []byte
		Meta  VMetaResp
	}
	// KVOP is common kv operation of Client and Txn
	KVOP interface {
		CommonKVOP
		Scan(option ScanOption) ([]Entry, error)
	}

	// ScanOption for scan
	// will return no more than limited entries
	ScanOption struct {
		ProviderScanOption
		Limit int
	}
)

const (
	// MaxEntry for a single Scan operation
	MaxEntry = 1000
)
