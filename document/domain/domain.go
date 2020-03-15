package domain

import (
	"errors"
	"sync"
	"time"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/config"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/mondis/util"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// Domain represents a storage space
type Domain struct {
	handle *schema.Handle
	kvdb   mondis.KVDB
	mu     struct {
		sync.RWMutex
		dbs map[string]*DB
	}
	ddl      *ddl.DDL
	reloadMu sync.Mutex
}

// NewDomain is ctor for Domain
func NewDomain(kvdb mondis.KVDB) *Domain {
	do := &Domain{
		handle: schema.NewHandle(),
		kvdb:   kvdb,
	}
	do.init()
	return do
}

func (do *Domain) init() {

	err := do.reload()
	if err != nil {
		logger.Instance().Fatal("reload", zap.Error(err))
	}

	callback := ddl.Callback{OnChanged: do.onChange}
	do.ddl = ddl.New(do.kvdb, ddl.Options{Callback: callback})
	go do.reloadInLoop()
}

func (do *Domain) onChange(err error) {
	if err != nil {
		return
	}

	do.mustReload()
}

func (do *Domain) mustReload() {
	for {
		err := do.reload()
		if err != nil {
			logger.Instance().Error("mustReload reload", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

func (do *Domain) reloadInLoop() {
	conf := config.Load()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(util.ChooseTime(conf.Lease/2, conf.ReloadMaxTickInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := do.reload()
			if err != nil {
				logger.Instance().Error("reloadInLoop reload", zap.Error(err))
			}
		}
	}
}

func (do *Domain) reload() (err error) {

	do.reloadMu.Lock()
	defer do.reloadMu.Unlock()

	return
}

func (do *Domain) loadSchema() (err error) {
	var schemaVersionCache int64
	if schemaCache := do.handle.Get(); schemaCache != nil {
		schemaVersionCache = schemaCache.Version()
	}

	txn := do.kvdb.NewTransaction(false)
	defer txn.Discard()
	m := meta.NewMeta(txn)

	schemaVersionInKV, err := m.GetSchemaVersion()
	if err != nil {
		return
	}

	if schemaVersionCache == schemaVersionInKV {
		return
	}

	return
}

var (
	// ErrDBNotExists used by Domain
	ErrDBNotExists = errors.New("db not exists")
	// ErrCollectionNotExists used by Domain
	ErrCollectionNotExists = errors.New("collection not exists")
	// ErrIndexNotExists used by Domain
	ErrIndexNotExists = errors.New("index not exists")
)

// DB for find a db by name
func (do *Domain) DB(name string) (db *DB, err error) {
	do.mu.RLock()
	db = do.mu.dbs[name]
	do.mu.RUnlock()
	if db == nil {
		err = ErrDBNotExists
	}
	return
}

// DDL getter
func (do *Domain) DDL() *ddl.DDL {
	return do.ddl
}

func (do *Domain) newCollection(dbID int64, info model.CollectionInfo) (collection *Collection, err error) {
	didSequence, err := meta.NewDocIDSequence(do.kvdb, dbID, info.ID, 0)
	if err != nil {
		return
	}
	collection = &Collection{didSequence: didSequence}
	return
}
