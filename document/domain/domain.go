package domain

import (
	"context"
	"sync"
	"time"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/config"
	"github.com/zhiqiangxu/mondis/document/db"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/meta"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/document/schema"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// Domain represents a storage space
type Domain struct {
	handle   *schema.Handle
	kvdb     mondis.KVDB
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
	if conf.Lease == 0 {
		return
	}
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(conf.Lease / 2)
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

	err = do.reloadSchema()
	return
}

func (do *Domain) reloadSchema() (err error) {
	var schemaVersionCache int64
	metaCache := do.handle.Get()
	if metaCache != nil {
		schemaVersionCache = metaCache.Version()
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

	ok, diffs, err := do.tryLoadSchemaDiff(m, schemaVersionCache, schemaVersionInKV)
	if err != nil {
		return
	}

	if ok {
		newMetaCache := metaCache.Clone()
		err = newMetaCache.ApplyDiffs(diffs)
		if err != nil {
			return
		}

		err = do.handle.Update(context.Background(), newMetaCache)
		return
	}

	dbInfos, err := do.fetchAllDBs(m)
	if err != nil {
		return
	}

	newMetaCache := schema.NewMetaCache(schemaVersionInKV, dbInfos)
	err = do.handle.Update(context.Background(), newMetaCache)

	return
}

const (
	maxNumberOfDiffsToLoad = 100
)

func (do *Domain) tryLoadSchemaDiff(m *meta.Meta, schemaVersionCache, schemaVersionInKV int64) (ok bool, diffs []*model.SchemaDiff, err error) {

	if schemaVersionCache == 0 || schemaVersionInKV-schemaVersionCache > maxNumberOfDiffsToLoad {
		return
	}

	diffs = make([]*model.SchemaDiff, 0, schemaVersionInKV-schemaVersionCache)
	var diff *model.SchemaDiff
	for schemaVersionCache < schemaVersionInKV {
		schemaVersionCache++
		diff, err = m.GetSchemaDiff(schemaVersionCache)
		if err != nil {
			return
		}
		if diff == nil {
			return
		}
		diffs = append(diffs, diff)
	}

	ok = true
	return
}

func (do *Domain) fetchAllDBs(m *meta.Meta) (dbInfos []*model.DBInfo, err error) {

	dbInfos, err = m.ListDatabases()
	return
}

// DB for find a db by name
func (do *Domain) DB(name string) (db *db.DB, err error) {

	db = db.NewDB(name, do.kvdb, do.handle)
	return
}

// DDL getter
func (do *Domain) DDL() *ddl.DDL {
	return do.ddl
}
