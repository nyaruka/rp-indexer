package indexer

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

// Indexer is base interface for indexers
type Indexer interface {
	Name() string
	Index(db *sql.DB) error
}

type BaseIndexer struct {
	name       string // e.g. contacts, used as based index name
	ElasticURL string
	Rebuild    bool // whether indexer should rebuild entire index in one pass
	Cleanup    bool // whether after rebuilding, indexer should cleanup old indexes

	// statistics
	indexedTotal int64
	deletedTotal int64
	elapsedTotal time.Duration
}

func NewBaseIndexer(name, elasticURL string, rebuild, cleanup bool) BaseIndexer {
	return BaseIndexer{name: name, ElasticURL: elasticURL, Rebuild: rebuild, Cleanup: cleanup}
}

func (i *BaseIndexer) Name() string {
	return i.name
}

// UpdateStats updates statistics for this indexer
func (i *BaseIndexer) UpdateStats(indexed, deleted int, elapsed time.Duration) {
	i.indexedTotal += int64(indexed)
	i.deletedTotal += int64(deleted)
	i.elapsedTotal += elapsed

	logrus.WithField("indexer", i.name).WithField("indexed", indexed).WithField("deleted", deleted).WithField("elapsed", elapsed).Info("completed indexing")
}
