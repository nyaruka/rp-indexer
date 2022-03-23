package indexer

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

// Indexer is base interface for indexers
type Indexer interface {
	Index(db *sql.DB) error
}

type BaseIndexer struct {
	ElasticURL string
	IndexName  string // e.g. contacts
	Rebuild    bool   // whether indexer should rebuild entire index in one pass
	Cleanup    bool   // whether after rebuilding, indexer should cleanup old indexes

	// statistics
	indexedTotal int64
	deletedTotal int64
	elapsedTotal time.Duration
}

// UpdateStats updates statistics for this indexer
func (i *BaseIndexer) UpdateStats(indexed, deleted int, elapsed time.Duration) {
	i.indexedTotal += int64(indexed)
	i.deletedTotal += int64(deleted)
	i.elapsedTotal += elapsed

	logrus.WithField("index", i.IndexName).WithField("indexed", indexed).WithField("deleted", deleted).WithField("elapsed", elapsed).Info("completed indexing")
}
