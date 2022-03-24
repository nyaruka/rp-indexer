package indexer

import (
	"database/sql"
	"sync"
	"time"

	"github.com/nyaruka/librato"
	"github.com/nyaruka/rp-indexer/indexers"
	"github.com/sirupsen/logrus"
)

type Daemon struct {
	cfg      *Config
	db       *sql.DB
	wg       *sync.WaitGroup
	quit     chan bool
	indexers []indexers.Indexer
}

// NewDaemon creates a new daemon to run the given indexers
func NewDaemon(cfg *Config, db *sql.DB, ixs []indexers.Indexer) *Daemon {
	return &Daemon{
		cfg:      cfg,
		db:       db,
		wg:       &sync.WaitGroup{},
		quit:     make(chan bool),
		indexers: ixs,
	}
}

// Start starts this daemon
func (d *Daemon) Start() {
	// if we have a librato token, configure it
	if d.cfg.LibratoToken != "" {
		librato.Configure(d.cfg.LibratoUsername, d.cfg.LibratoToken, d.cfg.InstanceName, time.Second, d.wg)
		librato.Start()
	}

	for _, i := range d.indexers {
		d.startIndexer(i, time.Second*5)
	}
}

func (d *Daemon) startIndexer(indexer indexers.Indexer, interval time.Duration) {
	d.wg.Add(1) // add ourselves to the wait group

	go func() {
		defer func() {
			logrus.WithField("indexer", indexer.Name()).Info("indexer exiting")
			d.wg.Done()
		}()

		for {
			select {
			case <-d.quit:
				return
			case <-time.After(interval):
				_, err := indexer.Index(d.db, d.cfg.Rebuild, d.cfg.Cleanup)
				if err != nil {
					logrus.WithField("index", d.cfg.Index).WithError(err).Error("error during indexing")
				}
			}
		}
	}()
}

// Stop stops this daemon
func (d *Daemon) Stop() {
	logrus.Info("daemon stopping")
	librato.Stop()

	close(d.quit)
	d.wg.Wait()
}
