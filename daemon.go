package indexer

import (
	"database/sql"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/rp-indexer/indexers"
	"github.com/sirupsen/logrus"
)

type Daemon struct {
	cfg      *Config
	db       *sql.DB
	wg       *sync.WaitGroup
	quit     chan bool
	indexers []indexers.Indexer
	poll     time.Duration

	prevStats map[indexers.Indexer]indexers.Stats
}

// NewDaemon creates a new daemon to run the given indexers
func NewDaemon(cfg *Config, db *sql.DB, ixs []indexers.Indexer, poll time.Duration) *Daemon {
	return &Daemon{
		cfg:       cfg,
		db:        db,
		wg:        &sync.WaitGroup{},
		quit:      make(chan bool),
		indexers:  ixs,
		poll:      poll,
		prevStats: make(map[indexers.Indexer]indexers.Stats, len(ixs)),
	}
}

// Start starts this daemon
func (d *Daemon) Start() {
	// if we have a librato token, configure it
	if d.cfg.LibratoToken != "" {
		analytics.RegisterBackend(analytics.NewLibrato(d.cfg.LibratoUsername, d.cfg.LibratoToken, d.cfg.InstanceName, time.Second, d.wg))
	}

	analytics.Start()

	for _, i := range d.indexers {
		d.startIndexer(i)
	}

	d.startStatsReporter(time.Minute)
}

func (d *Daemon) startIndexer(indexer indexers.Indexer) {
	d.wg.Add(1) // add ourselves to the wait group

	log := logrus.WithField("indexer", indexer.Name())

	go func() {
		defer func() {
			log.Info("indexer exiting")
			d.wg.Done()
		}()

		for {
			select {
			case <-d.quit:
				return
			case <-time.After(d.poll):
				_, err := indexer.Index(d.db, d.cfg.Rebuild, d.cfg.Cleanup)
				if err != nil {
					log.WithError(err).Error("error during indexing")
				}
			}
		}
	}()
}

func (d *Daemon) startStatsReporter(interval time.Duration) {
	d.wg.Add(1) // add ourselves to the wait group

	go func() {
		defer func() {
			logrus.Info("analytics exiting")
			d.wg.Done()
		}()

		for {
			select {
			case <-d.quit:
				return
			case <-time.After(interval):
				d.reportStats()
			}
		}
	}()
}

func (d *Daemon) reportStats() {
	metrics := make(map[string]float64, len(d.indexers)*2)

	for _, ix := range d.indexers {
		stats := ix.Stats()
		prev := d.prevStats[ix]

		indexedInPeriod := stats.Indexed - prev.Indexed
		deletedInPeriod := stats.Deleted - prev.Deleted
		elapsedInPeriod := stats.Elapsed - prev.Elapsed
		rateInPeriod := float64(0)
		if indexedInPeriod > 0 && elapsedInPeriod > 0 {
			rateInPeriod = float64(indexedInPeriod) / (float64(elapsedInPeriod) / float64(time.Second))
		}

		metrics[ix.Name()+"_indexed"] = float64(indexedInPeriod)
		metrics[ix.Name()+"_deleted"] = float64(deletedInPeriod)
		metrics[ix.Name()+"_rate"] = rateInPeriod

		d.prevStats[ix] = stats
	}

	log := logrus.NewEntry(logrus.StandardLogger())

	for k, v := range metrics {
		analytics.Gauge("indexer."+k, v)
		log = log.WithField(k, v)
	}

	log.Info("stats reported")
}

// Stop stops this daemon
func (d *Daemon) Stop() {
	logrus.Info("daemon stopping")
	analytics.Stop()

	close(d.quit)
	d.wg.Wait()
}
