package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/rp-indexer/v9/indexers"
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

	log := slog.With("indexer", indexer.Name())

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
					log.Error("error during indexing", "error", err)
				}
			}
		}
	}()
}

func (d *Daemon) startStatsReporter(interval time.Duration) {
	d.wg.Add(1) // add ourselves to the wait group

	// calculating lag is more expensive than reading indexer stats so we only do it every 5th iteration
	var iterations int64

	go func() {
		defer func() {
			slog.Info("analytics exiting")
			d.wg.Done()
		}()

		for {
			select {
			case <-d.quit:
				return
			case <-time.After(interval):
				d.reportStats(iterations%5 == 0)
			}

			iterations++
		}
	}()
}

func (d *Daemon) reportStats(includeLag bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := slog.New(slog.Default().Handler())
	metrics := make(map[string]float64, len(d.indexers)*2)

	for _, ix := range d.indexers {
		stats := ix.Stats()
		prev := d.prevStats[ix]

		indexedInPeriod := stats.Indexed - prev.Indexed
		elapsedInPeriod := stats.Elapsed - prev.Elapsed
		rateInPeriod := float64(0)
		if indexedInPeriod > 0 && elapsedInPeriod > 0 {
			rateInPeriod = float64(indexedInPeriod) / (float64(elapsedInPeriod) / float64(time.Second))
		}

		metrics[ix.Name()+"_indexed"] = float64(indexedInPeriod)
		metrics[ix.Name()+"_rate"] = rateInPeriod

		d.prevStats[ix] = stats

		if includeLag {
			lag, err := d.calculateLag(ctx, ix)
			if err != nil {
				log.Error("error getting db last modified", "index", ix.Name(), "error", err)
			} else {
				metrics[ix.Name()+"_lag"] = lag.Seconds()
			}
		}
	}

	for k, v := range metrics {
		analytics.Gauge("indexer."+k, v)
		log = log.With(k, v)
	}

	log.Info("stats reported")
}

func (d *Daemon) calculateLag(ctx context.Context, ix indexers.Indexer) (time.Duration, error) {
	esLastModified, err := ix.GetESLastModified(ix.Name())
	if err != nil {
		return 0, fmt.Errorf("error getting ES last modified: %w", err)
	}

	dbLastModified, err := ix.GetDBLastModified(ctx, d.db)
	if err != nil {
		return 0, fmt.Errorf("error getting DB last modified: %w", err)
	}

	return dbLastModified.Sub(esLastModified), nil
}

// Stop stops this daemon
func (d *Daemon) Stop() {
	slog.Info("daemon stopping")
	analytics.Stop()

	close(d.quit)
	d.wg.Wait()
}
