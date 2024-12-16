package indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/rp-indexer/v9/indexers"
	"github.com/nyaruka/rp-indexer/v9/runtime"
)

type Daemon struct {
	rt       *runtime.Runtime
	wg       *sync.WaitGroup
	quit     chan bool
	indexers []indexers.Indexer
	poll     time.Duration

	prevStats map[indexers.Indexer]indexers.Stats
}

// NewDaemon creates a new daemon to run the given indexers
func NewDaemon(rt *runtime.Runtime, ixs []indexers.Indexer) *Daemon {
	return &Daemon{
		rt:        rt,
		wg:        &sync.WaitGroup{},
		quit:      make(chan bool),
		indexers:  ixs,
		poll:      time.Duration(rt.Config.Poll) * time.Second,
		prevStats: make(map[indexers.Indexer]indexers.Stats, len(ixs)),
	}
}

// Start starts this daemon
func (d *Daemon) Start() {
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
				_, err := indexer.Index(d.rt, d.rt.Config.Rebuild, d.rt.Config.Cleanup)
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
			slog.Info("metrics reporter exiting")
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
	metrics := make([]types.MetricDatum, 0, len(d.indexers)*3)

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

		idxDim := cwatch.Dimension("Index", ix.Name())

		metrics = append(metrics,
			cwatch.Datum("RecordsIndexed", float64(indexedInPeriod), types.StandardUnitCount, idxDim),
			cwatch.Datum("RecordsDeleted", float64(deletedInPeriod), types.StandardUnitCount, idxDim),
			cwatch.Datum("IndexingRate", rateInPeriod, types.StandardUnitCountSecond, idxDim),
		)

		d.prevStats[ix] = stats

		if includeLag {
			lag, err := d.calculateLag(ctx, ix)
			if err != nil {
				log.Error("error getting db last modified", "index", ix.Name(), "error", err)
			} else {
				metrics = append(metrics, cwatch.Datum("IndexingLag", lag.Seconds(), types.StandardUnitSeconds, idxDim))
			}
		}
	}

	if err := d.rt.CW.Send(ctx, metrics...); err != nil {
		log.Error("error putting metrics", "error", err)
	}

	log.Info("stats reported")
}

func (d *Daemon) calculateLag(ctx context.Context, ix indexers.Indexer) (time.Duration, error) {
	esLastModified, err := ix.GetESLastModified(ix.Name())
	if err != nil {
		return 0, fmt.Errorf("error getting ES last modified: %w", err)
	}

	dbLastModified, err := ix.GetDBLastModified(ctx, d.rt.DB)
	if err != nil {
		return 0, fmt.Errorf("error getting DB last modified: %w", err)
	}

	return dbLastModified.Sub(esLastModified), nil
}

// Stop stops this daemon
func (d *Daemon) Stop() {
	slog.Info("daemon stopping")

	close(d.quit)
	d.wg.Wait()
}
