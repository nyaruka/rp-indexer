package main

import (
	"database/sql"
	"os"
	"time"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	indexer "github.com/nyaruka/rp-indexer"
	"github.com/nyaruka/rp-indexer/contacts"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type config struct {
	ElasticURL string `help:"the url for our elastic search instance"`
	DB         string `help:"the connection string for our database"`
	Index      string `help:"the alias for our contact index"`
	Poll       int    `help:"the number of seconds to wait between checking for updated contacts"`
	Rebuild    bool   `help:"whether to rebuild the index, swapping it when complete, then exiting (default false)"`
	Cleanup    bool   `help:"whether to remove old indexes after a rebuild"`
	LogLevel   string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN  string `help:"the sentry configuration to log errors to, if any"`
}

func main() {
	config := config{
		ElasticURL: "http://localhost:9200",
		DB:         "postgres://localhost/temba?sslmode=disable",
		Index:      "contacts",
		Poll:       5,
		Rebuild:    false,
		Cleanup:    false,
		LogLevel:   "info",
	}
	loader := ezconf.NewLoader(&config, "indexer", "Indexes RapidPro contacts to ElasticSearch", []string{"indexer.toml"})
	loader.MustLoad()

	// configure our logger
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{})

	level, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		log.Fatalf("Invalid log level '%s'", level)
	}
	log.SetLevel(level)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			log.Fatalf("invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		log.StandardLogger().Hooks.Add(hook)
	}

	db, err := sql.Open("postgres", config.DB)
	if err != nil {
		log.Fatal(err)
	}

	ci := NewContactIndexer(config.ElasticURL, config.Index, config.Rebuild, config.Cleanup)

	for {
		err := ci.Index(db)

		if err != nil {
			if config.Rebuild {
				log.WithField("index", config.Index).WithError(err).Fatal("error during rebuilding")
			} else {
				log.WithField("index", config.Index).WithError(err).Error("error during indexing")
			}
		}

		// if we were rebuilding then we're done
		if config.Rebuild {
			os.Exit(0)
		}

		// sleep a bit before starting again
		time.Sleep(time.Second * 5)
	}
}

type ContactIndexer struct {
	indexer.BaseIndexer
}

func NewContactIndexer(elasticURL, indexName string, rebuild, cleanup bool) indexer.Indexer {
	return &ContactIndexer{
		BaseIndexer: indexer.BaseIndexer{ElasticURL: elasticURL, IndexName: indexName, Rebuild: rebuild, Cleanup: cleanup},
	}
}

func (i *ContactIndexer) Index(db *sql.DB) error {
	var err error

	// find our physical index
	physicalIndexes := indexer.FindPhysicalIndexes(i.ElasticURL, i.IndexName)
	log.WithField("physicalIndexes", physicalIndexes).WithField("index", i.IndexName).Debug("found physical indexes")

	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}

	// whether we need to remap our alias after building
	remapAlias := false

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || i.Rebuild {
		physicalIndex, err = indexer.CreateNewIndex(i.ElasticURL, i.IndexName, contacts.IndexSettings)
		if err != nil {
			return errors.Wrap(err, "error creating new index")
		}
		log.WithField("index", i.IndexName).WithField("physicalIndex", physicalIndex).Info("created new physical index")
		remapAlias = true
	}

	lastModified, err := indexer.GetLastModified(i.ElasticURL, physicalIndex)
	if err != nil {
		return errors.Wrap(err, "error finding last modified")
	}

	log.WithField("last_modified", lastModified).WithField("index", physicalIndex).Info("indexing newer than last modified")

	// now index our docs
	start := time.Now()
	indexed, deleted, err := indexer.IndexContacts(db, i.ElasticURL, physicalIndex, lastModified.Add(-5*time.Second))
	if err != nil {
		return errors.Wrap(err, "error indexing documents")
	}

	i.UpdateStats(indexed, deleted, time.Since(start))

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := indexer.MapIndexAlias(i.ElasticURL, i.IndexName, physicalIndex)
		if err != nil {
			return errors.Wrap(err, "error remapping alias")
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if i.Cleanup {
		err := indexer.CleanupIndexes(i.ElasticURL, i.IndexName)
		if err != nil {
			return errors.Wrap(err, "error cleaning up old indexes")
		}
	}

	return nil
}
