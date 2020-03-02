package main

import (
	"database/sql"
	"os"
	"time"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	indexer "github.com/nyaruka/rp-indexer"
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
			log.Fatalf("Invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		log.StandardLogger().Hooks.Add(hook)
	}

	db, err := sql.Open("postgres", config.DB)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// find our physical index
		physicalIndexes := indexer.FindPhysicalIndexes(config.ElasticURL, config.Index)
		log.WithField("physicalIndexes", physicalIndexes).WithField("index", config.Index).Debug("found physical indexes")

		physicalIndex := ""
		if len(physicalIndexes) > 0 {
			physicalIndex = physicalIndexes[0]
		}

		// whether we need to remap our alias after building
		remapAlias := false

		// doesn't exist or we are rebuilding, create it
		if physicalIndex == "" || config.Rebuild {
			physicalIndex, err = indexer.CreateNewIndex(config.ElasticURL, config.Index)
			if err != nil {
				logError(config.Rebuild, err, "error creating new index")
				continue
			}
			log.WithField("index", config.Index).WithField("physicalIndex", physicalIndex).Info("created new physical index")
			remapAlias = true
		}

		lastModified, err := indexer.GetLastModified(config.ElasticURL, physicalIndex)
		if err != nil {
			logError(config.Rebuild, err, "error finding last modified")
			continue
		}

		start := time.Now()
		log.WithField("last_modified", lastModified).WithField("index", physicalIndex).Info("indexing contacts newer than last modified")

		// now index our docs
		indexed, deleted, err := indexer.IndexContacts(db, config.ElasticURL, physicalIndex, lastModified.Add(-5*time.Second))
		if err != nil {
			logError(config.Rebuild, err, "error indexing contacts")
			continue
		}
		log.WithField("added", indexed).WithField("deleted", deleted).WithField("index", physicalIndex).WithField("elapsed", time.Now().Sub(start)).Info("completed indexing")

		// if the index didn't previously exist or we are rebuilding, remap to our alias
		if remapAlias {
			err := indexer.MapIndexAlias(config.ElasticURL, config.Index, physicalIndex)
			if err != nil {
				logError(config.Rebuild, err, "error remapping alias")
				continue
			}
			remapAlias = false
		}

		// cleanup our aliases if appropriate
		if config.Cleanup {
			err := indexer.CleanupIndexes(config.ElasticURL, config.Index)
			if err != nil {
				logError(config.Rebuild, err, "error cleaning up aliases")
				continue
			}
		}

		if config.Rebuild {
			os.Exit(0)
		}

		// sleep a bit before starting again
		time.Sleep(time.Second * 5)
	}
}

func logError(fatal bool, err error, msg string) {
	if fatal {
		log.WithError(err).Fatal(msg)
	} else {
		log.WithError(err).Error(msg)
		time.Sleep(time.Second * 5)
	}
}
