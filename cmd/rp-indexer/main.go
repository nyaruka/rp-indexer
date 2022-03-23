package main

import (
	"database/sql"
	"os"
	"time"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/rp-indexer/contacts"
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

	ci := contacts.NewIndexer(config.ElasticURL, config.Index, config.Rebuild, config.Cleanup)

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
