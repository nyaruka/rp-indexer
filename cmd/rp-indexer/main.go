package main

import (
	"database/sql"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	indexer "github.com/nyaruka/rp-indexer"
	"github.com/nyaruka/rp-indexer/indexers"
	log "github.com/sirupsen/logrus"
)

var (
	// https://goreleaser.com/cookbooks/using-main.version
	version = "dev"
	date    = "unknown"
)

func main() {
	cfg := indexer.NewDefaultConfig()
	loader := ezconf.NewLoader(cfg, "indexer", "Indexes RapidPro contacts to ElasticSearch", []string{"indexer.toml"})
	loader.MustLoad()

	level, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Fatalf("Invalid log level '%s'", level)
	}

	log.SetLevel(level)
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{})
	log.WithField("version", version).WithField("released", date).Info("starting indexer")

	// if we have a DSN entry, try to initialize it
	if cfg.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(cfg.SentryDSN, []log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			log.Fatalf("invalid sentry DSN: '%s': %s", cfg.SentryDSN, err)
		}
		log.StandardLogger().Hooks.Add(hook)
	}

	db, err := sql.Open("postgres", cfg.DB)
	if err != nil {
		log.Fatalf("unable to connect to database")
	}

	idxrs := []indexers.Indexer{
		indexers.NewContactIndexer(cfg.ElasticURL, cfg.ContactsIndex, cfg.ContactsShards, cfg.ContactsReplicas, 500),
	}

	if cfg.Rebuild {
		// if rebuilding, just do a complete index and quit. In future when we support multiple indexers,
		// the rebuild argument can be become the name of the index to rebuild, e.g. --rebuild=contacts
		idxr := idxrs[0]
		if _, err := idxr.Index(db, true, cfg.Cleanup); err != nil {
			log.WithField("indexer", idxr.Name()).WithError(err).Fatal("error during rebuilding")
		}
	} else {
		d := indexer.NewDaemon(cfg, db, idxrs, time.Duration(cfg.Poll)*time.Second)
		d.Start()

		handleSignals(d)
	}
}

// handleSignals takes care of trapping quit, interrupt or terminate signals and doing the right thing
func handleSignals(d *indexer.Daemon) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		sig := <-sigs
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			log.WithField("signal", sig).Info("received exit signal, exiting")
			d.Stop()
			return
		}
	}
}
