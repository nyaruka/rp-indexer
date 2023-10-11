package main

import (
	"database/sql"

	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evalphobia/logrus_sentry"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	indexer "github.com/nyaruka/rp-indexer/v8"
	"github.com/nyaruka/rp-indexer/v8/indexers"
	"github.com/nyaruka/rp-indexer/v8/utils"
	"github.com/sirupsen/logrus"
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

	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logrus.Fatalf("Invalid log level '%s'", level)
	}

	logrus.SetLevel(level)
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.WithField("version", version).WithField("released", date).Info("starting indexer")

	// configure golang std structured logging to route to logrus
	slog.SetDefault(slog.New(utils.NewLogrusHandler(logrus.StandardLogger())))

	logger := slog.With("comp", "main")
	logger.Info("starting indexer", "version", version, "released", date)

	// if we have a DSN entry, try to initialize it
	if cfg.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(cfg.SentryDSN, []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			logger.Error("invalid sentry DSN: '%s': %s", cfg.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	db, err := sql.Open("postgres", cfg.DB)
	if err != nil {
		logger.Error("unable to connect to database")
	}

	idxrs := []indexers.Indexer{
		indexers.NewContactIndexer(cfg.ElasticURL, cfg.ContactsIndex, cfg.ContactsShards, cfg.ContactsReplicas, 500),
	}

	if cfg.Rebuild {
		// if rebuilding, just do a complete index and quit. In future when we support multiple indexers,
		// the rebuild argument can be become the name of the index to rebuild, e.g. --rebuild=contacts
		idxr := idxrs[0]
		if _, err := idxr.Index(db, true, cfg.Cleanup); err != nil {
			logger.Error("error during rebuilding", "error", err, "indexer", idxr.Name())
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
			slog.Info("received exit signal, exiting", "signal", sig)
			d.Stop()
			return
		}
	}
}
