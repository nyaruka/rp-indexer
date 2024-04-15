package main

import (
	"database/sql"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	indexer "github.com/nyaruka/rp-indexer/v8"
	"github.com/nyaruka/rp-indexer/v8/indexers"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry"
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

	var level slog.Level
	err := level.UnmarshalText([]byte(cfg.LogLevel))
	if err != nil {
		log.Fatalf("invalid log level %s", level)
		os.Exit(1)
	}

	// configure our logger
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(logHandler))

	logger := slog.With("comp", "main")
	logger.Info("starting indexer", "version", version, "released", date)

	// if we have a DSN entry, try to initialize it
	if cfg.SentryDSN != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:           cfg.SentryDSN,
			EnableTracing: false,
		})
		if err != nil {
			log.Fatalf("error initiating sentry client, error %s, dsn %s", err, cfg.SentryDSN)
			os.Exit(1)
		}

		defer sentry.Flush(2 * time.Second)

		logger = slog.New(
			slogmulti.Fanout(
				logHandler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			),
		)
		logger = logger.With("release", version)
		slog.SetDefault(logger)
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
