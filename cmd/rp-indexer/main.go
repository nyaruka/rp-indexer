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
	"github.com/nyaruka/gocommon/aws/cwatch"
	indexer "github.com/nyaruka/rp-indexer/v9"
	"github.com/nyaruka/rp-indexer/v9/indexers"
	"github.com/nyaruka/rp-indexer/v9/runtime"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry"
)

var (
	// https://goreleaser.com/cookbooks/using-main.version
	version = "dev"
	date    = "unknown"
)

func main() {
	cfg := runtime.NewDefaultConfig()
	loader := ezconf.NewLoader(cfg, "indexer", "Indexes RapidPro contacts to ElasticSearch", []string{"indexer.toml"})
	loader.MustLoad()

	var level slog.Level
	err := level.UnmarshalText([]byte(cfg.LogLevel))
	if err != nil {
		log.Fatalf("invalid log level %s", level)
		os.Exit(1)
	}

	rt := &runtime.Runtime{Config: cfg}

	// configure our logger
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(logHandler))

	// if we have a DSN entry, try to initialize it
	if rt.Config.SentryDSN != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:           cfg.SentryDSN,
			EnableTracing: false,
		})
		if err != nil {
			log.Fatalf("error initiating sentry client, error %s, dsn %s", err, cfg.SentryDSN)
			os.Exit(1)
		}

		defer sentry.Flush(2 * time.Second)

		logger := slog.New(
			slogmulti.Fanout(
				logHandler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			),
		)
		logger = logger.With("release", version)
		slog.SetDefault(logger)
	}

	log := slog.With("comp", "main")
	log.Info("starting indexer", "version", version, "released", date)

	rt.DB, err = sql.Open("postgres", cfg.DB)
	if err != nil {
		log.Error("unable to connect to database", "error", err)
	}

	rt.CW, err = cwatch.NewService(rt.Config.AWSAccessKeyID, rt.Config.AWSSecretAccessKey, rt.Config.AWSRegion, rt.Config.CloudwatchNamespace, rt.Config.DeploymentID)
	if err != nil {
		log.Error("unable to create cloudwatch service", "error", err)
	}

	idxrs := []indexers.Indexer{
		indexers.NewContactIndexer(rt.Config.ElasticURL, rt.Config.ContactsIndex, rt.Config.ContactsShards, rt.Config.ContactsReplicas, 500),
	}

	if rt.Config.Rebuild {
		// if rebuilding, just do a complete index and quit. In future when we support multiple indexers,
		// the rebuild argument can be become the name of the index to rebuild, e.g. --rebuild=contacts
		idxr := idxrs[0]
		if _, err := idxr.Index(rt, true, rt.Config.Cleanup); err != nil {
			log.Error("error during rebuilding", "error", err, "indexer", idxr.Name())
		}
	} else {
		d := indexer.NewDaemon(rt, idxrs)
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
