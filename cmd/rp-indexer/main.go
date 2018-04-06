package main

import (
	"database/sql"
	"os"
	"time"

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
}

func main() {
	config := config{
		ElasticURL: "http://localhost:9200",
		DB:         "postgres://localhost/rapidpro?sslmode=disable",
		Index:      "contacts",
		Poll:       5,
		Rebuild:    false,
	}
	loader := ezconf.NewLoader(&config, "indexer", "Indexes RapidPro contacts to ElasticSearch", []string{"indexer.toml"})
	loader.MustLoad()

	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.InfoLevel)

	db, err := sql.Open("postgres", config.DB)
	if err != nil {
		log.Fatal(err)
	}

	physicalIndexes := indexer.FindPhysicalIndexes(config.ElasticURL, config.Index)
	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}
	oldIndex := physicalIndex

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || config.Rebuild {
		physicalIndex, err = indexer.CreateNewIndex(config.ElasticURL, config.Index)
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		lastModified, err := indexer.GetLastModified(config.ElasticURL, physicalIndex)
		if err != nil {
			logError(config.Rebuild, err, "error finding last modified")
			continue
		}

		start := time.Now()
		log.WithField("last_modified", lastModified).Info("indexing contacts newer than last modified")

		// now index our docs
		indexed, deleted, err := indexer.IndexContacts(db, config.ElasticURL, physicalIndex, lastModified)
		if err != nil {
			logError(config.Rebuild, err, "error indexing contacts")
			continue
		}
		log.WithField("added", indexed).WithField("deleted", deleted).WithField("elapsed", time.Now().Sub(start)).Info("completed indexing")

		// if the index didn't previously exist or we are rebuilding, remap to our alias
		if oldIndex == "" || config.Rebuild {
			err := indexer.MapIndexAlias(config.ElasticURL, config.Index, physicalIndex)
			if err != nil {
				logError(config.Rebuild, err, "error mapping alias")
				continue
			}
			oldIndex = physicalIndex
		}

		if config.Rebuild {
			os.Exit(0)
		} else {
			time.Sleep(time.Second * 5)
			physicalIndex = ""
			physicalIndexes = indexer.FindPhysicalIndexes(config.ElasticURL, config.Index)
			if len(physicalIndex) > 0 {
				physicalIndex = physicalIndexes[0]
			}
		}
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
