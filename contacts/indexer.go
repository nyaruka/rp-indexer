package contacts

import (
	"bytes"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	indexer "github.com/nyaruka/rp-indexer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

//go:embed index_settings.json
var indexSettings json.RawMessage

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// ContactIndexer is an indexer for contacts
type Indexer struct {
	indexer.BaseIndexer

	batchSize int
}

// NewIndexer creates a new contact indexer
func NewIndexer(elasticURL, name string, batchSize int) *Indexer {
	return &Indexer{
		BaseIndexer: indexer.NewBaseIndexer(elasticURL, name),
		batchSize:   batchSize,
	}
}

// Index indexes modified contacts and returns the name of the concrete index
func (i *Indexer) Index(db *sql.DB, rebuild, cleanup bool) (string, error) {
	var err error

	// find our physical index
	physicalIndexes := i.FindIndexes()

	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}

	// whether we need to remap our alias after building
	remapAlias := false

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || rebuild {
		physicalIndex, err = i.CreateNewIndex(indexSettings)
		if err != nil {
			return "", errors.Wrap(err, "error creating new index")
		}
		i.Log().WithField("index", physicalIndex).Info("created new physical index")
		remapAlias = true
	}

	lastModified, err := i.GetLastModified(physicalIndex)
	if err != nil {
		return "", errors.Wrap(err, "error finding last modified")
	}

	i.Log().WithField("index", physicalIndex).WithField("last_modified", lastModified).Info("indexing newer than last modified")

	// now index our docs
	start := time.Now()
	indexed, deleted, err := i.indexModified(db, physicalIndex, lastModified.Add(-5*time.Second))
	if err != nil {
		return "", errors.Wrap(err, "error indexing documents")
	}

	i.RecordComplete(indexed, deleted, time.Since(start))

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := i.UpdateAlias(physicalIndex)
		if err != nil {
			return "", errors.Wrap(err, "error updating alias")
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if cleanup {
		err := i.CleanupIndexes()
		if err != nil {
			return "", errors.Wrap(err, "error cleaning up old indexes")
		}
	}

	return physicalIndex, nil
}

// IndexModified queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func (i *Indexer) indexModified(db *sql.DB, index string, lastModified time.Time) (int, int, error) {
	batch := &bytes.Buffer{}
	createdCount, deletedCount, processedCount := 0, 0, 0

	var modifiedOn time.Time
	var contactJSON string
	var id, orgID int64
	var isActive bool

	start := time.Now()

	for {
		rows, err := FetchModified(db, lastModified)

		queryCreated := 0
		queryCount := 0
		queryModified := lastModified

		// no more rows? return
		if err == sql.ErrNoRows {
			return 0, 0, nil
		}
		if err != nil {
			return 0, 0, err
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&orgID, &id, &modifiedOn, &isActive, &contactJSON)
			if err != nil {
				return 0, 0, err
			}

			queryCount++
			processedCount++
			lastModified = modifiedOn

			if isActive {
				logrus.WithField("id", id).WithField("modifiedOn", modifiedOn).WithField("contact", contactJSON).Debug("modified contact")

				batch.WriteString(fmt.Sprintf(indexCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
				batch.WriteString(contactJSON)
				batch.WriteString("\n")
			} else {
				logrus.WithField("id", id).WithField("modifiedOn", modifiedOn).Debug("deleted contact")

				batch.WriteString(fmt.Sprintf(deleteCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
			}

			// write to elastic search in batches
			if queryCount%i.batchSize == 0 {
				created, deleted, err := i.IndexBatch(index, batch.Bytes())
				if err != nil {
					return 0, 0, err
				}
				batch.Reset()

				queryCreated += created
				createdCount += created
				deletedCount += deleted
			}
		}

		if batch.Len() > 0 {
			created, deleted, err := i.IndexBatch(index, batch.Bytes())
			if err != nil {
				return 0, 0, err
			}

			queryCreated += created
			createdCount += created
			deletedCount += deleted
			batch.Reset()
		}

		// last modified stayed the same and we didn't add anything, seen it all, break out
		if lastModified.Equal(queryModified) && queryCreated == 0 {
			break
		}

		rows.Close()

		elapsed := time.Since(start)
		rate := float32(processedCount) / (float32(elapsed) / float32(time.Second))

		i.Log().WithField("index", index).WithFields(logrus.Fields{"rate": int(rate), "added": createdCount, "deleted": deletedCount, "elapsed": elapsed}).Info("indexed contact batch")
	}

	return createdCount, deletedCount, nil
}
