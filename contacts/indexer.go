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

var BatchSize = 500

//go:embed index_settings.json
var IndexSettings json.RawMessage

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// ContactIndexer is an indexer for contacts
type Indexer struct {
	indexer.BaseIndexer
}

// NewIndexer creates a new contact indexer
func NewIndexer(name, elasticURL string) *Indexer {
	return &Indexer{
		BaseIndexer: indexer.NewBaseIndexer(name, elasticURL),
	}
}

func (i *Indexer) Index(db *sql.DB, rebuild, cleanup bool) error {
	var err error

	// find our physical index
	physicalIndexes := i.FindPhysicalIndexes()

	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}

	// whether we need to remap our alias after building
	remapAlias := false

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || rebuild {
		physicalIndex, err = i.CreateNewIndex(IndexSettings)
		if err != nil {
			return errors.Wrap(err, "error creating new index")
		}
		logrus.WithField("indexer", i.Name()).WithField("index", physicalIndex).Info("created new physical index")
		remapAlias = true
	}

	lastModified, err := indexer.GetLastModified(i.ElasticURL, physicalIndex)
	if err != nil {
		return errors.Wrap(err, "error finding last modified")
	}

	logrus.WithField("indexer", i.Name()).WithField("index", physicalIndex).WithField("last_modified", lastModified).Info("indexing newer than last modified")

	// now index our docs
	start := time.Now()
	indexed, deleted, err := i.IndexModified(db, physicalIndex, lastModified.Add(-5*time.Second))
	if err != nil {
		return errors.Wrap(err, "error indexing documents")
	}

	i.RecordComplete(indexed, deleted, time.Since(start))

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := i.UpdateAlias(physicalIndex)
		if err != nil {
			return errors.Wrap(err, "error remapping alias")
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if cleanup {
		err := i.CleanupIndexes()
		if err != nil {
			return errors.Wrap(err, "error cleaning up old indexes")
		}
	}

	return nil
}

// IndexModified queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func (i *Indexer) IndexModified(db *sql.DB, index string, lastModified time.Time) (int, int, error) {
	batch := &bytes.Buffer{}
	createdCount, deletedCount, processedCount := 0, 0, 0

	if index == "" {
		return 0, 0, errors.New("empty index")
	}

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
			if queryCount%BatchSize == 0 {
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

		elapsed := time.Since(start)
		rate := float32(processedCount) / (float32(elapsed) / float32(time.Second))
		logrus.WithFields(map[string]interface{}{
			"rate":    int(rate),
			"added":   createdCount,
			"deleted": deletedCount,
			"elapsed": elapsed,
			"index":   index}).Info("updated contact index")

		rows.Close()
	}

	return createdCount, deletedCount, nil
}
