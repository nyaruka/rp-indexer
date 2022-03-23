package contacts

import (
	"bytes"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	indexer "github.com/nyaruka/rp-indexer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var BatchSize = 500

// settings and mappings for our index
//go:embed index_settings.json
var IndexSettings json.RawMessage

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

type ContactIndexer struct {
	indexer.BaseIndexer
}

func NewIndexer(name, elasticURL string, rebuild, cleanup bool) indexer.Indexer {
	return &ContactIndexer{
		BaseIndexer: indexer.NewBaseIndexer(name, elasticURL, rebuild, cleanup),
	}
}

func (i *ContactIndexer) Index(db *sql.DB) error {
	var err error

	// find our physical index
	physicalIndexes := indexer.FindPhysicalIndexes(i.ElasticURL, i.Name())
	logrus.WithField("physicalIndexes", physicalIndexes).WithField("indexer", i.Name()).Debug("found physical indexes")

	physicalIndex := ""
	if len(physicalIndexes) > 0 {
		physicalIndex = physicalIndexes[0]
	}

	// whether we need to remap our alias after building
	remapAlias := false

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || i.Rebuild {
		physicalIndex, err = indexer.CreateNewIndex(i.ElasticURL, i.Name(), IndexSettings)
		if err != nil {
			return errors.Wrap(err, "error creating new index")
		}
		logrus.WithField("indexer", i.Name()).WithField("physicalIndex", physicalIndex).Info("created new physical index")
		remapAlias = true
	}

	lastModified, err := indexer.GetLastModified(i.ElasticURL, physicalIndex)
	if err != nil {
		return errors.Wrap(err, "error finding last modified")
	}

	logrus.WithField("last_modified", lastModified).WithField("index", physicalIndex).Info("indexing newer than last modified")

	// now index our docs
	start := time.Now()
	indexed, deleted, err := IndexModified(db, i.ElasticURL, physicalIndex, lastModified.Add(-5*time.Second))
	if err != nil {
		return errors.Wrap(err, "error indexing documents")
	}

	i.UpdateStats(indexed, deleted, time.Since(start))

	// if the index didn't previously exist or we are rebuilding, remap to our alias
	if remapAlias {
		err := indexer.MapIndexAlias(i.ElasticURL, i.Name(), physicalIndex)
		if err != nil {
			return errors.Wrap(err, "error remapping alias")
		}
		remapAlias = false
	}

	// cleanup our aliases if appropriate
	if i.Cleanup {
		err := indexer.CleanupIndexes(i.ElasticURL, i.Name())
		if err != nil {
			return errors.Wrap(err, "error cleaning up old indexes")
		}
	}

	return nil
}

// IndexModified queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func IndexModified(db *sql.DB, elasticURL string, index string, lastModified time.Time) (int, int, error) {
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
				created, deleted, err := indexBatch(elasticURL, index, batch.Bytes())
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
			created, deleted, err := indexBatch(elasticURL, index, batch.Bytes())
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

// indexes the batch of contacts
func indexBatch(elasticURL string, index string, batch []byte) (int, int, error) {
	response := indexer.IndexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", elasticURL, index)

	_, err := indexer.MakeJSONRequest(http.MethodPut, indexURL, batch, &response)
	if err != nil {
		return 0, 0, err
	}

	createdCount, deletedCount, conflictedCount := 0, 0, 0
	for _, item := range response.Items {
		if item.Index.ID != "" {
			logrus.WithField("id", item.Index.ID).WithField("status", item.Index.Status).Debug("index response")
			if item.Index.Status == 200 || item.Index.Status == 201 {
				createdCount++
			} else if item.Index.Status == 409 {
				conflictedCount++
			} else {
				logrus.WithField("id", item.Index.ID).WithField("batch", batch).WithField("result", item.Index.Result).Error("error indexing contact")
			}
		} else if item.Delete.ID != "" {
			logrus.WithField("id", item.Index.ID).WithField("status", item.Index.Status).Debug("delete response")
			if item.Delete.Status == 200 {
				deletedCount++
			} else if item.Delete.Status == 409 {
				conflictedCount++
			}
		} else {
			logrus.Error("unparsed item in response")
		}
	}
	logrus.WithField("created", createdCount).WithField("deleted", deletedCount).WithField("conflicted", conflictedCount).Debug("indexed batch")

	return createdCount, deletedCount, nil
}
