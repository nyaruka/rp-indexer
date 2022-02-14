package indexer

import (
	"bytes"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nyaruka/rp-indexer/contacts"
	log "github.com/sirupsen/logrus"
)

var batchSize = 500

// CreateNewIndex creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func CreateNewIndex(url string, alias string) (string, error) {
	// create our day-specific name
	physicalIndex := fmt.Sprintf("%s_%s", alias, time.Now().Format("2006_01_02"))
	idx := 0

	// check if it exists
	for {
		resp, err := http.Get(fmt.Sprintf("%s/%s", url, physicalIndex))
		if err != nil {
			return "", err
		}
		// not found, great, move on
		if resp.StatusCode == http.StatusNotFound {
			break
		}

		// was found, increase our index and try again
		idx++
		physicalIndex = fmt.Sprintf("%s_%s_%d", alias, time.Now().Format("2006_01_02"), idx)
	}

	// initialize our index
	createURL := fmt.Sprintf("%s/%s?include_type_name=true", url, physicalIndex)
	_, err := MakeJSONRequest(http.MethodPut, createURL, contacts.IndexSettings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	log.WithField("index", physicalIndex).Info("created index")
	return physicalIndex, nil
}

// GetLastModified queries our index and finds the last modified contact, returning it
func GetLastModified(url string, index string) (time.Time, error) {
	lastModified := time.Time{}
	if index == "" {
		return lastModified, fmt.Errorf("empty index passed to GetLastModified")
	}

	// get the newest document on our index
	queryResponse := queryResponse{}
	_, err := MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", url, index), lastModifiedQuery, &queryResponse)
	if err != nil {
		return lastModified, err
	}

	if len(queryResponse.Hits.Hits) > 0 {
		lastModified = queryResponse.Hits.Hits[0].Source.ModifiedOn
	}
	return lastModified, nil
}

// FindPhysicalIndexes finds all the physical indexes for the passed in alias
func FindPhysicalIndexes(url string, alias string) []string {
	indexResponse := infoResponse{}
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, alias), nil, &indexResponse)
	indexes := make([]string, 0)

	// error could mean a variety of things, but we'll figure that out later
	if err != nil {
		return indexes
	}

	// our top level key is our physical index name
	for key := range indexResponse {
		indexes = append(indexes, key)
	}

	// reverse sort order should put our newest index first
	sort.Sort(sort.Reverse(sort.StringSlice(indexes)))
	return indexes
}

// CleanupIndexes removes all indexes that are older than the currently active index
func CleanupIndexes(url string, alias string) error {
	// find our current indexes
	currents := FindPhysicalIndexes(url, alias)

	// no current indexes? this a noop
	if len(currents) == 0 {
		return nil
	}

	// find all the current indexes
	healthResponse := healthResponse{}
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, "_cluster/health?level=indices"), nil, &healthResponse)
	if err != nil {
		return err
	}

	// for each active index, if it starts with our alias but is before our current index, remove it
	for key := range healthResponse.Indices {
		if strings.HasPrefix(key, alias) && strings.Compare(key, currents[0]) < 0 {
			log.WithField("index", key).Info("removing old index")
			_, err = MakeJSONRequest(http.MethodDelete, fmt.Sprintf("%s/%s", url, key), nil, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// IndexBatch indexes the batch of contacts
func IndexBatch(elasticURL string, index string, batch []byte) (int, int, error) {
	response := indexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", elasticURL, index)

	_, err := MakeJSONRequest(http.MethodPut, indexURL, batch, &response)
	if err != nil {
		return 0, 0, err
	}

	createdCount, deletedCount, conflictedCount := 0, 0, 0
	for _, item := range response.Items {
		if item.Index.ID != "" {
			log.WithField("id", item.Index.ID).WithField("status", item.Index.Status).Debug("index response")
			if item.Index.Status == 200 || item.Index.Status == 201 {
				createdCount++
			} else if item.Index.Status == 409 {
				conflictedCount++
			} else {
				log.WithField("id", item.Index.ID).WithField("batch", batch).WithField("result", item.Index.Result).Error("error indexing contact")
			}
		} else if item.Delete.ID != "" {
			log.WithField("id", item.Index.ID).WithField("status", item.Index.Status).Debug("delete response")
			if item.Delete.Status == 200 {
				deletedCount++
			} else if item.Delete.Status == 409 {
				conflictedCount++
			}
		} else {
			log.Error("unparsed item in response")
		}
	}
	log.WithField("created", createdCount).WithField("deleted", deletedCount).WithField("conflicted", conflictedCount).Debug("indexed batch")

	return createdCount, deletedCount, nil
}

// IndexContacts queries and indexes all contacts with a lastModified greater than or equal to the passed in time
func IndexContacts(db *sql.DB, elasticURL string, index string, lastModified time.Time) (int, int, error) {
	batch := &bytes.Buffer{}
	createdCount, deletedCount, processedCount := 0, 0, 0

	if index == "" {
		return createdCount, deletedCount, fmt.Errorf("empty index passed to IndexContacts")
	}

	var modifiedOn time.Time
	var contactJSON string
	var id, orgID int64
	var isActive bool

	start := time.Now()

	for {
		rows, err := contacts.FetchModified(db, lastModified)

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
				log.WithField("id", id).WithField("modifiedOn", modifiedOn).WithField("contact", contactJSON).Debug("modified contact")
				batch.WriteString(fmt.Sprintf(indexCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
				batch.WriteString(contactJSON)
				batch.WriteString("\n")
			} else {
				log.WithField("id", id).WithField("modifiedOn", modifiedOn).Debug("deleted contact")
				batch.WriteString(fmt.Sprintf(deleteCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
			}

			// write to elastic search in batches
			if queryCount%batchSize == 0 {
				created, deleted, err := IndexBatch(elasticURL, index, batch.Bytes())
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
			created, deleted, err := IndexBatch(elasticURL, index, batch.Bytes())
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
		log.WithFields(map[string]interface{}{
			"rate":    int(rate),
			"added":   createdCount,
			"deleted": deletedCount,
			"elapsed": elapsed,
			"index":   index}).Info("updated contact index")

		rows.Close()
	}

	return createdCount, deletedCount, nil
}

// MapIndexAlias maps the passed in alias to the new physical index, optionally removing
// existing aliases if they exit.
func MapIndexAlias(elasticURL string, alias string, newIndex string) error {
	commands := make([]interface{}, 0)

	// find existing physical indexes
	existing := FindPhysicalIndexes(elasticURL, alias)
	for _, idx := range existing {
		remove := removeAliasCommand{}
		remove.Remove.Alias = alias
		remove.Remove.Index = idx
		commands = append(commands, remove)

		log.WithField("index", idx).WithField("alias", alias).Info("removing old alias")
	}

	// add our new index
	add := addAliasCommand{}
	add.Add.Alias = alias
	add.Add.Index = newIndex
	commands = append(commands, add)

	log.WithField("index", newIndex).WithField("alias", alias).Info("adding new alias")

	aliasURL := fmt.Sprintf("%s/_aliases", elasticURL)
	aliasJSON, err := json.Marshal(aliasCommand{Actions: commands})
	if err != nil {
		return err
	}
	_, err = MakeJSONRequest(http.MethodPost, aliasURL, aliasJSON, nil)
	return err
}

// gets our last modified contact
var lastModifiedQuery = []byte(`{ "sort": [{ "modified_on_mu": "desc" }]}`)

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`
