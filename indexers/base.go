package indexers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/rp-indexer/utils"
	"github.com/sirupsen/logrus"
)

// indexes a document
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

// deletes a document
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "version": %d, "version_type": "external", "routing": %d} }`

type Stats struct {
	Indexed int64         // total number of documents indexed
	Deleted int64         // total number of documents deleted
	Elapsed time.Duration // total time spent actually indexing
}

// Indexer is base interface for indexers
type Indexer interface {
	Name() string
	Index(db *sql.DB, rebuild, cleanup bool) (string, error)
	Stats() Stats
}

type baseIndexer struct {
	elasticURL string
	name       string // e.g. contacts, used as the alias

	stats Stats
}

func newBaseIndexer(elasticURL, name string) baseIndexer {
	return baseIndexer{elasticURL: elasticURL, name: name}
}

func (i *baseIndexer) Name() string {
	return i.name
}

func (i *baseIndexer) Stats() Stats {
	return i.stats
}

func (i *baseIndexer) log() *logrus.Entry {
	return logrus.WithField("indexer", i.name)
}

// records a complete index and updates statistics
func (i *baseIndexer) recordComplete(indexed, deleted int, elapsed time.Duration) {
	i.stats.Indexed += int64(indexed)
	i.stats.Deleted += int64(deleted)
	i.stats.Elapsed += elapsed

	i.log().WithField("indexed", indexed).WithField("deleted", deleted).WithField("elapsed", elapsed).Info("completed indexing")
}

// our response for figuring out the physical index for an alias
type infoResponse map[string]interface{}

// FindIndexes finds all our physical indexes
func (i *baseIndexer) FindIndexes() []string {
	response := infoResponse{}
	_, err := utils.MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", i.elasticURL, i.name), nil, &response)
	indexes := make([]string, 0)

	// error could mean a variety of things, but we'll figure that out later
	if err != nil {
		return indexes
	}

	// our top level key is our physical index name
	for key := range response {
		indexes = append(indexes, key)
	}

	// reverse sort order should put our newest index first
	sort.Sort(sort.Reverse(sort.StringSlice(indexes)))

	i.log().WithField("indexes", indexes).Debug("found physical indexes")

	return indexes
}

// creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func (i *baseIndexer) createNewIndex(settings json.RawMessage) (string, error) {
	// create our day-specific name
	index := fmt.Sprintf("%s_%s", i.name, time.Now().Format("2006_01_02"))
	idx := 0

	// check if it exists
	for {
		resp, err := http.Get(fmt.Sprintf("%s/%s", i.elasticURL, index))
		if err != nil {
			return "", err
		}
		// not found, great, move on
		if resp.StatusCode == http.StatusNotFound {
			break
		}

		// was found, increase our index and try again
		idx++
		index = fmt.Sprintf("%s_%s_%d", i.name, time.Now().Format("2006_01_02"), idx)
	}

	// create the new index
	_, err := utils.MakeJSONRequest(http.MethodPut, fmt.Sprintf("%s/%s?include_type_name=true", i.elasticURL, index), settings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	i.log().WithField("index", index).Info("created new index")

	return index, nil
}

// our top level command for remapping aliases
type aliasCommand struct {
	Actions []interface{} `json:"actions"`
}

// adds an alias for an index
type addAliasCommand struct {
	Add struct {
		Index string `json:"index"`
		Alias string `json:"alias"`
	} `json:"add"`
}

// removes an alias for an index
type removeAliasCommand struct {
	Remove struct {
		Index string `json:"index"`
		Alias string `json:"alias"`
	} `json:"remove"`
}

// maps this indexer's alias to the new physical index, removing existing aliases if they exist
func (i *baseIndexer) updateAlias(newIndex string) error {
	commands := make([]interface{}, 0)

	// find existing physical indexes
	existing := i.FindIndexes()
	for _, idx := range existing {
		remove := removeAliasCommand{}
		remove.Remove.Alias = i.name
		remove.Remove.Index = idx
		commands = append(commands, remove)

		logrus.WithField("indexer", i.name).WithField("index", idx).Debug("removing old alias")
	}

	// add our new index
	add := addAliasCommand{}
	add.Add.Alias = i.name
	add.Add.Index = newIndex
	commands = append(commands, add)

	aliasJSON := jsonx.MustMarshal(aliasCommand{Actions: commands})

	_, err := utils.MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/_aliases", i.elasticURL), aliasJSON, nil)

	i.log().WithField("index", newIndex).Info("updated alias")

	return err
}

// our response for our index health
type healthResponse struct {
	Indices map[string]struct {
		Status string `json:"status"`
	} `json:"indices"`
}

// removes all indexes that are older than the currently active index
func (i *baseIndexer) cleanupIndexes() error {
	// find our current indexes
	currents := i.FindIndexes()

	// no current indexes? this a noop
	if len(currents) == 0 {
		return nil
	}

	// find all the current indexes
	healthResponse := healthResponse{}
	_, err := utils.MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", i.elasticURL, "_cluster/health?level=indices"), nil, &healthResponse)
	if err != nil {
		return err
	}

	// for each active index, if it starts with our alias but is before our current index, remove it
	for key := range healthResponse.Indices {
		if strings.HasPrefix(key, i.name) && strings.Compare(key, currents[0]) < 0 {
			logrus.WithField("index", key).Info("removing old index")
			_, err = utils.MakeJSONRequest(http.MethodDelete, fmt.Sprintf("%s/%s", i.elasticURL, key), nil, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// our response for indexing contacts
type indexResponse struct {
	Items []struct {
		Index struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Result string `json:"result"`
		} `json:"index"`
		Delete struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
		} `json:"delete"`
	} `json:"items"`
}

// indexes the batch of contacts
func (i *baseIndexer) indexBatch(index string, batch []byte) (int, int, error) {
	response := indexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", i.elasticURL, index)

	_, err := utils.MakeJSONRequest(http.MethodPut, indexURL, batch, &response)
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
				logrus.WithField("id", item.Index.ID).WithField("status", item.Index.Status).WithField("result", item.Index.Result).Error("error indexing document")
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

// our response for finding the last modified document
type queryResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source struct {
				ID         int64     `json:"id"`
				ModifiedOn time.Time `json:"modified_on"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// GetLastModified queries a concrete index and finds the last modified document, returning its modified time
func (i *baseIndexer) GetLastModified(index string) (time.Time, error) {
	lastModified := time.Time{}

	// get the newest document on our index
	queryResponse := queryResponse{}
	_, err := utils.MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", i.elasticURL, index), []byte(`{ "sort": [{ "modified_on_mu": "desc" }]}`), &queryResponse)
	if err != nil {
		return lastModified, err
	}

	if len(queryResponse.Hits.Hits) > 0 {
		lastModified = queryResponse.Hits.Hits[0].Source.ModifiedOn
	}
	return lastModified, nil
}
