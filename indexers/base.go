package indexers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/rp-indexer/v9/utils"
)

// indexes a document
const indexCommand = `{ "index": { "_id": %d, "version": %d, "version_type": "external", "routing": %d} }`

// deletes a document
const deleteCommand = `{ "delete" : { "_id": %d, "version": %d, "version_type": "external", "routing": %d} }`

type Stats struct {
	Indexed int64         // total number of documents indexed
	Deleted int64         // total number of documents deleted
	Elapsed time.Duration // total time spent actually indexing (excludes poll delay)
}

// Indexer is base interface for indexers
type Indexer interface {
	Name() string
	Index(db *sql.DB, rebuild, cleanup bool) (string, error)
	Stats() Stats

	GetESLastModified(index string) (time.Time, error)
	GetDBLastModified(ctx context.Context, db *sql.DB) (time.Time, error)
}

// IndexDefinition is what we pass to elastic to create an index,
// see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
type IndexDefinition struct {
	Settings struct {
		Index struct {
			NumberOfShards       int `json:"number_of_shards"`
			NumberOfReplicas     int `json:"number_of_replicas"`
			RoutingPartitionSize int `json:"routing_partition_size"`
		} `json:"index"`
		Analysis json.RawMessage `json:"analysis"`
	} `json:"settings"`
	Mappings json.RawMessage `json:"mappings"`
}

func newIndexDefinition(base []byte, shards, replicas int) *IndexDefinition {
	d := &IndexDefinition{}
	jsonx.MustUnmarshal(base, d)

	d.Settings.Index.NumberOfShards = shards
	d.Settings.Index.NumberOfReplicas = replicas
	return d
}

type baseIndexer struct {
	elasticURL string
	name       string // e.g. contacts, used as the alias
	definition *IndexDefinition

	stats Stats
}

func newBaseIndexer(elasticURL, name string, def *IndexDefinition) baseIndexer {
	return baseIndexer{elasticURL: elasticURL, name: name, definition: def}
}

func (i *baseIndexer) Name() string {
	return i.name
}

func (i *baseIndexer) Stats() Stats {
	return i.stats
}

func (i *baseIndexer) log() *slog.Logger {
	return slog.With("indexer", i.name)
}

// records indexing activity and updates statistics
func (i *baseIndexer) recordActivity(indexed, deleted int, elapsed time.Duration) {
	i.stats.Indexed += int64(indexed)
	i.stats.Deleted += int64(deleted)
	i.stats.Elapsed += elapsed

	i.log().Info("completed indexing", "indexed", indexed, "deleted", deleted, "elapsed", elapsed)
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

	i.log().Debug("found physical indexes", "indexes", indexes)

	return indexes
}

// creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func (i *baseIndexer) createNewIndex(def *IndexDefinition) (string, error) {
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
	settings := jsonx.MustMarshal(def)

	_, err := utils.MakeJSONRequest(http.MethodPut, fmt.Sprintf("%s/%s", i.elasticURL, index), settings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	i.log().Info("created new index", "index", index)

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

		slog.Debug("removing old alias", "indexer", i.name, "index", idx)
	}

	// add our new index
	add := addAliasCommand{}
	add.Add.Alias = i.name
	add.Add.Index = newIndex
	commands = append(commands, add)

	aliasJSON := jsonx.MustMarshal(aliasCommand{Actions: commands})

	_, err := utils.MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/_aliases", i.elasticURL), aliasJSON, nil)

	i.log().Info("updated alias", "index", newIndex)

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
			slog.Info("removing old index", "index", key)
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
func (i *baseIndexer) indexBatch(index string, batch []byte) (int, int, int, error) {
	response := indexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", i.elasticURL, index)

	_, err := utils.MakeJSONRequest(http.MethodPut, indexURL, batch, &response)
	if err != nil {
		return 0, 0, 0, err
	}

	createdCount, updatedCount, deletedCount, conflictedCount := 0, 0, 0, 0

	for _, item := range response.Items {
		if item.Index.ID != "" {
			slog.Debug("index response", "id", item.Index.ID, "status", item.Index.Status)
			if item.Index.Status == 200 {
				updatedCount++
			} else if item.Index.Status == 201 {
				createdCount++
			} else if item.Index.Status == 409 {
				conflictedCount++
			} else {
				slog.Error("error indexing document", "id", item.Index.ID, "status", item.Index.Status, "result", item.Index.Result)
			}
		} else if item.Delete.ID != "" {
			slog.Debug("delete response", "id", item.Index.ID, "status", item.Index.Status)
			if item.Delete.Status == 200 {
				deletedCount++
			} else if item.Delete.Status == 409 {
				conflictedCount++
			}
		} else {
			slog.Error("unparsed item in response")
		}
	}

	slog.Debug("indexed batch", "created", createdCount, "updated", updatedCount, "deleted", deletedCount, "conflicted", conflictedCount)

	return createdCount, updatedCount, deletedCount, nil
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

// GetESLastModified queries a concrete index and finds the last modified document, returning its modified time
func (i *baseIndexer) GetESLastModified(index string) (time.Time, error) {
	lastModified := time.Time{}

	// get the newest document on our index
	queryResponse := &queryResponse{}
	_, err := utils.MakeJSONRequest(
		http.MethodPost,
		fmt.Sprintf("%s/%s/_search", i.elasticURL, index),
		[]byte(`{ "sort": [{ "modified_on_mu": "desc" }], "_source": {"includes": ["modified_on", "id"]}, "size": 1, "track_total_hits": false}`),
		queryResponse,
	)
	if err != nil {
		return lastModified, err
	}

	if len(queryResponse.Hits.Hits) > 0 {
		lastModified = queryResponse.Hits.Hits[0].Source.ModifiedOn
	}
	return lastModified, nil
}
