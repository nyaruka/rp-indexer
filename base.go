package indexer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/sirupsen/logrus"
)

// Indexer is base interface for indexers
type Indexer interface {
	Name() string
	Index(db *sql.DB, rebuild, cleanup bool) error
}

type BaseIndexer struct {
	name       string // e.g. contacts, used as based index name
	ElasticURL string

	// statistics
	indexedTotal int64
	deletedTotal int64
	elapsedTotal time.Duration
}

func NewBaseIndexer(name, elasticURL string) BaseIndexer {
	return BaseIndexer{name: name, ElasticURL: elasticURL}
}

func (i *BaseIndexer) Name() string {
	return i.name
}

func (i *BaseIndexer) Stats() (int64, int64, time.Duration) {
	return i.indexedTotal, i.deletedTotal, i.elapsedTotal
}

// RecordComplete records a complete index and updates statistics
func (i *BaseIndexer) RecordComplete(indexed, deleted int, elapsed time.Duration) {
	i.indexedTotal += int64(indexed)
	i.deletedTotal += int64(deleted)
	i.elapsedTotal += elapsed

	logrus.WithField("indexer", i.name).WithField("indexed", indexed).WithField("deleted", deleted).WithField("elapsed", elapsed).Info("completed indexing")
}

// our response for figuring out the physical index for an alias
type infoResponse map[string]interface{}

// FindPhysicalIndexes finds all our physical indexes
func (i *BaseIndexer) FindPhysicalIndexes() []string {
	response := infoResponse{}
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", i.ElasticURL, i.name), nil, &response)
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

	logrus.WithField("indexer", i.name).WithField("indexes", indexes).Debug("found physical indexes")

	return indexes
}

// CreateNewIndex creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func (i *BaseIndexer) CreateNewIndex(settings json.RawMessage) (string, error) {
	// create our day-specific name
	index := fmt.Sprintf("%s_%s", i.name, time.Now().Format("2006_01_02"))
	idx := 0

	// check if it exists
	for {
		resp, err := http.Get(fmt.Sprintf("%s/%s", i.ElasticURL, index))
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
	_, err := MakeJSONRequest(http.MethodPut, fmt.Sprintf("%s/%s?include_type_name=true", i.ElasticURL, index), settings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	logrus.WithField("indexer", i.name).WithField("index", index).Info("created new index")

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

// UpdateAlias maps the passed in alias to the new physical index, optionally removing
// existing aliases if they exit.
func (i *BaseIndexer) UpdateAlias(newIndex string) error {
	commands := make([]interface{}, 0)

	// find existing physical indexes
	existing := i.FindPhysicalIndexes()
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

	_, err := MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/_aliases", i.ElasticURL), aliasJSON, nil)

	logrus.WithField("indexer", i.name).WithField("index", newIndex).Debug("adding new alias")

	return err
}

// our response for our index health
type healthResponse struct {
	Indices map[string]struct {
		Status string `json:"status"`
	} `json:"indices"`
}

// CleanupIndexes removes all indexes that are older than the currently active index
func (i *BaseIndexer) CleanupIndexes() error {
	// find our current indexes
	currents := i.FindPhysicalIndexes()

	// no current indexes? this a noop
	if len(currents) == 0 {
		return nil
	}

	// find all the current indexes
	healthResponse := healthResponse{}
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", i.ElasticURL, "_cluster/health?level=indices"), nil, &healthResponse)
	if err != nil {
		return err
	}

	// for each active index, if it starts with our alias but is before our current index, remove it
	for key := range healthResponse.Indices {
		if strings.HasPrefix(key, i.name) && strings.Compare(key, currents[0]) < 0 {
			logrus.WithField("index", key).Info("removing old index")
			_, err = MakeJSONRequest(http.MethodDelete, fmt.Sprintf("%s/%s", i.ElasticURL, key), nil, nil)
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
func (i *BaseIndexer) IndexBatch(index string, batch []byte) (int, int, error) {
	response := indexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", i.ElasticURL, index)

	_, err := MakeJSONRequest(http.MethodPut, indexURL, batch, &response)
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
				logrus.WithField("id", item.Index.ID).WithField("batch", batch).WithField("result", item.Index.Result).Error("error indexing document")
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
