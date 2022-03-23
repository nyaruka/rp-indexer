package indexer

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// CreateNewIndex creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func CreateNewIndex(url, alias string, settings json.RawMessage) (string, error) {
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
	_, err := MakeJSONRequest(http.MethodPut, createURL, settings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	log.WithField("index", physicalIndex).Info("created index")
	return physicalIndex, nil
}

// GetLastModified queries an index and finds the last modified document, returning its modified time
func GetLastModified(url string, index string) (time.Time, error) {
	lastModified := time.Time{}
	if index == "" {
		return lastModified, fmt.Errorf("empty index passed to GetLastModified")
	}

	// get the newest document on our index
	queryResponse := queryResponse{}
	_, err := MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", url, index), []byte(`{ "sort": [{ "modified_on_mu": "desc" }]}`), &queryResponse)
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
