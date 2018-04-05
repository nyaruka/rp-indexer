package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	log "github.com/sirupsen/logrus"
)

// Creates a new index for the passed in alias.
//
// Note that we do not create an index with the passed name, instead creating one
// based on the day, for example `contacts_2018_03_05`, then create an alias from
// that index to `contacts`.
//
// If the day-specific name already exists, we append a .1 or .2 to the name.
func createNewIndex(url string, alias string) (string, error) {
	// create our day-specific name
	physicalIndex := fmt.Sprintf("%s_%s", alias, time.Now().Format("2006_01_02"))
	idx := 0

	// check if it exists
	for true {
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
	createURL := fmt.Sprintf("%s/%s", url, physicalIndex)
	_, err := makeJSONRequest(http.MethodPut, createURL, indexSettings, nil)
	if err != nil {
		return "", err
	}

	// all went well, return our physical index name
	log.WithField("index", physicalIndex).Info("created index")
	return physicalIndex, nil
}

// Queries our index and finds the last modified contact, returning it
func getLastModified(url string, index string) (time.Time, error) {
	lastModified := time.Time{}

	// get the newest document on our index
	queryResponse := queryResponse{}
	_, err := makeJSONRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", url, index), lastModifiedQuery, &queryResponse)
	if err != nil {
		return lastModified, err
	}

	if len(queryResponse.Hits.Hits) > 0 {
		lastModified = queryResponse.Hits.Hits[0].Source.ModifiedOn
	}
	return lastModified, nil
}

// Finds the physical index backing the passed in alias
func findPhysicalIndex(url string, alias string) string {
	indexResponse := infoResponse{}
	_, err := makeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, alias), "", &indexResponse)

	// error could mean a variety of things, but we'll figure that out later
	if err != nil {
		return ""
	}

	// our top level key is our physical index name
	for key := range indexResponse {
		return key
	}

	return ""
}

// Returns the JSON marshalled value of the passed in value
func jsonEscape(value interface{}) string {
	bs, _ := json.Marshal(value)
	return string(bs)
}

// Utility function to make a JSON request, optionally decoding the response into the passed in struct
func makeJSONRequest(method string, url string, body string, jsonStruct interface{}) (*http.Response, error) {
	req, _ := http.NewRequest(method, url, bytes.NewReader([]byte(body)))
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}

	// if we have a body, try to decode it
	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp, err
	}

	// error if we got a non-200
	if resp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("received non 200 response %d: %s", resp.StatusCode, jsonBody)
	}

	if jsonStruct == nil {
		return resp, nil
	}

	err = json.Unmarshal(jsonBody, jsonStruct)
	return resp, err
}

func logError(fatal bool, err error, msg string) {
	if fatal {
		log.WithError(err).Fatal(msg)
	} else {
		log.WithError(err).Error(msg)
		time.Sleep(time.Second * 5)
	}
}

func main() {
	config := config{
		ElasticURL: "http://localhost:9200",
		DB:         "postgres://localhost/rapidpro",
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

	physicalIndex := findPhysicalIndex(config.ElasticURL, config.Index)
	oldIndex := physicalIndex

	// doesn't exist or we are rebuilding, create it
	if physicalIndex == "" || config.Rebuild {
		physicalIndex, err = createNewIndex(config.ElasticURL, config.Index)
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		lastModified, err := getLastModified(config.ElasticURL, physicalIndex)
		if err != nil {
			logError(config.Rebuild, err, "error finding last modified")
			continue
		}

		start := time.Now()
		log.WithField("last_modified", lastModified).Info("indexing contacts newer than last modified")

		// now index our docs
		indexed, deleted, err := indexContacts(db, config.ElasticURL, physicalIndex, lastModified)
		if err != nil {
			logError(config.Rebuild, err, "error indexing contacts")
			continue
		}
		log.WithField("added", indexed).WithField("deleted", deleted).WithField("elapsed", time.Now().Sub(start)).Info("completed indexing")

		// if the index didn't previously exist or we are rebuilding, remap to our alias
		if oldIndex == "" || config.Rebuild {
			aliasMapping := fmt.Sprintf(addAliasCommand, jsonEscape(physicalIndex), jsonEscape(config.Index))
			if oldIndex != "" {
				aliasMapping = fmt.Sprintf(replaceAliasCommand, jsonEscape(oldIndex), jsonEscape(config.Index), jsonEscape(physicalIndex), jsonEscape(config.Index))
			}
			log.WithField("index", physicalIndex).WithField("alias", config.Index).Info("remapped index to alias")

			aliasURL := fmt.Sprintf("%s/_aliases", config.ElasticURL)
			_, err := makeJSONRequest(http.MethodPost, aliasURL, aliasMapping, nil)
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
			physicalIndex = findPhysicalIndex(config.ElasticURL, config.Index)
		}
	}
}

func indexBatch(elasticURL string, index string, batch string) (int, int, error) {
	response := indexResponse{}
	indexURL := fmt.Sprintf("%s/%s/_bulk", elasticURL, index)
	_, err := makeJSONRequest(http.MethodPut, indexURL, batch, &response)
	if err != nil {
		return 0, 0, err
	}

	createdCount, deletedCount := 0, 0
	for _, item := range response.Items {
		if item.Index.Status == 200 || item.Index.Status == 201 {
			createdCount++
		} else if item.Delete.Status == 200 {
			deletedCount++
		}
	}

	return createdCount, deletedCount, nil
}

func indexContacts(db *sql.DB, elasticURL string, index string, lastModified time.Time) (int, int, error) {
	batch := strings.Builder{}
	createdCount, deletedCount := 0, 0
	processedCount := 0

	var modifiedOn time.Time
	var contactJSON string
	var id, orgID int64
	var isActive bool

	start := time.Now()

	for {
		batchCount := 0
		batchModified := lastModified

		rows, err := db.Query(contactQuery, lastModified)
		defer rows.Close()

		// no more rows? return
		if err == sql.ErrNoRows {
			return 0, 0, nil
		}
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			err = rows.Scan(&orgID, &id, &modifiedOn, &isActive, &contactJSON)
			if err != nil {
				log.Fatal(err)
			}

			processedCount++

			if isActive {
				batch.WriteString(fmt.Sprintf(indexCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
				batch.WriteString(contactJSON)
				batch.WriteString("\n")
			} else {
				batch.WriteString(fmt.Sprintf(deleteCommand, id, modifiedOn.UnixNano(), orgID))
				batch.WriteString("\n")
			}

			// write to elastic search in batches of 500
			if processedCount%500 == 0 {
				created, deleted, err := indexBatch(elasticURL, index, batch.String())
				if err != nil {
					return 0, 0, err
				}
				batch.Reset()

				createdCount += created
				deletedCount += deleted
				batchCount += created
			}

			lastModified = modifiedOn
		}

		if batch.Len() > 0 {
			created, deleted, err := indexBatch(elasticURL, index, batch.String())
			if err != nil {
				return 0, 0, err
			}
			createdCount += created
			deletedCount += deleted
			batchCount += created
			batch.Reset()
		}

		// didn't add anything in this batch and our last modified stayed the same, seen it all, break out
		if batchCount == 0 && lastModified.Equal(batchModified) {
			break
		}

		elapsed := time.Now().Sub(start)
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

type config struct {
	ElasticURL string `help:"the url for our elastic search instance"`
	DB         string `help:"the connection string for our database"`
	Index      string `help:"the alias for our contact index"`
	Poll       int    `help:"the number of seconds to wait between checking for updated contacts"`
	Rebuild    bool   `help:"whether to rebuild the index, swapping it when complete, then exiting (default false)"`
}

const contactQuery = `
SELECT org_id, id, modified_on, is_active, row_to_json(t) FROM(
    SELECT id, org_id, uuid, name, language, is_stopped, is_blocked, is_active, created_on, modified_on,
    (
        SELECT array_to_json(array_agg(row_to_json(u))) FROM (
            SELECT scheme, path
            FROM contacts_contacturn
            WHERE contact_id=contacts_contact.id
        ) u
    ) as urns,
    (
        SELECT jsonb_agg(f.value) FROM (
            SELECT value||jsonb_build_object('field', key) as value from jsonb_each(contacts_contact.fields)
        ) as f
    ) as fields
    FROM contacts_contact
	WHERE is_test = FALSE AND modified_on >= $1
	ORDER BY modified_on ASC
	LIMIT 10000
) t
`

// settings and mappings for our index
const indexSettings = `
{
	"settings": {
		"index": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"routing_partition_size": 3
		},
		"analysis": {
            "filter": {
                "trigrams_filter": {
                    "type":     "ngram",
                    "min_gram": 3,
                    "max_gram": 3
				}
            },
            "analyzer": {
                "trigrams": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "trigrams_filter"
                    ]
				},
				"locations": {
					"tokenizer": "location_tokenizer",
					"filter": [
						"lowercase"
					]
				}
			},
			"tokenizer": {
				"location_tokenizer": {
				  "type": "pattern",
				  "pattern": "(.* > )?([^>]+)",
				  "group": 2
				}
			},
			"normalizer": {
				"lowercase": {
					"type": "custom",
					"char_filter": [],
					"filter": ["lowercase"]
				}
			}
        }
	},

	"mappings": {
		"_doc": {
			"_routing": {
				"required": true
			},
			"properties": {
				"fields": {
					"type": "nested",
					"properties": {
						"field": {
							"type": "keyword"
						},
						"text": {
							"type": "keyword",
							"ignore_above": 64,
							"normalizer": "lowercase"
						},
						"decimal": {
							"type": "scaled_float",
							"scaling_factor": 10000
						},
						"datetime": {
							"type": "date"
						},
						"state": {
							"type": "text",
							"analyzer": "locations"
						},
						"district": {
							"type": "text",
							"analyzer": "locations"
						},
						"ward": {
							"type": "text",
							"analyzer": "locations"
						}
					}
				},
				"urns": {
					"type": "nested",
					"properties": {
						"path": {
							"type": "text",
							"analyzer": "trigrams",
							"fields": {
								"keyword": {
									"type": "keyword",
									"ignore_above": 64,
									"normalizer": "lowercase"
								}
							}
						},
						"scheme": {
							"type": "keyword",
							"normalizer": "lowercase"
						}
					}
				},
				"uuid": {
					"type": "keyword"
				},
				"language": {
					"type": "keyword",
					"normalizer": "lowercase"
				},
				"modified_on": {
					"type": "date"
				},				
				"name": {
					"type": "text",
					"analyzer": "simple",
					"fields": {
						"keyword": {
							"type": "keyword",
							"ignore_above": 64,
							"normalizer": "lowercase"
						}
					}
				}
			}
		}
	}
}
`

// gets our last modified contact
const lastModifiedQuery = `{ "sort": [{ "modified_on": "desc"	}]}`

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "_version": %d, "_version_type": "external", "_routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "_version": %d, "_version_type": "external", "_routing": %d} }`

// adds an alias for an index
const addAliasCommand = `{"actions": [{ "add": { "index": %s, "alias": %s }}]}`

// atomically removes an alias for an index and adds a new one
const replaceAliasCommand = `{"actions": [{ "remove": { "index": %s, "alias": %s }}, { "add": { "index": %s, "alias": %s }}]}`

// our response for finding the most recent contact
type queryResponse struct {
	Hits struct {
		Total int `json:"total"`
		Hits  []struct {
			Source struct {
				ID         int64     `json:"id"`
				ModifiedOn time.Time `json:"modified_on"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// our response for indexing contacts
type indexResponse struct {
	Items []struct {
		Index struct {
			Status int `json:"status"`
		} `json:"index"`
		Delete struct {
			Status int `json:"status"`
		} `json:"delete"`
	} `json:"items"`
}

// our response for figuring out the physical index for an alias
type infoResponse map[string]interface{}
