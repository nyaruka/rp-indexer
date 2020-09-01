package indexer

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	log "github.com/sirupsen/logrus"
)

var batchSize = 500

var retryConfig *httpx.RetryConfig

func init() {
	//setup httpx retry configuration
	var retrycount = 5
	var initialBackoff = 1 * time.Second
	retryConfig = ElasticRetries(initialBackoff, retrycount)
}

func ElasticRetries(initialBackoff time.Duration, count int) *httpx.RetryConfig {
	backoffs := make([]time.Duration, count)
	backoffs[0] = initialBackoff
	for i := 1; i < count; i++ {
		backoffs[i] = backoffs[i-1] * 2
	}
	return &httpx.RetryConfig{Backoffs: backoffs, ShouldRetry: ShouldRetry}
}
func ShouldRetry(request *http.Request, response *http.Response, withDelay time.Duration) bool {

	// 429 Too Many Requests is recoverable. Sometimes the server puts
	// a Retry-After response header to indicate when the server is
	// available to start processing request from client.
	if response.StatusCode == http.StatusTooManyRequests {
		return true
	}

	// check for unexpected EOF
	bodyBytes, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		log.WithError(err).Error("error reading ES response, retrying")
		return true
	}

	response.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	return false
}

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
	_, err := MakeJSONRequest(http.MethodPut, createURL, indexSettings, nil)
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
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, alias), "", &indexResponse)
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
	_, err := MakeJSONRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, "_cluster/health?level=indices"), "", &healthResponse)
	if err != nil {
		return err
	}

	// for each active index, if it starts with our alias but is before our current index, remove it
	for key := range healthResponse.Indices {
		if strings.HasPrefix(key, alias) && strings.Compare(key, currents[0]) < 0 {
			log.WithField("index", key).Info("removing old index")
			_, err = MakeJSONRequest(http.MethodDelete, fmt.Sprintf("%s/%s", url, key), "", nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// MakeJSONRequest is a utility function to make a JSON request, optionally decoding the response into the passed in struct
func MakeJSONRequest(method string, url string, body string, jsonStruct interface{}) (*http.Response, error) {
	req, _ := http.NewRequest(method, url, bytes.NewReader([]byte(body)))
	req.Header.Add("Content-Type", "application/json")
	resp, err := httpx.Do(http.DefaultClient, req, retryConfig, nil)

	l := log.WithField("url", url).WithField("method", method).WithField("request", body)
	if err != nil {
		l.WithError(err).Error("error making ES request")
		return resp, err
	}
	defer resp.Body.Close()

	// if we have a body, try to decode it
	jsonBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		l.WithError(err).Error("error reading ES response")
		return resp, err
	}

	l = l.WithField("response", string(jsonBody)).WithField("status", resp.StatusCode)

	// error if we got a non-200
	if resp.StatusCode != http.StatusOK {
		l.WithError(err).Error("error reaching ES")
		return resp, fmt.Errorf("received non 200 response %d: %s", resp.StatusCode, jsonBody)
	}

	if jsonStruct == nil {
		l.Debug("ES request successful")
		return resp, nil
	}

	err = json.Unmarshal(jsonBody, jsonStruct)
	if err != nil {
		l.WithError(err).Error("error unmarshalling ES response")
		return resp, err
	}

	l.Debug("ES request successful")
	return resp, nil
}

// IndexBatch indexes the batch of contacts
func IndexBatch(elasticURL string, index string, batch string) (int, int, error) {
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
	batch := strings.Builder{}
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
		rows, err := db.Query(contactQuery, lastModified)

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
				created, deleted, err := IndexBatch(elasticURL, index, batch.String())
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
			created, deleted, err := IndexBatch(elasticURL, index, batch.String())
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
	_, err = MakeJSONRequest(http.MethodPost, aliasURL, string(aliasJSON), nil)
	return err
}

const contactQuery = `
SELECT org_id, id, modified_on, is_active, row_to_json(t) FROM (
  SELECT
   id, org_id, uuid, name, language, status, status = 'S' AS is_stopped, status = 'B' AS is_blocked, is_active, created_on, modified_on, last_seen_on,
   EXTRACT(EPOCH FROM modified_on) * 1000000 as modified_on_mu,
   (
     SELECT array_to_json(array_agg(row_to_json(u)))
     FROM (
            SELECT scheme, path
            FROM contacts_contacturn
            WHERE contact_id = contacts_contact.id
          ) u
   ) as urns,
   (
     SELECT jsonb_agg(f.value)
     FROM (
                       select case
                    when value ? 'ward'
                      then jsonb_build_object(
                        'ward_keyword', trim(substring(value ->> 'ward' from  '(?!.* > )([\w ]+)'))
                      )
                    else '{}' :: jsonb
                    end || district_value.value as value
           FROM (
                  select case
                           when value ? 'district'
                             then jsonb_build_object(
                               'district_keyword', trim(substring(value ->> 'district' from  '(?!.* > )([\w ]+)'))
                             )
                           else '{}' :: jsonb
                           end || state_value.value as value
                  FROM (

                         select case
                                  when value ? 'state'
                                    then jsonb_build_object(
                                      'state_keyword', trim(substring(value ->> 'state' from  '(?!.* > )([\w ]+)'))
                                    )
                                  else '{}' :: jsonb
                                  end ||
                                jsonb_build_object('field', key) || value as value
                         from jsonb_each(contacts_contact.fields)
                       ) state_value
                ) as district_value
          ) as f
   ) as fields,
   (
     SELECT array_to_json(array_agg(g.uuid))
     FROM (
            SELECT contacts_contactgroup.uuid
            FROM contacts_contactgroup_contacts, contacts_contactgroup
            WHERE contact_id = contacts_contact.id AND
                  contacts_contactgroup_contacts.contactgroup_id = contacts_contactgroup.id
          ) g
   ) as groups
  FROM contacts_contact
  WHERE modified_on >= $1
  ORDER BY modified_on ASC
  LIMIT 500000
) t;
`

// settings and mappings for our index
const indexSettings = `
{
	"settings": {
		"index": {
			"number_of_shards": 2,
			"number_of_replicas": 1,
			"routing_partition_size": 1
		},
		"analysis": {
            "analyzer": {
                "trigrams": {
                    "type":      "custom",
                    "tokenizer": "trigram",
                    "filter": [
                        "lowercase"
                    ]
				},
				"locations": {
					"tokenizer": "location_tokenizer",
					"filter": [
						"lowercase",
						"word_delimiter"
					]
				},
				"prefix": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "prefix_filter"
                    ]
				},
				"name_search": {
					"type": "custom",
					"tokenizer": "standard",
					"filter": [
						"lowercase",
						"max_length"
					]
				}
			},
			"tokenizer": {
				"location_tokenizer": {
				  "type": "pattern",
				  "pattern": "(.* > )?([^>]+)",
				  "group": 2
				},
				"trigram": {
					"type" : "ngram",
					"min_gram" : 3,
					"max_gram" : 3
				}
			},
			"normalizer": {
				"lowercase": {
					"type": "custom",
					"char_filter": [],
					"filter": ["lowercase", "trim"]
				}
			},
			"filter": {
                "prefix_filter": {
                    "type":     "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 8
				},
				"max_length":{
					"type": "truncate",
					"length": 8
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
							"normalizer": "lowercase"
						},
						"number": {
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
                        "state_keyword": {
							"type": "keyword",
							"normalizer": "lowercase"
                        },
						"district": {
							"type": "text",
							"analyzer": "locations"
						},
						"district_keyword": {
							"type": "keyword",
							"normalizer": "lowercase"
                        },
						"ward": {
							"type": "text",
							"analyzer": "locations"
						},
						"ward_keyword": {
							"type": "keyword",
							"normalizer": "lowercase"
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
				"groups": {
					"type": "keyword"
				},
				"uuid": {
					"type": "keyword"
				},
				"status": {
					"type": "keyword"
				},
				"language": {
					"type": "keyword",
					"normalizer": "lowercase"
				},
				"modified_on": {
					"type": "date"
				},
				"created_on": {
					"type": "date"
				},
				"modified_on_mu": {
					"type": "long"
				},
				"last_seen_on": {
					"type": "date"
				},
				"name": {
					"type": "text",
					"analyzer": "prefix",
					"search_analyzer": "name_search",
					"fields": {
						"keyword": {
							"type": "keyword",
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
const lastModifiedQuery = `{ "sort": [{ "modified_on_mu": "desc" }]}`

// indexes a contact
const indexCommand = `{ "index": { "_id": %d, "_type": "_doc", "_version": %d, "_version_type": "external", "_routing": %d} }`

// deletes a contact
const deleteCommand = `{ "delete" : { "_id": %d, "_type": "_doc", "_version": %d, "_version_type": "external", "_routing": %d} }`

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

// our top level command for remapping aliases
type aliasCommand struct {
	Actions []interface{} `json:"actions"`
}

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

// our response for our index health
type healthResponse struct {
	Indices map[string]struct {
		Status string `json:"status"`
	} `json:"indices"`
}

// our response for figuring out the physical index for an alias
type infoResponse map[string]interface{}
