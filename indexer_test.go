package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const elasticURL = "http://localhost:9200"
const indexName = "rp_elastic_test"

func setup(t *testing.T) (*sql.DB, *elastic.Client) {
	testDB, err := ioutil.ReadFile("testdb.sql")
	require.NoError(t, err)

	db, err := sql.Open("postgres", "postgres://temba:temba@localhost:5432/elastic_test?sslmode=disable")
	require.NoError(t, err)

	_, err = db.Exec(string(testDB))
	require.NoError(t, err)

	client, err := elastic.NewClient(elastic.SetURL(elasticURL), elastic.SetTraceLog(log.New(os.Stdout, "", log.LstdFlags)), elastic.SetSniff(false))
	require.NoError(t, err)

	existing := FindPhysicalIndexes(elasticURL, indexName)
	for _, idx := range existing {
		_, err = client.DeleteIndex(idx).Do(context.Background())
		require.NoError(t, err)
	}

	logrus.SetLevel(logrus.DebugLevel)

	return db, client
}

func assertQuery(t *testing.T, client *elastic.Client, index string, query elastic.Query, hits []int64) {
	results, err := client.Search().Index(index).Query(query).Sort("id", true).Pretty(true).Do(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(len(hits)), results.Hits.TotalHits)

	if int64(len(hits)) == results.Hits.TotalHits {
		for i, hit := range results.Hits.Hits {
			assert.Equal(t, fmt.Sprintf("%d", hits[i]), hit.Id)
		}
	}
}

func TestIndexing(t *testing.T) {
	batchSize = 4
	db, client := setup(t)

	physicalName, err := CreateNewIndex(elasticURL, indexName)
	assert.NoError(t, err)

	added, deleted, err := IndexContacts(db, elasticURL, physicalName, time.Time{})
	assert.NoError(t, err)
	assert.Equal(t, 9, added)
	assert.Equal(t, 0, deleted)

	time.Sleep(2 * time.Second)

	assertQuery(t, client, physicalName, elastic.NewMatchQuery("name", "JOHn"), []int64{4})

	// prefix on name matches both john and joanne, but no ajodi
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("name", "JO"), []int64{4, 6})
	assertQuery(t, client, physicalName, elastic.NewTermQuery("name.keyword", "JOHN DOE"), []int64{4})

	// can search on both first and last name
	boolQuery := elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("name", "john"),
		elastic.NewMatchQuery("name", "doe"))
	assertQuery(t, client, physicalName, boolQuery, []int64{4})

	// can search on a long name
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("name", "Ajodinabiff"), []int64{5})

	assertQuery(t, client, physicalName, elastic.NewMatchQuery("language", "eng"), []int64{1})

	// test contact, not indexed
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("language", "fra"), []int64{})

	assertQuery(t, client, physicalName, elastic.NewMatchQuery("is_blocked", "true"), []int64{3})
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("is_stopped", "true"), []int64{2})

	assertQuery(t, client, physicalName, elastic.NewMatchQuery("status", "B"), []int64{3})
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("status", "S"), []int64{2})

	assertQuery(t, client, physicalName, elastic.NewMatchQuery("org_id", "1"), []int64{1, 2, 3, 4})

	// created_on range query
	assertQuery(t, client, physicalName, elastic.NewRangeQuery("created_on").Gt("2017-01-01"), []int64{1, 6, 8})

	// last_seen_on range query
	assertQuery(t, client, physicalName, elastic.NewRangeQuery("last_seen_on").Lt("2019-01-01"), []int64{3, 4})

	// last_seen_on is set / not set queries
	assertQuery(t, client, physicalName, elastic.NewExistsQuery("last_seen_on"), []int64{1, 2, 3, 4, 5, 6})
	assertQuery(t, client, physicalName, elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery("last_seen_on")), []int64{7, 8, 9})

	// urn query
	query := elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("urns.scheme", "facebook"),
		elastic.NewMatchQuery("urns.path.keyword", "1000001")))
	assertQuery(t, client, physicalName, query, []int64{8})

	// urn substring query
	query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("urns.scheme", "tel"),
		elastic.NewMatchPhraseQuery("urns.path", "779")))
	assertQuery(t, client, physicalName, query, []int64{1, 2, 3, 6})

	// urn substring query with more characters (77911)
	query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("urns.scheme", "tel"),
		elastic.NewMatchPhraseQuery("urns.path", "77911")))
	assertQuery(t, client, physicalName, query, []int64{1})

	// urn substring query with more characters (600055)
	query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("urns.scheme", "tel"),
		elastic.NewMatchPhraseQuery("urns.path", "600055")))
	assertQuery(t, client, physicalName, query, []int64{5})

	// match a contact with multiple tel urns
	query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("urns.scheme", "tel"),
		elastic.NewMatchPhraseQuery("urns.path", "222")))
	assertQuery(t, client, physicalName, query, []int64{1})

	// text query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
		elastic.NewMatchQuery("fields.text", "the rock")))
	assertQuery(t, client, physicalName, query, []int64{1})

	// people with no nickname
	notQuery := elastic.NewBoolQuery().MustNot(
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
			elastic.NewExistsQuery("fields.text"))))
	assertQuery(t, client, physicalName, notQuery, []int64{2, 3, 4, 5, 6, 7, 8, 9})

	// no tokenizing of field text
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
		elastic.NewMatchQuery("fields.text", "rock")))
	assertQuery(t, client, physicalName, query, []int64{})

	// number field range query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "05bca1cd-e322-4837-9595-86d0d85e5adb"),
		elastic.NewRangeQuery("fields.number").Gt(10)))
	assertQuery(t, client, physicalName, query, []int64{2})

	// datetime field range query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "e0eac267-463a-4c00-9732-cab62df07b16"),
		elastic.NewRangeQuery("fields.datetime").Lt(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))))
	assertQuery(t, client, physicalName, query, []int64{3})

	// state query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
		elastic.NewMatchPhraseQuery("fields.state", "washington")))
	assertQuery(t, client, physicalName, query, []int64{5})

	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
		elastic.NewMatchQuery("fields.state_keyword", "  washington")))
	assertQuery(t, client, physicalName, query, []int64{5})

	// doesn't include country
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
		elastic.NewMatchQuery("fields.state_keyword", "usa")))
	assertQuery(t, client, physicalName, query, []int64{})

	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
		elastic.NewMatchPhraseQuery("fields.state", "usa")))
	assertQuery(t, client, physicalName, query, []int64{})

	// district query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
		elastic.NewMatchPhraseQuery("fields.district", "king")))
	assertQuery(t, client, physicalName, query, []int64{7, 9})

	// phrase matches all
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
		elastic.NewMatchPhraseQuery("fields.district", "King Côunty")))
	assertQuery(t, client, physicalName, query, []int64{7})

	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
		elastic.NewMatchQuery("fields.district_keyword", "King Côunty")))
	assertQuery(t, client, physicalName, query, []int64{7})

	// ward query
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
		elastic.NewMatchPhraseQuery("fields.ward", "district")))
	assertQuery(t, client, physicalName, query, []int64{8})

	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
		elastic.NewMatchQuery("fields.ward_keyword", "central district")))
	assertQuery(t, client, physicalName, query, []int64{8})

	// no substring though on keyword
	query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
		elastic.NewMatchQuery("fields.ward_keyword", "district")))
	assertQuery(t, client, physicalName, query, []int64{})

	// group query
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("groups", "4ea0f313-2f62-4e57-bdf0-232b5191dd57"), []int64{1})
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("groups", "529bac39-550a-4d6f-817c-1833f3449007"), []int64{1, 2})
	assertQuery(t, client, physicalName, elastic.NewMatchQuery("groups", "4c016340-468d-4675-a974-15cb7a45a5ab"), []int64{})

	lastModified, err := GetLastModified(elasticURL, physicalName)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2017, 11, 10, 21, 11, 59, 890662000, time.UTC), lastModified.In(time.UTC))

	// map our index over
	err = MapIndexAlias(elasticURL, indexName, physicalName)
	assert.NoError(t, err)
	time.Sleep(5 * time.Second)

	// try a test query to check it worked
	assertQuery(t, client, indexName, elastic.NewMatchQuery("name", "john"), []int64{4})

	// look up our mapping
	physical := FindPhysicalIndexes(elasticURL, indexName)
	assert.Equal(t, physicalName, physical[0])

	// rebuild again
	newIndex, err := CreateNewIndex(elasticURL, indexName)
	assert.NoError(t, err)

	added, deleted, err = IndexContacts(db, elasticURL, newIndex, time.Time{})
	assert.NoError(t, err)
	assert.Equal(t, 9, added)
	assert.Equal(t, 0, deleted)

	// remap again
	err = MapIndexAlias(elasticURL, indexName, newIndex)
	assert.NoError(t, err)
	time.Sleep(5 * time.Second)

	// old index still around
	resp, err := http.Get(fmt.Sprintf("%s/%s", elasticURL, physicalName))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK)

	// cleanup our indexes, will remove our original index
	err = CleanupIndexes(elasticURL, indexName)
	assert.NoError(t, err)

	// old physical index should be gone
	resp, err = http.Get(fmt.Sprintf("%s/%s", elasticURL, physicalName))
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusNotFound)

	// new index still works
	assertQuery(t, client, newIndex, elastic.NewMatchQuery("name", "john"), []int64{4})

	// update our database, removing one contact, updating another
	dbUpdate, err := ioutil.ReadFile("testdb_update.sql")
	assert.NoError(t, err)
	_, err = db.Exec(string(dbUpdate))
	assert.NoError(t, err)

	added, deleted, err = IndexContacts(db, elasticURL, indexName, lastModified)
	assert.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.Equal(t, 1, deleted)

	time.Sleep(5 * time.Second)

	// should only match new john, old john is gone
	assertQuery(t, client, indexName, elastic.NewMatchQuery("name", "john"), []int64{2})

	// 3 is no longer in our group
	assertQuery(t, client, indexName, elastic.NewMatchQuery("groups", "529bac39-550a-4d6f-817c-1833f3449007"), []int64{1})

}
func TestRetryServer(t *testing.T) {
	responseCounter := 0
	responses := []func(w http.ResponseWriter, r *http.Request){
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "5")
		},
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1")
		},
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1")
		},
		func(w http.ResponseWriter, r *http.Request) {
			resp := `{
				"took": 1,
				"timed_out": false,
				"_shards": {
				  "total": 2,
				  "successful": 2,
				  "skipped": 0,
				  "failed": 0
				},
				"hits": {
				  "total": 1,
				  "max_score": null,
				  "hits": [
					{
					  "_index": "rp_elastic_test_2020_08_14_1",
					  "_type": "_doc",
					  "_id": "1",
					  "_score": null,
					  "_routing": "1",
					  "_source": {
						"id": 1,
						"org_id": 1,
						"uuid": "c7a2dd87-a80e-420b-8431-ca48d422e924",
						"name": null,
						"language": "eng",
						"is_stopped": false,
						"is_blocked": false,
						"is_active": true,
						"created_on": "2017-11-10T16:11:59.890662-05:00",
						"modified_on": "2017-11-10T16:11:59.890662-05:00",
						"last_seen_on": "2020-08-04T21:11:00-04:00",
						"modified_on_mu": 1.510348319890662e15,
						"urns": [
						  {
							"scheme": "tel",
							"path": "+12067791111"
						  },
						  {
							"scheme": "tel",
							"path": "+12067792222"
						  }
						],
						"fields": [
						  {
							"text": "the rock",
							"field": "17103bb1-1b48-4b70-92f7-1f6b73bd3488"
						  }
						],
						"groups": [
						  "4ea0f313-2f62-4e57-bdf0-232b5191dd57",
						  "529bac39-550a-4d6f-817c-1833f3449007"
						]
					  },
					  "sort": [1]
					}
				  ]
				}
			  }`

			w.Write([]byte(resp))
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		responses[responseCounter](w, r)
		responseCounter++
	}))
	defer ts.Close()
	FindPhysicalIndexes(ts.URL, "rp_elastic_test")
	require.Equal(t, responseCounter, 4)
}
