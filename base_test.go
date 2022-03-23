package indexer_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nyaruka/rp-indexer/contacts"
	"github.com/stretchr/testify/require"
)

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

	ci := contacts.NewIndexer("rp_elastic_test", ts.URL)
	ci.FindPhysicalIndexes()

	require.Equal(t, responseCounter, 4)
}
