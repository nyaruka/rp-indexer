package indexers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/nyaruka/rp-indexer/v9/indexers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var contactQueryTests = []struct {
	query    string
	expected []int64
}{
	{`{"match":{"org_id":{"query":"1"}}}`, []int64{1, 2, 3, 4}},
	{`{"match":{"name":{"query":"JOHn"}}}`, []int64{4}},
	{`{"term":{"name.keyword":"JOHN DOE"}}`, []int64{4}},
	{`{"bool":{"must":[{"match":{"name":{"query":"john"}}},{"match":{"name":{"query":"doe"}}}]}}`, []int64{4}},
	{`{"match":{"name":{"query":"Ajodinabiff"}}}`, []int64{5}},
	{`{"match":{"language":{"query":"eng"}}}`, []int64{1}},
	{`{"match":{"status":{"query":"B"}}}`, []int64{3}},
	{`{"match":{"status":{"query":"S"}}}`, []int64{2}},
	{`{"match":{"tickets":{"query":2}}}`, []int64{1}},
	{`{"match":{"tickets":{"query":1}}}`, []int64{2, 3}},
	{`{"range":{"tickets":{"from":0,"include_lower":false,"include_upper":true,"to":null}}}`, []int64{1, 2, 3}},
	{`{"match":{"flow_id":{"query":1}}}`, []int64{2, 3}},
	{`{"match":{"flow_id":{"query":2}}}`, []int64{4}},
	{`{"match":{"flow_history_ids":{"query":1}}}`, []int64{1, 2, 3}},
	{`{"match":{"flow_history_ids":{"query":2}}}`, []int64{1, 2}},
	{`{"range":{"created_on":{"from":"2017-01-01","include_lower":false,"include_upper":true,"to":null}}}`, []int64{1, 6, 8}},
	{`{"range":{"last_seen_on":{"from":null,"include_lower":true,"include_upper":false,"to":"2019-01-01"}}}`, []int64{3, 4}},
	{`{"exists":{"field":"last_seen_on"}}`, []int64{1, 2, 3, 4, 5, 6}},
	{`{"bool":{"must_not":{"exists":{"field":"last_seen_on"}}}}`, []int64{7, 8, 9}},
	{`{"nested":{"path":"urns","query":{"bool":{"must":[{"match":{"urns.scheme":{"query":"facebook"}}},{"match":{"urns.path.keyword":{"query":"1000001"}}}]}}}}`, []int64{8}},
	{`{"nested":{"path":"urns","query":{"bool":{"must":[{"match":{"urns.scheme":{"query":"tel"}}},{"match_phrase":{"urns.path":{"query":"779"}}}]}}}}`, []int64{1, 2, 3, 6}},
	{`{"nested":{"path":"urns","query":{"bool":{"must":[{"match":{"urns.scheme":{"query":"tel"}}},{"match_phrase":{"urns.path":{"query":"77911"}}}]}}}}`, []int64{1}},
	{`{"nested":{"path":"urns","query":{"bool":{"must":[{"match":{"urns.scheme":{"query":"tel"}}},{"match_phrase":{"urns.path":{"query":"600055"}}}]}}}}`, []int64{5}},
	{`{"nested":{"path":"urns","query":{"bool":{"must":[{"match":{"urns.scheme":{"query":"tel"}}},{"match_phrase":{"urns.path":{"query":"222"}}}]}}}}`, []int64{1}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"17103bb1-1b48-4b70-92f7-1f6b73bd3488"}}},{"match":{"fields.text":{"query":"the rock"}}}]}}}}`, []int64{1}},
	{`{"bool":{"must_not":{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"17103bb1-1b48-4b70-92f7-1f6b73bd3488"}}},{"exists":{"field":"fields.text"}}]}}}}}}`, []int64{2, 3, 4, 5, 6, 7, 8, 9}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"17103bb1-1b48-4b70-92f7-1f6b73bd3488"}}},{"match":{"fields.text":{"query":"rock"}}}]}}}}`, []int64{}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"05bca1cd-e322-4837-9595-86d0d85e5adb"}}},{"range":{"fields.number":{"from":10,"include_lower":false,"include_upper":true,"to":null}}}]}}}}`, []int64{2}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"e0eac267-463a-4c00-9732-cab62df07b16"}}},{"range":{"fields.datetime":{"from":null,"include_lower":true,"include_upper":false,"to":"2020-01-01T00:00:00Z"}}}]}}}}`, []int64{3}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"22d11697-edba-4186-b084-793e3b876379"}}},{"match_phrase":{"fields.state":{"query":"washington"}}}]}}}}`, []int64{5}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"22d11697-edba-4186-b084-793e3b876379"}}},{"match":{"fields.state_keyword":{"query":"  washington"}}}]}}}}`, []int64{5}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"22d11697-edba-4186-b084-793e3b876379"}}},{"match":{"fields.state_keyword":{"query":"usa"}}}]}}}}`, []int64{}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"22d11697-edba-4186-b084-793e3b876379"}}},{"match_phrase":{"fields.state":{"query":"usa"}}}]}}}}`, []int64{}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"fcab2439-861c-4832-aa54-0c97f38f24ab"}}},{"match_phrase":{"fields.district":{"query":"king"}}}]}}}}`, []int64{7, 9}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"fcab2439-861c-4832-aa54-0c97f38f24ab"}}},{"match_phrase":{"fields.district":{"query":"King-Côunty"}}}]}}}}`, []int64{7}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"fcab2439-861c-4832-aa54-0c97f38f24ab"}}},{"match":{"fields.district_keyword":{"query":"King-Côunty"}}}]}}}}`, []int64{7}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"a551ade4-e5a0-4d83-b185-53b515ad2f2a"}}},{"match_phrase":{"fields.ward":{"query":"district"}}}]}}}}`, []int64{8}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"a551ade4-e5a0-4d83-b185-53b515ad2f2a"}}},{"match":{"fields.ward_keyword":{"query":"central district"}}}]}}}}`, []int64{8}},
	{`{"nested":{"path":"fields","query":{"bool":{"must":[{"match":{"fields.field":{"query":"a551ade4-e5a0-4d83-b185-53b515ad2f2a"}}},{"match":{"fields.ward_keyword":{"query":"district"}}}]}}}}`, []int64{}},
	{`{"match":{"group_ids":{"query":1}}}`, []int64{1}},
	{`{"match":{"group_ids":{"query":4}}}`, []int64{1, 2}},
	{`{"match":{"group_ids":{"query":2}}}`, []int64{}},
}

func TestContacts(t *testing.T) {
	cfg, db := setup(t)

	ix1 := indexers.NewContactIndexer(cfg.ElasticURL, aliasName, 2, 1, 4)
	assert.Equal(t, "indexer_test", ix1.Name())

	dbModified, err := ix1.GetDBLastModified(context.Background(), db)
	assert.NoError(t, err)
	assert.WithinDuration(t, time.Date(2017, 11, 10, 21, 11, 59, 890662000, time.UTC), dbModified, 0)

	// error trying to get ES last modified on before index exists
	_, err = ix1.GetESLastModified(aliasName)
	assert.Error(t, err)

	expectedIndexName := fmt.Sprintf("indexer_test_%s", time.Now().Format("2006_01_02"))

	indexName, err := ix1.Index(db, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName, indexName)

	time.Sleep(1 * time.Second)

	esModified, err := ix1.GetESLastModified(aliasName)
	assert.NoError(t, err)
	assert.WithinDuration(t, time.Date(2017, 11, 10, 21, 11, 59, 890662000, time.UTC), esModified, 0)

	assertIndexerStats(t, ix1, 9, 0)
	assertIndexesWithPrefix(t, cfg, aliasName, []string{expectedIndexName})

	for _, tc := range contactQueryTests {
		assertQuery(t, cfg, []byte(tc.query), tc.expected, "query mismatch for %s", tc.query)
	}

	lastModified, err := ix1.GetESLastModified(indexName)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2017, 11, 10, 21, 11, 59, 890662000, time.UTC), lastModified.In(time.UTC))

	// now make some contact changes, removing one contact, updating another
	_, err = db.Exec(`
	DELETE FROM contacts_contactgroup_contacts WHERE id = 3;
	UPDATE contacts_contact SET name = 'John Deer', modified_on = '2020-08-20 14:00:00+00' where id = 2;
	UPDATE contacts_contact SET is_active = FALSE, modified_on = '2020-08-22 15:00:00+00' where id = 4;`)
	require.NoError(t, err)

	// and index again...
	indexName, err = ix1.Index(db, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName, indexName) // same index used
	assertIndexerStats(t, ix1, 10, 1)

	time.Sleep(1 * time.Second)

	assertIndexesWithPrefix(t, cfg, aliasName, []string{expectedIndexName})

	// should only match new john, old john is gone
	assertQuery(t, cfg, []byte(`{"match": {"name": {"query": "john"}}}`), []int64{2})

	// 3 is no longer in our group
	assertQuery(t, cfg, []byte(`{"match": {"group_ids": {"query": 4}}}`), []int64{1})

	// change John's name to Eric..
	_, err = db.Exec(`
	UPDATE contacts_contact SET name = 'Eric', modified_on = '2020-08-20 14:00:00+00' where id = 2;`)
	require.NoError(t, err)

	// and simulate another indexer doing a parallel rebuild
	ix2 := indexers.NewContactIndexer(cfg.ElasticURL, aliasName, 2, 1, 4)

	indexName2, err := ix2.Index(db, true, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_1", indexName2) // new index used
	assertIndexerStats(t, ix2, 8, 0)

	time.Sleep(1 * time.Second)

	// check we have a new index but the old index is still around
	assertIndexesWithPrefix(t, cfg, aliasName, []string{expectedIndexName, expectedIndexName + "_1"})

	// and the alias points to the new index
	assertQuery(t, cfg, []byte(`{"match": {"name": {"query": "eric"}}}`), []int64{2})

	// simulate another indexer doing a parallel rebuild with cleanup
	ix3 := indexers.NewContactIndexer(cfg.ElasticURL, aliasName, 2, 1, 4)
	indexName3, err := ix3.Index(db, true, true)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_2", indexName3) // new index used
	assertIndexerStats(t, ix3, 8, 0)

	// check we cleaned up indexes besides the new one
	assertIndexesWithPrefix(t, cfg, aliasName, []string{expectedIndexName + "_2"})

	// check that the original indexer now indexes against the new index
	indexName, err = ix1.Index(db, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_2", indexName)
}
