package indexers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/nyaruka/gocommon/elastic"
	"github.com/nyaruka/rp-indexer/v9/indexers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var contactQueryTests = []struct {
	query    elastic.Query
	expected []int64
}{
	{elastic.Match("org_id", 1), []int64{1, 2, 3, 4}},
	{elastic.Match("name", "JOHn"), []int64{4}},
	{elastic.Term("name.keyword", "JOHN DOE"), []int64{4}},
	{elastic.All(elastic.Match("name", "john"), elastic.Match("name", "doe")), []int64{4}}, // can search on both first and last name
	{elastic.Match("name", "Ajodinabiff"), []int64{5}},
	{elastic.Match("language", "eng"), []int64{1}},
	{elastic.Match("status", "B"), []int64{3}},
	{elastic.Match("status", "S"), []int64{2}},
	{elastic.Match("tickets", 2), []int64{1}},
	{elastic.Match("tickets", 1), []int64{2, 3}},
	{elastic.GreaterThan("tickets", 0), []int64{1, 2, 3}},
	{elastic.Match("flow_id", 1), []int64{2, 3}},
	{elastic.Match("flow_id", 2), []int64{4}},
	{elastic.Match("flow_history_ids", 1), []int64{1, 2, 3}},
	{elastic.Match("flow_history_ids", 2), []int64{1, 2}},
	{elastic.GreaterThan("created_on", "2017-01-01"), []int64{1, 6, 8}},
	{elastic.LessThan("last_seen_on", "2019-01-01"), []int64{3, 4}},
	{elastic.Exists("last_seen_on"), []int64{1, 2, 3, 4, 5, 6}},
	{elastic.Not(elastic.Exists("last_seen_on")), []int64{7, 8, 9}},
	{
		elastic.Nested("urns", elastic.All(elastic.Match("urns.scheme", "facebook"), elastic.Match("urns.path.keyword", "1000001"))), []int64{8},
	},
	{ // urn substring
		elastic.Nested("urns", elastic.All(elastic.Match("urns.scheme", "tel"), elastic.MatchPhrase("urns.path", "779"))), []int64{1, 2, 3, 6},
	},
	{ // urn substring with more characters (77911)
		elastic.Nested("urns", elastic.All(elastic.Match("urns.scheme", "tel"), elastic.MatchPhrase("urns.path", "77911"))), []int64{1},
	},
	{ // urn substring with more characters (600055)
		elastic.Nested("urns", elastic.All(elastic.Match("urns.scheme", "tel"), elastic.MatchPhrase("urns.path", "600055"))), []int64{5},
	},
	{ // match a contact with multiple tel urns
		elastic.Nested("urns", elastic.All(elastic.Match("urns.scheme", "tel"), elastic.MatchPhrase("urns.path", "222"))), []int64{1},
	},
	{ // text field
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
			elastic.Match("fields.text", "the rock"),
		)),
		[]int64{1},
	},
	{ // people with no nickname
		elastic.Not(
			elastic.Nested("fields", elastic.All(
				elastic.Match("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
				elastic.Exists("fields.text"),
			)),
		),
		[]int64{2, 3, 4, 5, 6, 7, 8, 9},
	},
	{ // no tokenizing of field text
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
			elastic.Match("fields.text", "rock"),
		)),
		[]int64{},
	},
	{ // number field range
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "05bca1cd-e322-4837-9595-86d0d85e5adb"),
			elastic.GreaterThan("fields.number", 10),
		)),
		[]int64{2},
	},
	{ // datetime field range
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "e0eac267-463a-4c00-9732-cab62df07b16"),
			elastic.LessThan("fields.datetime", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		)),
		[]int64{3},
	},
	{ // state field
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.MatchPhrase("fields.state", "washington"),
		)),
		[]int64{5},
	},
	{
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.Match("fields.state_keyword", "  washington"),
		)),
		[]int64{5},
	},
	{ // doesn't include country
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.Match("fields.state_keyword", "usa"),
		)),
		[]int64{},
	},
	{
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.MatchPhrase("fields.state", "usa"),
		)),
		[]int64{},
	},
	{ // district field
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.MatchPhrase("fields.district", "king"),
		)),
		[]int64{7, 9},
	},
	{ // phrase matches all
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.MatchPhrase("fields.district", "King-Côunty"),
		)),
		[]int64{7},
	},
	{
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.Match("fields.district_keyword", "King-Côunty"),
		)),
		[]int64{7},
	},
	{ // ward field
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.MatchPhrase("fields.ward", "district"),
		)),
		[]int64{8},
	},
	{
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.Match("fields.ward_keyword", "central district"),
		)),
		[]int64{8},
	},
	{ // no substring though on keyword
		elastic.Nested("fields", elastic.All(
			elastic.Match("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.Match("fields.ward_keyword", "district"),
		)),
		[]int64{},
	},
	{elastic.Match("group_ids", 1), []int64{1}},
	{elastic.Match("group_ids", 4), []int64{1, 2}},
	{elastic.Match("group_ids", 2), []int64{}},
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
		assertQuery(t, cfg, tc.query, tc.expected, "query mismatch for %s", tc.query)
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
	assertQuery(t, cfg, elastic.Match("name", "john"), []int64{2})

	// 3 is no longer in our group
	assertQuery(t, cfg, elastic.Match("group_ids", 4), []int64{1})

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
	assertQuery(t, cfg, elastic.Match("name", "eric"), []int64{2})

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
