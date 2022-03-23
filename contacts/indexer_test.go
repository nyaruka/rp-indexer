package contacts_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/nyaruka/gocommon/jsonx"
	indexer "github.com/nyaruka/rp-indexer"
	"github.com/nyaruka/rp-indexer/contacts"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const elasticURL = "http://localhost:9200"
const aliasName = "indexer_test"

func setup(t *testing.T) (*sql.DB, *elastic.Client) {
	testDB, err := ioutil.ReadFile("../testdb.sql")
	require.NoError(t, err)

	db, err := sql.Open("postgres", "postgres://nyaruka:nyaruka@localhost:5432/elastic_test?sslmode=disable")
	require.NoError(t, err)

	_, err = db.Exec(string(testDB))
	require.NoError(t, err)

	es, err := elastic.NewClient(elastic.SetURL(elasticURL), elastic.SetTraceLog(log.New(os.Stdout, "", log.LstdFlags)), elastic.SetSniff(false))
	require.NoError(t, err)

	// delete all indexes with our alias prefix
	existing, err := es.IndexNames()
	require.NoError(t, err)

	for _, name := range existing {
		if strings.HasPrefix(name, aliasName) {
			_, err = es.DeleteIndex(name).Do(context.Background())
			require.NoError(t, err)
		}
	}

	logrus.SetLevel(logrus.DebugLevel)

	return db, es
}

func assertQuery(t *testing.T, client *elastic.Client, query elastic.Query, expected []int64, msgAndArgs ...interface{}) {
	results, err := client.Search().Index(aliasName).Query(query).Sort("id", true).Pretty(true).Do(context.Background())
	assert.NoError(t, err)

	actual := make([]int64, len(results.Hits.Hits))
	for h, hit := range results.Hits.Hits {
		asInt, _ := strconv.Atoi(hit.Id)
		actual[h] = int64(asInt)
	}

	assert.Equal(t, expected, actual, msgAndArgs...)
}

func assertIndexesWithPrefix(t *testing.T, es *elastic.Client, prefix string, expected []string) {
	all, err := es.IndexNames()
	require.NoError(t, err)

	actual := []string{}
	for _, name := range all {
		if strings.HasPrefix(name, prefix) {
			actual = append(actual, name)
		}
	}
	sort.Strings(actual)
	assert.Equal(t, expected, actual)
}

func assertIndexerStats(t *testing.T, ix indexer.Indexer, expectedIndexed, expectedDeleted int64) {
	actualIndexed, actualDeleted, _ := ix.Stats()
	assert.Equal(t, expectedIndexed, actualIndexed, "indexed mismatch")
	assert.Equal(t, expectedDeleted, actualDeleted, "deleted mismatch")
}

var queryTests = []struct {
	query    elastic.Query
	expected []int64
}{
	{elastic.NewMatchQuery("org_id", "1"), []int64{1, 2, 3, 4}},
	{elastic.NewMatchQuery("name", "JOHn"), []int64{4}},
	{elastic.NewTermQuery("name.keyword", "JOHN DOE"), []int64{4}},
	{elastic.NewBoolQuery().Must(elastic.NewMatchQuery("name", "john"), elastic.NewMatchQuery("name", "doe")), []int64{4}}, // can search on both first and last name
	{elastic.NewMatchQuery("name", "Ajodinabiff"), []int64{5}},                                                             // long name
	{elastic.NewMatchQuery("language", "eng"), []int64{1}},
	{elastic.NewMatchQuery("status", "B"), []int64{3}},
	{elastic.NewMatchQuery("status", "S"), []int64{2}},
	{elastic.NewMatchQuery("tickets", 2), []int64{1}},
	{elastic.NewMatchQuery("tickets", 1), []int64{2, 3}},
	{elastic.NewRangeQuery("tickets").Gt(0), []int64{1, 2, 3}},
	{elastic.NewMatchQuery("flow", "6d3cf1eb-546e-4fb8-a5ca-69187648fbf6"), []int64{2, 3}},
	{elastic.NewMatchQuery("flow", "4eea8ff1-4fe2-4ce5-92a4-0870a499973a"), []int64{4}},
	{elastic.NewRangeQuery("created_on").Gt("2017-01-01"), []int64{1, 6, 8}},                   // created_on range
	{elastic.NewRangeQuery("last_seen_on").Lt("2019-01-01"), []int64{3, 4}},                    // last_seen_on range
	{elastic.NewExistsQuery("last_seen_on"), []int64{1, 2, 3, 4, 5, 6}},                        // last_seen_on is set
	{elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery("last_seen_on")), []int64{7, 8, 9}}, // last_seen_on is not set
	{
		elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("urns.scheme", "facebook"),
			elastic.NewMatchQuery("urns.path.keyword", "1000001"),
		)),
		[]int64{8},
	},
	{ // urn substring
		elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("urns.scheme", "tel"),
			elastic.NewMatchPhraseQuery("urns.path", "779"),
		)),
		[]int64{1, 2, 3, 6},
	},
	{ // urn substring with more characters (77911)
		elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("urns.scheme", "tel"),
			elastic.NewMatchPhraseQuery("urns.path", "77911"),
		)),
		[]int64{1},
	},
	{ // urn substring with more characters (600055)
		elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("urns.scheme", "tel"),
			elastic.NewMatchPhraseQuery("urns.path", "600055"),
		)),
		[]int64{5},
	},
	{ // match a contact with multiple tel urns
		elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("urns.scheme", "tel"),
			elastic.NewMatchPhraseQuery("urns.path", "222"),
		)),
		[]int64{1},
	},
	{ // text field
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
			elastic.NewMatchQuery("fields.text", "the rock")),
		),
		[]int64{1},
	},
	{ // people with no nickname
		elastic.NewBoolQuery().MustNot(
			elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
				elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
				elastic.NewExistsQuery("fields.text")),
			),
		),
		[]int64{2, 3, 4, 5, 6, 7, 8, 9},
	},
	{ // no tokenizing of field text
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "17103bb1-1b48-4b70-92f7-1f6b73bd3488"),
			elastic.NewMatchQuery("fields.text", "rock"),
		)),
		[]int64{},
	},
	{ // number field range
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "05bca1cd-e322-4837-9595-86d0d85e5adb"),
			elastic.NewRangeQuery("fields.number").Gt(10),
		)),
		[]int64{2},
	},
	{ // datetime field range
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "e0eac267-463a-4c00-9732-cab62df07b16"),
			elastic.NewRangeQuery("fields.datetime").Lt(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		)),
		[]int64{3},
	},
	{ // state field
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.NewMatchPhraseQuery("fields.state", "washington"),
		)),
		[]int64{5},
	},
	{
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.NewMatchQuery("fields.state_keyword", "  washington"),
		)),
		[]int64{5},
	},
	{ // doesn't include country
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.NewMatchQuery("fields.state_keyword", "usa"),
		)),
		[]int64{},
	},
	{
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "22d11697-edba-4186-b084-793e3b876379"),
			elastic.NewMatchPhraseQuery("fields.state", "usa"),
		)),
		[]int64{},
	},
	{ // district field
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.NewMatchPhraseQuery("fields.district", "king"),
		)),
		[]int64{7, 9},
	},
	{ // phrase matches all
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.NewMatchPhraseQuery("fields.district", "King-Côunty"),
		)),
		[]int64{7},
	},
	{
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "fcab2439-861c-4832-aa54-0c97f38f24ab"),
			elastic.NewMatchQuery("fields.district_keyword", "King-Côunty"),
		)),
		[]int64{7},
	},
	{ // ward field
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.NewMatchPhraseQuery("fields.ward", "district"),
		)),
		[]int64{8},
	},
	{
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.NewMatchQuery("fields.ward_keyword", "central district"),
		)),
		[]int64{8},
	},
	{ // no substring though on keyword
		elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("fields.field", "a551ade4-e5a0-4d83-b185-53b515ad2f2a"),
			elastic.NewMatchQuery("fields.ward_keyword", "district"),
		)),
		[]int64{},
	},
	{elastic.NewMatchQuery("groups", "4ea0f313-2f62-4e57-bdf0-232b5191dd57"), []int64{1}},
	{elastic.NewMatchQuery("groups", "529bac39-550a-4d6f-817c-1833f3449007"), []int64{1, 2}},
	{elastic.NewMatchQuery("groups", "4c016340-468d-4675-a974-15cb7a45a5ab"), []int64{}},
}

func TestIndexer(t *testing.T) {
	db, es := setup(t)

	ix1 := contacts.NewIndexer(elasticURL, aliasName, 4)
	assert.Equal(t, "indexer_test", ix1.Name())

	expectedIndexName := fmt.Sprintf("indexer_test_%s", time.Now().Format("2006_01_02"))

	indexName, err := ix1.Index(db, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName, indexName)

	time.Sleep(1 * time.Second)

	assertIndexerStats(t, ix1, 9, 0)
	assertIndexesWithPrefix(t, es, aliasName, []string{expectedIndexName})

	for _, tc := range queryTests {
		src, _ := tc.query.Source()
		assertQuery(t, es, tc.query, tc.expected, "query mismatch for %s", string(jsonx.MustMarshal(src)))
	}

	lastModified, err := ix1.GetLastModified(indexName)
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

	assertIndexesWithPrefix(t, es, aliasName, []string{expectedIndexName})

	// should only match new john, old john is gone
	assertQuery(t, es, elastic.NewMatchQuery("name", "john"), []int64{2})

	// 3 is no longer in our group
	assertQuery(t, es, elastic.NewMatchQuery("groups", "529bac39-550a-4d6f-817c-1833f3449007"), []int64{1})

	// change John's name to Eric..
	_, err = db.Exec(`
	UPDATE contacts_contact SET name = 'Eric', modified_on = '2020-08-20 14:00:00+00' where id = 2;`)
	require.NoError(t, err)

	// and simulate another indexer doing a parallel rebuild
	ix2 := contacts.NewIndexer(elasticURL, aliasName, 4)

	indexName2, err := ix2.Index(db, true, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_1", indexName2) // new index used
	assertIndexerStats(t, ix2, 8, 0)

	time.Sleep(1 * time.Second)

	// check we have a new index but the old index is still around
	assertIndexesWithPrefix(t, es, aliasName, []string{expectedIndexName, expectedIndexName + "_1"})

	// and the alias points to the new index
	assertQuery(t, es, elastic.NewMatchQuery("name", "eric"), []int64{2})

	// simulate another indexer doing a parallel rebuild with cleanup
	ix3 := contacts.NewIndexer(elasticURL, aliasName, 4)
	indexName3, err := ix3.Index(db, true, true)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_2", indexName3) // new index used
	assertIndexerStats(t, ix3, 8, 0)

	// check we cleaned up indexes besides the new one
	assertIndexesWithPrefix(t, es, aliasName, []string{expectedIndexName + "_2"})

	// check that the original indexer now indexes against the new index
	indexName, err = ix1.Index(db, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedIndexName+"_2", indexName)
}
