package indexers_test

import (
	"context"
	"database/sql"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/nyaruka/rp-indexer/indexers"
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

func assertIndexerStats(t *testing.T, ix indexers.Indexer, expectedIndexed, expectedDeleted int64) {
	actual := ix.Stats()
	assert.Equal(t, expectedIndexed, actual.Indexed, "indexed mismatch")
	assert.Equal(t, expectedDeleted, actual.Deleted, "deleted mismatch")
}
