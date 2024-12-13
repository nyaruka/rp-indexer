package indexers_test

import (
	"bytes"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/nyaruka/gocommon/elastic"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/rp-indexer/v9/indexers"
	"github.com/nyaruka/rp-indexer/v9/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) *runtime.Runtime {
	cfg := runtime.NewDefaultConfig()
	cfg.DB = "postgres://indexer_test:temba@localhost:5432/indexer_test?sslmode=disable"
	cfg.ContactsIndex = "indexer_test"

	testDB, err := os.ReadFile("../testdb.sql")
	require.NoError(t, err)

	db, err := sql.Open("postgres", cfg.DB)
	require.NoError(t, err)

	_, err = db.Exec(string(testDB))
	require.NoError(t, err)

	// delete all indexes with our alias prefix
	existing := elasticRequest(t, cfg, http.MethodGet, "/_aliases", nil)

	for name := range existing {
		if strings.HasPrefix(name, cfg.ContactsIndex) {
			elasticRequest(t, cfg, http.MethodDelete, "/"+name, nil)
		}
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	return &runtime.Runtime{Config: cfg, DB: db}
}

func assertQuery(t *testing.T, cfg *runtime.Config, query elastic.Query, expected []int64, msgAndArgs ...interface{}) {
	results := elasticRequest(t, cfg, http.MethodPost, "/"+cfg.ContactsIndex+"/_search",
		map[string]any{"query": query, "sort": []map[string]any{{"id": "asc"}}},
	)
	hits := results["hits"].(map[string]any)["hits"].([]any)

	actual := make([]int64, len(hits))
	for h, hit := range hits {
		idStr := hit.(map[string]any)["_id"].(string)
		asInt, _ := strconv.Atoi(idStr)
		actual[h] = int64(asInt)
	}

	assert.Equal(t, expected, actual, msgAndArgs...)
}

func assertIndexesWithPrefix(t *testing.T, cfg *runtime.Config, prefix string, expected []string) {
	all := elasticRequest(t, cfg, http.MethodGet, "/_aliases", nil)

	actual := []string{}
	for name := range all {
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

func elasticRequest(t *testing.T, cfg *runtime.Config, method, path string, data map[string]any) map[string]any {
	var body io.Reader
	if data != nil {
		body = bytes.NewReader(jsonx.MustMarshal(data))
	}
	req, err := httpx.NewRequest(method, cfg.ElasticURL+path, body, map[string]string{"Content-Type": "application/json"})
	require.NoError(t, err)

	trace, err := httpx.DoTrace(http.DefaultClient, req, nil, nil, -1)
	require.NoError(t, err)

	resp, err := jsonx.DecodeGeneric(trace.ResponseBody)
	require.NoError(t, err)

	return resp.(map[string]any)
}
