package utils_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nyaruka/rp-indexer/utils"
	"github.com/stretchr/testify/assert"
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
			w.Write([]byte(`{"foo": 1}`))
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		responses[responseCounter](w, r)
		responseCounter++
	}))
	defer ts.Close()

	resp, err := utils.MakeJSONRequest("GET", ts.URL, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	require.Equal(t, responseCounter, 4)
}
