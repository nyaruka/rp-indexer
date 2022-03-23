package indexer

import (
	_ "embed"
	"fmt"
	"net/http"
	"time"
)

// our response for finding the last modified document
type queryResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source struct {
				ID         int64     `json:"id"`
				ModifiedOn time.Time `json:"modified_on"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// GetLastModified queries an index and finds the last modified document, returning its modified time
func GetLastModified(url string, index string) (time.Time, error) {
	lastModified := time.Time{}
	if index == "" {
		return lastModified, fmt.Errorf("empty index passed to GetLastModified")
	}

	// get the newest document on our index
	queryResponse := queryResponse{}
	_, err := MakeJSONRequest(http.MethodPost, fmt.Sprintf("%s/%s/_search", url, index), []byte(`{ "sort": [{ "modified_on_mu": "desc" }]}`), &queryResponse)
	if err != nil {
		return lastModified, err
	}

	if len(queryResponse.Hits.Hits) > 0 {
		lastModified = queryResponse.Hits.Hits[0].Source.ModifiedOn
	}
	return lastModified, nil
}
