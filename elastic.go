package indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	log "github.com/sirupsen/logrus"
)

var retryConfig *httpx.RetryConfig

func init() {
	backoffs := make([]time.Duration, 5)
	backoffs[0] = 1 * time.Second
	for i := 1; i < len(backoffs); i++ {
		backoffs[i] = backoffs[i-1] * 2
	}

	retryConfig = &httpx.RetryConfig{Backoffs: backoffs, ShouldRetry: shouldRetry}
}

func shouldRetry(request *http.Request, response *http.Response, withDelay time.Duration) bool {
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

// MakeJSONRequest is a utility function to make a JSON request, optionally decoding the response into the passed in struct
func MakeJSONRequest(method string, url string, body []byte, jsonStruct interface{}) (*http.Response, error) {
	req, _ := http.NewRequest(method, url, bytes.NewReader(body))
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
