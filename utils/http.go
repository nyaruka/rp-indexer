package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nyaruka/gocommon/httpx"
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
	// no response is a connection timeout which we can retry
	if response == nil {
		return true
	}

	// 429 Too Many Requests is recoverable. Sometimes the server puts
	// a Retry-After response header to indicate when the server is
	// available to start processing request from client.
	if response.StatusCode == http.StatusTooManyRequests {
		return true
	}

	// check for unexpected EOF
	bodyBytes, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		slog.Error("error reading ES response, retrying", "error", err)
		return true
	}

	response.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	return false
}

// MakeJSONRequest is a utility function to make a JSON request, optionally decoding the response into the passed in struct
func MakeJSONRequest(ctx context.Context, method string, url string, body []byte, dest any) (*http.Response, error) {
	l := slog.With("url", url, "method", method)

	req, _ := httpx.NewRequest(ctx, method, url, bytes.NewReader(body), map[string]string{"Content-Type": "application/json"})
	resp, err := httpx.Do(http.DefaultClient, req, retryConfig, nil)
	if err != nil {
		l.Error("error making request", "error", err)
		return resp, err
	}
	defer resp.Body.Close()

	// if we have a body, try to decode it
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		l.Error("error reading response", "error", err)
		return resp, err
	}

	l = l.With("response", string(respBody), "status", resp.StatusCode)

	// error if we got a non-200
	if resp.StatusCode != http.StatusOK {
		l.Error("error reaching ES", "error", err)
		return resp, fmt.Errorf("received non-200 response %d: %s", resp.StatusCode, respBody)
	}

	if dest != nil {
		err = json.Unmarshal(respBody, dest)
		if err != nil {
			l.Error("error unmarshalling response", "error", err)
			return resp, err
		}
	}

	return resp, nil
}
