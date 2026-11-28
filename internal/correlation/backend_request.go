// Package correlation provides bounded, normalized collection of backend
// metrics, lifecycle, GPU, and alias evidence from llama-swap backends.
package correlation

import (
	"context"
	"net/http"
)

// newBackendJSONRequest creates a GET request with a nil body for the given
// endpoint. It sets the Accept header to application/json, copies each
// configured header only when the request does not already set it, and adds
// Authorization: Bearer <apiKey> plus X-Api-Key: <apiKey> only when the API
// key is nonempty and neither header is already present.
func newBackendJSONRequest(ctx context.Context, endpoint string, configuredHeaders map[string]string, apiKey string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	for key, value := range configuredHeaders {
		if req.Header.Get(key) == "" {
			req.Header.Set(key, value)
		}
	}
	if apiKey != "" && req.Header.Get("Authorization") == "" && req.Header.Get("X-Api-Key") == "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("X-Api-Key", apiKey)
	}
	return req, nil
}
