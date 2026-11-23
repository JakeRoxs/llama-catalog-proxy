// Package correlation provides bounded, normalized collection of backend
// activity and lifecycle snapshots from llama-swap backends.
package correlation

import "net/http"

// backendStatusLabels holds the state strings produced by classifyBackendStatus
// for one collector. Each collector supplies its own labels so the shared
// switch has no knowledge of the caller's naming; only the four terminal
// status strings vary. The reasons are shared across collectors.
type backendStatusLabels struct {
	unauthorized string
	unsupported  string
	unknown      string
	unavailable  string
}

// classifyBackendStatus is the single implementation of HTTP status
// classification shared by ActivityCollector and LifecycleCollector. It maps
// an HTTP status to a backend state/reason and whether collection should
// stop. A 200 continues; every other status stops. The caller supplies the
// exact state strings for its four terminal states; the reasons are shared.
//
// Preserved mappings:
//   - 401/403 -> labels.unauthorized / "authorization_required" / stop
//   - 404/405 -> labels.unsupported / "not_available" / stop
//   - 300-399 -> labels.unknown / "redirect_refused" / stop (refused, never followed)
//   - other non-200 -> labels.unavailable / "http_error" / stop
//   - 200 -> "" / "" / continue
func classifyBackendStatus(code int, labels backendStatusLabels) (state, reason string, stop bool) {
	switch {
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return labels.unauthorized, "authorization_required", true
	case code == http.StatusNotFound || code == http.StatusMethodNotAllowed:
		return labels.unsupported, "not_available", true
	case code >= 300 && code < 400:
		return labels.unknown, "redirect_refused", true
	case code != http.StatusOK:
		return labels.unavailable, "http_error", true
	}
	return "", "", false
}
