package proxy

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/nodecontract"
)

const peerModelPrefix = "@peer/"

func peerPublicID(id nodecontract.ModelIdentity) string {
	return peerModelPrefix + id.OwnerNodeID + "/" +
		base64.RawURLEncoding.EncodeToString([]byte(id.BackendID)) + "/" +
		base64.RawURLEncoding.EncodeToString([]byte(id.LocalID))
}

func peerRouteID(id nodecontract.ModelIdentity) string {
	return id.OwnerNodeID + "/" + id.LocalID
}

func parsePeerPublicID(publicID string) (nodecontract.ModelIdentity, error) {
	value, ok := strings.CutPrefix(publicID, peerModelPrefix)
	if !ok {
		return nodecontract.ModelIdentity{}, errors.New("not a peer model ID")
	}
	parts := strings.Split(value, "/")
	if len(parts) != 3 || !identity.ValidNodeID(parts[0]) {
		return nodecontract.ModelIdentity{}, errors.New("invalid peer model ID")
	}
	decode := func(s string) (string, error) {
		if len(s) == 0 || len(s) > 342 {
			return "", errors.New("invalid peer identity component")
		}
		data, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil || base64.RawURLEncoding.EncodeToString(data) != s || !identity.ValidModelComponent(string(data)) {
			return "", errors.New("invalid peer identity component")
		}
		return string(data), nil
	}
	backend, err := decode(parts[1])
	if err != nil {
		return nodecontract.ModelIdentity{}, err
	}
	model, err := decode(parts[2])
	if err != nil {
		return nodecontract.ModelIdentity{}, err
	}
	return nodecontract.ModelIdentity{OwnerNodeID: parts[0], BackendID: backend, LocalID: model}, nil
}

type peerCatalogModel struct {
	ID           string                `json:"id"`
	Object       string                `json:"object"`
	OwnedBy      string                `json:"owned_by"`
	Meta         map[string]any        `json:"meta,omitempty"`
	CatalogProxy peerCatalogProvenance `json:"catalog_proxy"`
}

type peerCatalogProvenance struct {
	Identity      nodecontract.ModelIdentity `json:"identity"`
	ObservedAt    *time.Time                 `json:"observed_at,omitempty"`
	Freshness     string                     `json:"freshness"`
	Coverage      string                     `json:"coverage"`
	ReceiptStatus string                     `json:"receipt_status"`
}

func (c *peerCache) catalogModels(now time.Time) []peerCatalogModel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var models []peerCatalogModel
	for _, entry := range c.entries {
		models = append(models, catalogModelsForEntry(now, entry)...)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models
}

// catalogModelsForEntry builds the supported-observation catalog models for one
// peer cache entry. It is pure/read-only and must be invoked while
// peerCache.mu is held.
func catalogModelsForEntry(now time.Time, entry peerCacheEntry) []peerCatalogModel {
	if entry.receipt == nil {
		return nil
	}
	age := now.Sub(entry.receipt.ReceivedAt)
	if age < 0 || age > peerRetention {
		return nil
	}
	localIDCounts := localSupportedIDCounts(entry.receipt.Report.Observations)
	status := entryReceiptStatus(entry.lastError, age)
	models := make([]peerCatalogModel, 0, len(entry.receipt.Report.Observations))
	for _, obs := range entry.receipt.Report.Observations {
		if obs.State != "supported" {
			continue
		}
		models = append(models, peerCatalogModelFromObservation(obs, entry.routeInference, localIDCounts, status))
	}
	return models
}

// localSupportedIDCounts counts supported observations keyed by their LocalID.
func localSupportedIDCounts(observations []nodecontract.Observation) map[string]int {
	counts := make(map[string]int, len(observations))
	for _, observation := range observations {
		if observation.State == "supported" {
			counts[observation.Identity.LocalID]++
		}
	}
	return counts
}

// entryReceiptStatus reports "fresh" only when the receipt has no error and is
// within the freshness window; otherwise "stale".
func entryReceiptStatus(lastError string, age time.Duration) string {
	status := "stale"
	if lastError == "" && age <= peerFreshAge {
		status = "fresh"
	}
	return status
}

// peerCatalogModelFromObservation converts one supported observation into a
// catalog model. routeInference selects the peer route ID only when the local
// ID occurs exactly once among supported observations.
func peerCatalogModelFromObservation(obs nodecontract.Observation, routeInference bool, localIDCounts map[string]int, status string) peerCatalogModel {
	publicID := peerPublicID(obs.Identity)
	if routeInference && localIDCounts[obs.Identity.LocalID] == 1 {
		publicID = peerRouteID(obs.Identity)
	}
	model := peerCatalogModel{
		ID: publicID, Object: "model", OwnedBy: obs.Identity.OwnerNodeID,
		CatalogProxy: peerCatalogProvenance{
			Identity: obs.Identity, ObservedAt: obs.ObservedAt, Freshness: obs.Freshness,
			Coverage: obs.Coverage, ReceiptStatus: status,
		},
	}
	if len(obs.Aliases) > 0 {
		model.Meta = map[string]any{"llamaswap": map[string]any{"aliases": obs.Aliases}}
	}
	return model
}

func (c *peerCache) modelForRoute(nodeID, backendID, modelID string) (nodecontract.ModelIdentity, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[nodeID]
	if !ok || entry.receipt == nil || !entry.routeInference {
		return nodecontract.ModelIdentity{}, false
	}
	var identity nodecontract.ModelIdentity
	for _, observation := range entry.receipt.Report.Observations {
		if observation.State != "supported" || observation.Identity.OwnerNodeID != nodeID ||
			(backendID != "" && observation.Identity.BackendID != backendID) ||
			(observation.Identity.LocalID != modelID && !modelHasAlias(observation.Aliases, modelID)) {
			continue
		}
		if identity.OwnerNodeID != "" {
			return nodecontract.ModelIdentity{}, false
		}
		identity = observation.Identity
	}
	return identity, identity.OwnerNodeID != ""
}

func modelHasAlias(aliases []string, modelID string) bool {
	for _, alias := range aliases {
		if alias == modelID {
			return true
		}
	}
	return false
}

func (c *peerCache) freshModel(id nodecontract.ModelIdentity, now time.Time) (nodecontract.Source, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[id.OwnerNodeID]
	if !ok || entry.receipt == nil || entry.lastError != "" {
		return nodecontract.Source{}, false
	}
	receipt := entry.receipt
	receiptAge, generationAge := now.Sub(receipt.ReceivedAt), now.Sub(receipt.Report.GeneratedAt)
	if receiptAge < 0 || receiptAge > peerFreshAge || generationAge < 0 || generationAge > peerFreshAge ||
		receipt.Report.Capabilities["catalog_read"] != "enabled" {
		return nodecontract.Source{}, false
	}
	for _, observation := range receipt.Report.Observations {
		if observation.Identity != id || observation.State != "supported" ||
			observation.Freshness != "fresh" || observation.ObservedAt == nil {
			continue
		}
		age := now.Sub(*observation.ObservedAt)
		if age >= 0 && observation.ExpiresAt != nil && now.Before(*observation.ExpiresAt) {
			return receipt.Report.Source, true
		}
	}
	return nodecontract.Source{}, false
}

func (h *Handler) mergePeerCatalog(response catalog.Response) (catalog.Response, error) {
	root, local, err := decodeCatalogResponse(response.Body)
	if err != nil {
		return catalog.Response{}, err
	}
	peerModels := h.peerCache.catalogModels(time.Now())
	peerIDs := peerModelIDs(peerModels)
	reservedPrefixes := reservedPeerPrefixes(h)
	combined, err := filterLocalRows(local, peerIDs, reservedPrefixes, h.logger)
	if err != nil {
		return catalog.Response{}, err
	}
	return marshalPeerCatalog(root, combined, peerModels, response)
}

// decodeCatalogResponse parses the catalog response body into its root object
// and the local data array.
func decodeCatalogResponse(body []byte) (map[string]json.RawMessage, []json.RawMessage, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, nil, err
	}
	var local []json.RawMessage
	if err := json.Unmarshal(root["data"], &local); err != nil {
		return nil, nil, err
	}
	return root, local, nil
}

// peerModelIDs returns the set of public IDs exposed by the peer catalog models.
func peerModelIDs(peerModels []peerCatalogModel) map[string]struct{} {
	ids := make(map[string]struct{}, len(peerModels))
	for _, model := range peerModels {
		ids[model.ID] = struct{}{}
	}
	return ids
}

// reservedPeerPrefixes returns the route-inference peer namespace prefixes that
// must be reserved and excluded from the local catalog rows.
func reservedPeerPrefixes(h *Handler) []string {
	reservedRoutes := make(map[string]struct{}, len(h.peerRouteAliases)+len(h.peerClients))
	for id := range h.peerRouteAliases {
		client := h.peerClients[id]
		if client == nil || client.routeInference {
			reservedRoutes[id] = struct{}{}
		}
	}
	for id, client := range h.peerClients {
		if client.routeInference {
			reservedRoutes[id] = struct{}{}
		}
	}
	prefixes := make([]string, 0, len(reservedRoutes))
	for id := range reservedRoutes {
		prefixes = append(prefixes, id+"/")
	}
	return prefixes
}

// hasPeerRoutePrefix reports whether id begins with any of the reserved peer
// route prefixes.
func hasPeerRoutePrefix(id string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

// filterLocalRows keeps local catalog rows that do not conflict with the
// reserved peer namespace, are not already represented by a peer model, and
// logs a warning for reserved-namespace conflicts.
func filterLocalRows(local []json.RawMessage, peerIDs map[string]struct{}, reservedPrefixes []string, logger *slog.Logger) ([]json.RawMessage, error) {
	combined := make([]json.RawMessage, 0, len(local))
	for _, item := range local {
		var model struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(item, &model); err != nil {
			return nil, err
		}
		if strings.HasPrefix(model.ID, peerModelPrefix) {
			logger.Warn("local catalog model conflicts with reserved peer namespace")
			continue
		}
		if hasPeerRoutePrefix(model.ID, reservedPrefixes) {
			continue
		}
		if _, duplicate := peerIDs[model.ID]; duplicate {
			continue
		}
		combined = append(combined, item)
	}
	return combined, nil
}

// marshalPeerCatalog appends the peer models to the combined rows, marshals the
// full root preserving all other fields, and stores the result on the response.
func marshalPeerCatalog(root map[string]json.RawMessage, combined []json.RawMessage, peerModels []peerCatalogModel, response catalog.Response) (catalog.Response, error) {
	for _, model := range peerModels {
		data, err := json.Marshal(model)
		if err != nil {
			return catalog.Response{}, err
		}
		combined = append(combined, data)
	}
	data, err := json.Marshal(combined)
	if err != nil {
		return catalog.Response{}, err
	}
	root["data"] = data
	body, err := json.Marshal(root)
	if err != nil {
		return catalog.Response{}, err
	}
	response.Body = body
	return response, nil
}
