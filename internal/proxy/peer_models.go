package proxy

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
		if entry.receipt == nil {
			continue
		}
		age := now.Sub(entry.receipt.ReceivedAt)
		if age < 0 || age > peerRetention {
			continue
		}
		localIDCounts := make(map[string]int, len(entry.receipt.Report.Observations))
		for _, observation := range entry.receipt.Report.Observations {
			if observation.State == "supported" {
				localIDCounts[observation.Identity.LocalID]++
			}
		}
		status := "stale"
		if entry.lastError == "" && age <= peerFreshAge {
			status = "fresh"
		}
		for _, obs := range entry.receipt.Report.Observations {
			if obs.State != "supported" {
				continue
			}
			publicID := peerPublicID(obs.Identity)
			if entry.routeInference && localIDCounts[obs.Identity.LocalID] == 1 {
				publicID = peerRouteID(obs.Identity)
			}
			models = append(models, peerCatalogModel{
				ID: publicID, Object: "model", OwnedBy: obs.Identity.OwnerNodeID,
				CatalogProxy: peerCatalogProvenance{
					Identity: obs.Identity, ObservedAt: obs.ObservedAt, Freshness: obs.Freshness,
					Coverage: obs.Coverage, ReceiptStatus: status,
				},
			})
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models
}

func (c *peerCache) modelForRoute(nodeID, localID string) (nodecontract.ModelIdentity, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[nodeID]
	if !ok || entry.receipt == nil || !entry.routeInference {
		return nodecontract.ModelIdentity{}, false
	}
	var identity nodecontract.ModelIdentity
	for _, observation := range entry.receipt.Report.Observations {
		if observation.State != "supported" || observation.Identity.OwnerNodeID != nodeID || observation.Identity.LocalID != localID {
			continue
		}
		if identity.OwnerNodeID != "" {
			return nodecontract.ModelIdentity{}, false
		}
		identity = observation.Identity
	}
	return identity, identity.OwnerNodeID != ""
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
	var root map[string]json.RawMessage
	if err := json.Unmarshal(response.Body, &root); err != nil {
		return catalog.Response{}, err
	}
	var local []json.RawMessage
	if err := json.Unmarshal(root["data"], &local); err != nil {
		return catalog.Response{}, err
	}
	peerModels := h.peerCache.catalogModels(time.Now())
	peerIDs := make(map[string]struct{}, len(peerModels))
	for _, model := range peerModels {
		peerIDs[model.ID] = struct{}{}
	}
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
	peerPrefixes := make([]string, 0, len(reservedRoutes))
	for id := range reservedRoutes {
		peerPrefixes = append(peerPrefixes, id+"/")
	}
	combined := make([]json.RawMessage, 0, len(local))
	for _, item := range local {
		var model struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(item, &model); err != nil {
			return catalog.Response{}, err
		}
		if strings.HasPrefix(model.ID, peerModelPrefix) {
			h.logger.Warn("local catalog model conflicts with reserved peer namespace")
			continue
		}
		ownedByPeerRoute := false
		for _, prefix := range peerPrefixes {
			if strings.HasPrefix(model.ID, prefix) {
				ownedByPeerRoute = true
				break
			}
		}
		if ownedByPeerRoute {
			continue
		}
		if _, duplicate := peerIDs[model.ID]; duplicate {
			// When inference is explicitly routed through the peer, prefer the owner-verified model
			// identity over the legacy static backend entry with the same public prefix.
			continue
		}
		combined = append(combined, item)
	}
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
