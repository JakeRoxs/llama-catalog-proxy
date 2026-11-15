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
		status := "stale"
		if entry.lastError == "" && age <= peerFreshAge {
			status = "fresh"
		}
		for _, obs := range entry.receipt.Report.Observations {
			if obs.State != "supported" {
				continue
			}
			models = append(models, peerCatalogModel{
				ID: peerPublicID(obs.Identity), Object: "model", OwnedBy: obs.Identity.OwnerNodeID,
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
		if age >= 0 && age <= peerFreshAge {
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
		combined = append(combined, item)
	}
	for _, model := range h.peerCache.catalogModels(time.Now()) {
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
