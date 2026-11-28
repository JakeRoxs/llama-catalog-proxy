package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBenchmarkConfigurationValidation(t *testing.T) {
	token, diagnostic := strings.Repeat("b", 32), strings.Repeat("d", 32)
	directory := t.TempDir()
	base := fmt.Sprintf(`
default_backend: local
backends: {local: {url: http://localhost:9292}}
diagnostics_api_key: %s
benchmark:
  token: %s
  proxy_origin: http://127.0.0.1:8080
  report_directory: %q
`, diagnostic, token, directory)
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"missing diagnostics": strings.Replace(base, "diagnostics_api_key: "+diagnostic, "", 1),
		"same credential":     strings.Replace(base, "token: "+token, "token: "+diagnostic, 1),
		"short credential":    strings.Replace(base, "token: "+token, "token: short", 1),
		"relative directory":  strings.Replace(base, fmt.Sprintf("report_directory: %q", directory), "report_directory: relative", 1),
		"public origin":       strings.Replace(base, "127.0.0.1", "8.8.8.8", 1),
		"origin path":         strings.Replace(base, ":8080", ":8080/path", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), diagnostic) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestDelegatedStoreConfiguration(t *testing.T) {
	target, local, backups, proposals, operations := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	backupLine := fmt.Sprintf("  backup_store: {directory: %q, token: %q, max_backups: 2}\n", backups, strings.Repeat("b", 32))
	proposalLine := fmt.Sprintf("    proposal_directory: %q\n", proposals)
	operationLine := fmt.Sprintf("    operation_directory: %q\n", operations)
	base := fmt.Sprintf(`default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: %q
  proposal_store: {directory: %q, token: %q}
%s  delegated_store:
%s%s  targets:
    swap: {directory: %q, filename: config.yaml}
`, strings.Repeat("m", 32), local, strings.Repeat("p", 32), backupLine, proposalLine, operationLine, target)
	cfg, err := Load(writeConfig(t, base))
	if err != nil || cfg.Management.DelegatedStore.ProposalDirectory != proposals ||
		cfg.Management.DelegatedStore.OperationDirectory != operations {
		t.Fatalf("valid store configuration rejected: %v", err)
	}
	for name, contents := range map[string]string{
		"missing backup":   strings.Replace(base, backupLine, "", 1),
		"missing proposal": strings.Replace(base, proposalLine, "", 1),
		"relative path":    strings.Replace(base, operationLine, "    operation_directory: relative\n", 1),
		"unclean path":     strings.Replace(base, operationLine, fmt.Sprintf("    operation_directory: %q\n", operations+"/../other"), 1),
		"same stores":      strings.Replace(base, operationLine, fmt.Sprintf("    operation_directory: %q\n", proposals), 1),
		"nested stores":    strings.Replace(base, operationLine, fmt.Sprintf("    operation_directory: %q\n", filepath.Join(proposals, "nested")), 1),
		"target overlap":   strings.Replace(base, operationLine, fmt.Sprintf("    operation_directory: %q\n", target), 1),
		"backup overlap":   strings.Replace(base, proposalLine, fmt.Sprintf("    proposal_directory: %q\n", backups), 1),
		"local overlap":    strings.Replace(base, operationLine, fmt.Sprintf("    operation_directory: %q\n", local), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("invalid delegated store configuration accepted")
			}
		})
	}
	cfg.Benchmark = &BenchmarkConfig{ReportDirectory: operations}
	if err := validateManagement(cfg); err == nil {
		t.Fatal("delegated operation store overlaps benchmark reports")
	}
	cfg.Benchmark = nil
	cfg.Management.Ingestion = &IngestionConfig{
		Directory: t.TempDir(), RecordDirectory: proposals, MaxRecords: 1,
		Token: strings.Repeat("i", 32), MaxBytes: 1, MaxDirectoryBytes: 1,
		MinFreeBytes: 1, Timeout: "1s", FileMode: "0600",
	}
	if err := validateManagement(cfg); err == nil {
		t.Fatal("delegated proposal store overlaps ingestion records")
	}
}

func TestPeerConfigurationValidation(t *testing.T) {
	token := strings.Repeat("a", 32)
	base := `
default_backend: local
backends:
  local: {url: http://localhost:9292}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  local_backends: [local]
  peers:
    node-b: {token: ` + token + `, catalog_read: true}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"self peer":                    strings.Replace(base, "node-b:", "node-a:", 1),
		"missing backend":              strings.Replace(base, "[local]", "[missing]", 1),
		"duplicate local":              strings.Replace(base, "[local]", "[local, local]", 1),
		"short token":                  strings.Replace(base, token, "short", 1),
		"bad listen":                   strings.Replace(base, "127.0.0.1:9293", "bad", 1),
		"missing cert":                 strings.Replace(base, "tls_cert_file: cert.pem", "tls_cert_file: ''", 1),
		"duplicate token":              base + "    node-c: {token: " + token + ", catalog_read: true}\n",
		"diagnostics key":              base + "diagnostics_api_key: " + token + "\n",
		"diagnostics without catalog":  strings.Replace(base, "catalog_read: true", "catalog_read: false, diagnostics_read: true", 1),
		"management route without URL": strings.Replace(base, "catalog_read: true", "catalog_read: true, route_management: true", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), token) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestPeerFederationIdentityRequiresTrustedClientCA(t *testing.T) {
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  federation_id: lab.example
  client_ca_file: /certs/federation-ca.pem
  client_crl_file: /certs/federation.crl.pem
  crl_max_age: 24h
  membership_directory: /var/lib/catalog-proxy/federation
  audit_directory: /var/lib/catalog-proxy/audit
  pairing_join_origins: ["https://peer.example:9293/"]
  peers: {}
`
	cfg, err := Load(writeConfig(t, base))
	if err != nil {
		t.Fatalf("valid federation identity config rejected: %v", err)
	}
	if origins := cfg.Peer.PairingJoinOrigins; len(origins) != 1 || origins[0] != "https://peer.example:9293/" {
		t.Fatalf("pairing join origins = %v", origins)
	}
	cases := map[string]string{
		"missing client CA":               strings.Replace(base, "  client_ca_file: /certs/federation-ca.pem\n", "", 1),
		"missing client CRL":              strings.Replace(base, "  client_crl_file: /certs/federation.crl.pem\n", "", 1),
		"missing membership store":        strings.Replace(base, "  membership_directory: /var/lib/catalog-proxy/federation\n", "", 1),
		"missing audit store":             strings.Replace(base, "  audit_directory: /var/lib/catalog-proxy/audit\n", "", 1),
		"invalid CRL age":                 strings.Replace(base, "  crl_max_age: 24h\n", "  crl_max_age: 90d\n", 1),
		"client CA without federation ID": strings.Replace(base, "  federation_id: lab.example\n", "", 1),
		"invalid federation ID":           strings.Replace(base, "lab.example", "invalid_domain", 1),
		"non-HTTPS join origin":           strings.Replace(base, "https://peer.example:9293/", "http://peer.example:9293/", 1),
		"join URL with path":              strings.Replace(base, "https://peer.example:9293/", "https://peer.example:9293/other", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("invalid federation identity config accepted")
			}
		})
	}
}

func TestPeerDiscoveryRequiresAnExplicitMTLSBoundEndpointAndInterface(t *testing.T) {
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  federation_id: lab.example
  client_ca_file: /certs/federation-ca.pem
  client_crl_file: /certs/federation.crl.pem
  crl_max_age: 24h
  membership_directory: /var/lib/catalog-proxy/federation
  audit_directory: /var/lib/catalog-proxy/audit
  catalog_grant_directory: /var/lib/catalog-proxy/catalog-grants
  discovery:
    enabled: true
    interface: eth0
    advertise_url: https://192.168.1.10:9293
  peers: {}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatalf("valid discovery configuration rejected: %v", err)
	}
	if _, err := Load(writeConfig(t, strings.Replace(base, "192.168.1.10", "node-a.local", 1))); err != nil {
		t.Fatalf("valid .local discovery endpoint rejected: %v", err)
	}
	cases := map[string]string{
		"missing interface":                   strings.Replace(base, "    interface: eth0\n", "", 1),
		"unsafe interface":                    strings.Replace(base, "interface: eth0", "interface: ../eth0", 1),
		"non-HTTPS endpoint":                  strings.Replace(base, "https://192.168.1.10:9293", "http://192.168.1.10:9293", 1),
		"public IP endpoint":                  strings.Replace(base, "192.168.1.10", "203.0.113.10", 1),
		"loopback IP endpoint":                strings.Replace(base, "192.168.1.10", "127.0.0.1", 1),
		"endpoint path":                       strings.Replace(base, "https://192.168.1.10:9293", "https://192.168.1.10:9293/peer", 1),
		"port mismatch":                       strings.Replace(base, "192.168.1.10:9293", "192.168.1.10:443", 1),
		"non-local hostname":                  strings.Replace(base, "192.168.1.10", "proxy.example.com", 1),
		"without federation":                  strings.Replace(base, "  federation_id: lab.example\n", "", 1),
		"missing catalog grant directory":     strings.Replace(base, "  catalog_grant_directory: /var/lib/catalog-proxy/catalog-grants\n", "", 1),
		"overlapping catalog grant directory": strings.Replace(base, "/var/lib/catalog-proxy/catalog-grants", "/var/lib/catalog-proxy/federation/grants", 1),
		"federation ID too long for TXT":      strings.Replace(base, "lab.example", strings.Repeat("a", 63)+"."+strings.Repeat("b", 63)+"."+strings.Repeat("c", 63)+"."+strings.Repeat("d", 50), 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("invalid peer discovery configuration accepted")
			}
		})
	}
}

func TestMTLSPeerGrantRequiresServerCAAndCRL(t *testing.T) {
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  federation_id: lab.example
  client_ca_file: /certs/federation-ca.pem
  client_crl_file: /certs/federation.crl.pem
  membership_directory: /var/lib/catalog-proxy/federation
  audit_directory: /var/lib/catalog-proxy/audit
  peers:
    node-b:
      token: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      catalog_read: true
      url: https://node-b.example:9293
      outbound_token: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      ca_file: /certs/federation-ca.pem
      crl_file: /certs/federation.crl.pem
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatalf("valid mTLS peer grant rejected: %v", err)
	}
	withSettings := strings.Replace(base, "      catalog_read: true\n", "      catalog_read: true\n      manage_settings: true\n", 1)
	if _, err := Load(writeConfig(t, withSettings)); err == nil {
		t.Fatal("mTLS manage_settings grant without protected owner grant store was accepted")
	}
	for name, contents := range map[string]string{
		"missing server CA":  strings.Replace(base, "      ca_file: /certs/federation-ca.pem\n", "", 1),
		"missing server CRL": strings.Replace(base, "      crl_file: /certs/federation.crl.pem\n", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("mTLS outbound grant without CA/CRL was accepted")
			}
		})
	}
}

func TestPeerOutboundConfigurationValidation(t *testing.T) {
	inbound, outbound := strings.Repeat("a", 32), strings.Repeat("b", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  local_backends: [local]
  peers:
    node-b: {token: ` + inbound + `, catalog_read: true, url: "https://peer.example:9293", outbound_token: ` + outbound + `}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	managed := strings.Replace(base, "outbound_token: "+outbound, "outbound_token: "+outbound+", route_management: true", 1)
	cfg, err := Load(writeConfig(t, managed))
	if err != nil || !cfg.Peer.Peers["node-b"].RouteManagement || cfg.Peer.Peers["node-b"].RouteInference {
		t.Fatalf("independent outbound management scope rejected: %v", err)
	}
	cases := map[string]string{
		"plaintext":   strings.Replace(base, "https://peer.example:9293", "http://peer.example:9293", 1),
		"userinfo":    strings.Replace(base, "https://peer.example:9293", "https://user:pass@peer.example:9293", 1),
		"path":        strings.Replace(base, "https://peer.example:9293", "https://peer.example:9293/path", 1),
		"query":       strings.Replace(base, "https://peer.example:9293", "https://peer.example:9293/?x=y", 1),
		"same token":  strings.Replace(base, outbound, inbound, 1),
		"no outbound": strings.Replace(base, outbound, "", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), inbound) || strings.Contains(err.Error(), outbound) {
				t.Fatalf("invalid peer accepted or token leaked: %v", err)
			}
		})
	}
}

func TestPeerManagementTargetBindings(t *testing.T) {
	peerToken, previewToken := strings.Repeat("p", 32), strings.Repeat("m", 32)
	management := `
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: ` + previewToken + `
  targets:
    swap: {directory: /config/llama-swap, filename: config.yaml}
`
	base := `
default_backend: local
backends:
  local: {url: http://localhost:9292}
  remote: {url: http://localhost:9295}
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  local_backends: [local]
  management_targets: {local: swap}
  peers:
    node-b: {token: ` + peerToken + `, catalog_read: true}
` + management
	cfg, err := Load(writeConfig(t, base))
	if err != nil || cfg.Peer.ManagementTargets["local"] != "swap" {
		t.Fatalf("valid management binding rejected: %v", err)
	}
	managed := strings.Replace(base, "catalog_read: true", "manage_settings: true", 1)
	cfg, err = Load(writeConfig(t, managed))
	if err != nil || !cfg.Peer.Peers["node-b"].ManageSettings || cfg.Peer.Peers["node-b"].CatalogRead {
		t.Fatalf("independent settings grant rejected: %v", err)
	}
	for name, contents := range map[string]string{
		"missing management": strings.TrimSuffix(base, management),
		"unowned backend":    strings.Replace(base, "{local: swap}", "{remote: swap}", 1),
		"missing backend":    strings.Replace(base, "{local: swap}", "{missing: swap}", 1),
		"missing target":     strings.Replace(base, "{local: swap}", "{local: missing}", 1),
		"empty target":       strings.Replace(base, "{local: swap}", `{local: ""}`, 1),
		"grant no binding":   strings.Replace(managed, "  management_targets: {local: swap}\n", "", 1),
		"grant no token":     strings.Replace(managed, "{token: "+peerToken+", manage_settings: true}", `{manage_settings: true, url: "https://peer.example", outbound_token: `+strings.Repeat("o", 32)+`}`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("invalid management binding accepted")
			}
		})
	}
}

func TestManagementConfigurationValidation(t *testing.T) {
	token := strings.Repeat("m", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: ` + token + `
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"short token":              strings.Replace(base, token, "short", 1),
		"reused diagnostics token": base + "diagnostics_api_key: " + token + "\n",
		"reused backend token":     strings.Replace(base, "url: http://localhost:9292", "url: http://localhost:9292, api_key: "+token, 1),
		"relative directory":       strings.Replace(base, "/config/llama-swap", "relative", 1),
		"traversal filename":       strings.Replace(base, "config.yaml", "../config.yaml", 1),
		"invalid target ID":        strings.Replace(base, "    local: {directory:", "    '../local': {directory:", 1),
		"missing TLS":              strings.Replace(base, "tls_key_file: key.pem", "tls_key_file: ''", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), token) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestDelegationCredentialValidation(t *testing.T) {
	preview, delegation := strings.Repeat("m", 32), strings.Repeat("l", 32)
	proposal, backup, write := strings.Repeat("p", 32), strings.Repeat("b", 32), strings.Repeat("w", 32)
	ingestion, hub, benchmark := strings.Repeat("i", 32), strings.Repeat("h", 32), strings.Repeat("q", 32)
	diagnostic, backend, header := strings.Repeat("d", 32), strings.Repeat("a", 32), strings.Repeat("z", 32)
	inbound, outbound := strings.Repeat("n", 32), strings.Repeat("o", 32)
	base := fmt.Sprintf(`
default_backend: local
backends:
  local:
    url: http://localhost:9292
    api_key: %s
    headers: {X-Custom-Auth: %s}
diagnostics_api_key: %s
peer:
  node_id: node-a
  listen: "127.0.0.1:9293"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  peers:
    node-b: {token: %s, catalog_read: true, url: "https://peer.example", outbound_token: %s}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: %s
  delegation_token: %s
  proposal_store: {directory: /state/proposals, token: %s}
  backup_store: {directory: /state/backups, token: %s, max_backups: 8}
  write_token: %s
  ingestion:
    directory: /models
    record_directory: /state/ingestion
    max_records: 100
    token: %s
    huggingface_token: %s
    max_bytes: 4096
    max_directory_bytes: 8192
    min_free_bytes: 1024
    timeout: 1h
    file_mode: "0640"
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
benchmark:
  token: %s
  proxy_origin: http://127.0.0.1:8080
  report_directory: /reports
`, backend, header, diagnostic, inbound, outbound, preview, delegation, proposal, backup, write, ingestion, hub, benchmark)
	cfg, err := Load(writeConfig(t, base))
	if err != nil || cfg.Management.DelegationToken != delegation {
		t.Fatalf("valid delegation credential rejected: %v", err)
	}
	for name, credential := range map[string]string{
		"preview": preview, "proposal": proposal, "backup": backup, "write": write,
		"ingestion": ingestion, "hub": hub, "benchmark": benchmark, "diagnostics": diagnostic,
		"backend": backend, "backend header": header, "inbound peer": inbound, "outbound peer": outbound,
		"too short": "short", "too long": strings.Repeat("x", 257), "invalid character": strings.Repeat("x", 31) + ",",
	} {
		t.Run(name, func(t *testing.T) {
			current, err := Load(writeConfig(t, base))
			if err != nil {
				t.Fatal(err)
			}
			current.Management.DelegationToken = credential
			err = current.Validate()
			if err == nil || strings.Contains(err.Error(), credential) {
				t.Fatalf("invalid or reused credential accepted or disclosed: %v", err)
			}
		})
	}
}

func TestDelegationOnlyManagementConfiguration(t *testing.T) {
	token := strings.Repeat("l", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  delegation_token: ` + token + `
`
	cfg, err := Load(writeConfig(t, base))
	if err != nil || cfg.Management.DelegationToken != token || cfg.Management.PreviewToken != "" || len(cfg.Management.Targets) != 0 {
		t.Fatalf("receiving-only configuration rejected: %v", err)
	}
	for name, contents := range map[string]string{
		"no credential":          strings.Replace(base, "  delegation_token: "+token+"\n", "", 1),
		"preview without target": base + "  preview_token: " + strings.Repeat("m", 32) + "\n",
		"target without preview": base + "  targets: {local: {directory: /config, filename: config.yaml}}\n",
		"store without preview":  base + "  proposal_store: {directory: /proposals, token: " + strings.Repeat("p", 32) + "}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, contents)); err == nil {
				t.Fatal("invalid receiving-only configuration accepted")
			}
		})
	}
}

func TestIngestionConfigurationValidation(t *testing.T) {
	preview, token, hub := strings.Repeat("m", 32), strings.Repeat("i", 32), strings.Repeat("h", 32)
	base := fmt.Sprintf(`
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: %s
  ingestion:
    directory: /models
    record_directory: /state/ingestion
    max_records: 100
    token: %s
    huggingface_token: %s
    max_bytes: 4096
    max_directory_bytes: 8192
    min_free_bytes: 1024
    timeout: 1h
    file_mode: "0640"
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
`, preview, token, hub)
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	benchmark := fmt.Sprintf("\ndiagnostics_api_key: %s\nbenchmark:\n  token: %s\n  proxy_origin: http://127.0.0.1:8080\n  report_directory: /reports\n", strings.Repeat("d", 32), strings.Repeat("b", 32))
	for name, contents := range map[string]string{
		"relative path":          strings.Replace(base, "directory: /models", "directory: models", 1),
		"relative records":       strings.Replace(base, "record_directory: /state/ingestion", "record_directory: relative", 1),
		"overlapping records":    strings.Replace(base, "record_directory: /state/ingestion", "record_directory: /models/records", 1),
		"invalid record limit":   strings.Replace(base, "max_records: 100", "max_records: 257", 1),
		"overlapping target":     strings.Replace(base, "directory: /models", "directory: /config", 1),
		"missing size":           strings.Replace(base, "max_bytes: 4096", "max_bytes: 0", 1),
		"negative quota":         strings.Replace(base, "max_directory_bytes: 8192", "max_directory_bytes: -1", 1),
		"negative reserve":       strings.Replace(base, "min_free_bytes: 1024", "min_free_bytes: -1", 1),
		"zero quota":             strings.Replace(base, "max_directory_bytes: 8192", "max_directory_bytes: 0", 1),
		"zero reserve":           strings.Replace(base, "min_free_bytes: 1024", "min_free_bytes: 0", 1),
		"missing quota":          strings.Replace(base, "    max_directory_bytes: 8192\n", "", 1),
		"missing reserve":        strings.Replace(base, "    min_free_bytes: 1024\n", "", 1),
		"missing deadline":       strings.Replace(base, "timeout: 1h", "timeout: 0s", 1),
		"invalid deadline":       strings.Replace(base, "timeout: 1h", "timeout: forever", 1),
		"unsafe permissions":     strings.Replace(base, `"0640"`, `"0666"`, 1),
		"reused management key":  strings.Replace(base, "token: "+token, "token: "+preview, 1),
		"reused hub key":         strings.Replace(base, "huggingface_token: "+hub, "huggingface_token: "+token, 1),
		"hub management key":     strings.Replace(base, "huggingface_token: "+hub, "huggingface_token: "+preview, 1),
		"backend token as hub":   strings.Replace(base, "url: http://localhost:9292", "url: http://localhost:9292, api_key: "+hub, 1),
		"diagnostics key as hub": base + "diagnostics_api_key: " + hub + "\n",
		"benchmark key as hub":   base + strings.Replace(benchmark, strings.Repeat("b", 32), hub, 1),
		"benchmark key reused":   base + strings.Replace(benchmark, strings.Repeat("b", 32), token, 1),
		"benchmark directory":    base + strings.Replace(benchmark, "/reports", "/models/reports", 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), preview) || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), hub) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestProposalStoreConfigurationValidation(t *testing.T) {
	preview, proposal := strings.Repeat("m", 32), strings.Repeat("p", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: ` + preview + `
  proposal_store: {directory: /state/proposals, token: ` + proposal + `}
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"same credential":     strings.Replace(base, proposal, preview, 1),
		"short credential":    strings.Replace(base, proposal, "short", 1),
		"relative store":      strings.Replace(base, "/state/proposals", "state", 1),
		"target directory":    strings.Replace(base, "/state/proposals", "/config/llama-swap", 1),
		"inside target":       strings.Replace(base, "/state/proposals", "/config/llama-swap/state", 1),
		"target inside store": strings.Replace(base, "/state/proposals", "/config", 1),
		"backend credential":  strings.Replace(base, "url: http://localhost:9292", "url: http://localhost:9292, api_key: "+proposal, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), preview) || strings.Contains(err.Error(), proposal) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestBackupStoreConfigurationValidation(t *testing.T) {
	preview, proposal, backup := strings.Repeat("m", 32), strings.Repeat("p", 32), strings.Repeat("b", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: ` + preview + `
  proposal_store: {directory: /state/proposals, token: ` + proposal + `}
  backup_store: {directory: /state/backups, token: ` + backup + `, max_backups: 8}
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	standalone := strings.Replace(base, "  proposal_store: {directory: /state/proposals, token: "+proposal+"}\n", "", 1)
	if _, err := Load(writeConfig(t, standalone)); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"preview credential":           strings.Replace(base, backup, preview, 1),
		"proposal credential":          strings.Replace(base, backup, proposal, 1),
		"short credential":             strings.Replace(base, backup, "short", 1),
		"backend credential":           strings.Replace(base, "url: http://localhost:9292", "url: http://localhost:9292, api_key: "+backup, 1),
		"relative store":               strings.Replace(base, "/state/backups", "state", 1),
		"target directory":             strings.Replace(base, "/state/backups", "/config/llama-swap", 1),
		"inside target":                strings.Replace(base, "/state/backups", "/config/llama-swap/state", 1),
		"target inside store":          strings.Replace(base, "/state/backups", "/config", 1),
		"same proposal store":          strings.Replace(base, "/state/backups", "/state/proposals", 1),
		"inside proposal":              strings.Replace(base, "/state/backups", "/state/proposals/backups", 1),
		"proposal inside":              strings.Replace(base, "/state/backups", "/state", 1),
		"missing limit":                strings.Replace(base, ", max_backups: 8", "", 1),
		"zero limit":                   strings.Replace(base, "max_backups: 8", "max_backups: 0", 1),
		"limit exceeds bounds":         strings.Replace(base, "max_backups: 8", "max_backups: 33", 1),
		"standalone shared credential": strings.Replace(standalone, backup, preview, 1),
		"standalone missing limit":     strings.Replace(standalone, ", max_backups: 8", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), preview) ||
				strings.Contains(err.Error(), proposal) || strings.Contains(err.Error(), backup) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestManagementWriteConfigurationValidation(t *testing.T) {
	preview, proposal, backup, write := strings.Repeat("m", 32), strings.Repeat("p", 32), strings.Repeat("b", 32), strings.Repeat("w", 32)
	base := `
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: cert.pem
  tls_key_file: key.pem
  preview_token: ` + preview + `
  proposal_store: {directory: /state/proposals, token: ` + proposal + `}
  backup_store: {directory: /state/backups, token: ` + backup + `, max_backups: 8}
  write_token: ` + write + `
  targets:
    local: {directory: /config/llama-swap, filename: config.yaml}
`
	if _, err := Load(writeConfig(t, base)); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"preview credential":  strings.Replace(base, write, preview, 1),
		"proposal credential": strings.Replace(base, write, proposal, 1),
		"backup credential":   strings.Replace(base, write, backup, 1),
		"short credential":    strings.Replace(base, write, "short", 1),
		"backend credential":  strings.Replace(base, "url: http://localhost:9292", "url: http://localhost:9292, api_key: "+write, 1),
		"missing proposals":   strings.Replace(base, "  proposal_store: {directory: /state/proposals, token: "+proposal+"}\n", "", 1),
		"missing backups":     strings.Replace(base, "  backup_store: {directory: /state/backups, token: "+backup+", max_backups: 8}\n", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), write) {
				t.Fatalf("invalid configuration accepted or credential leaked: %v", err)
			}
		})
	}
}

func TestLoadAppliesDefaultsAndEnvironmentOverrides(t *testing.T) {
	path := writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
  remote:
    url: http://remote-host:9292
    prefix: remote/
`)
	t.Setenv("LLAMA_CATALOG_LISTEN", "127.0.0.1:9090")
	t.Setenv("LLAMA_CATALOG_REQUEST_TIMEOUT", "3s")
	t.Setenv("LLAMA_CATALOG_DEFAULT_BACKEND_API_KEY", "default-secret")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:9090" || cfg.ModelsCacheTTL != 5*time.Minute || cfg.RequestTimeout != 3*time.Second {
		t.Fatalf("defaults or overrides not applied: %#v", cfg)
	}
	if cfg.Backends["remote"].Prefix != "remote/" {
		t.Errorf("backend prefix = %q", cfg.Backends["remote"].Prefix)
	}
	if cfg.Backends["default"].APIKey != "default-secret" {
		t.Error("default backend API key override was not applied")
	}
	if cfg.MaxJSONBody != 64<<20 {
		t.Errorf("default max JSON body = %d", cfg.MaxJSONBody)
	}
}

func TestLoadSupportsJSONRequestBodyLimit(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nmax_json_request_body_bytes: 1024\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxJSONBody != 1024 {
		t.Fatalf("max JSON body = %d", cfg.MaxJSONBody)
	}
}

func TestLoadRejectsInvalidJSONRequestBodyLimit(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nmax_json_request_body_bytes: 0\n"))
	if err == nil || !strings.Contains(err.Error(), "max_json_request_body_bytes") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadSupportsBackendHeaders(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
    headers:
      X-Backend-Token: secret
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].Headers["X-Backend-Token"] != "secret" {
		t.Fatal("backend header was not loaded")
	}
}

func TestLoadRejectsInvalidBackendHeaders(t *testing.T) {
	for _, header := range []string{"Bad Header", "X-Bad:Header"} {
		_, err := Load(writeConfig(t, "default_backend: default\nbackends:\n  default:\n    url: http://default-host:9292\n    headers:\n      \""+header+"\": value\n"))
		if err == nil || !strings.Contains(err.Error(), "invalid header name") {
			t.Fatalf("header %q error = %v", header, err)
		}
	}
	_, err := Load(writeConfig(t, "default_backend: default\nbackends:\n  default:\n    url: http://default-host:9292\n    headers:\n      X-Test: \"bad\\nvalue\"\n"))
	if err == nil {
		t.Fatal("accepted backend header with a newline")
	}
}

func TestLoadSupportsPathRoutes(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  ai5090: {url: http://ai5090:9292, prefix: ai5090/}
path_routes:
  /comfyui: ai5090
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PathRoutes["/comfyui"] != "ai5090" {
		t.Fatalf("path route = %q", cfg.PathRoutes["/comfyui"])
	}
}

func TestLoadSupportsComfyUIWaitTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  gpu: {url: http://gpu-host:9292}
path_routes:
  /comfyui: gpu
comfyui_wait_timeout: 45m
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ComfyUIWaitTimeout != 45*time.Minute {
		t.Fatalf("comfyui_wait_timeout = %s", cfg.ComfyUIWaitTimeout)
	}
}

func TestLoadRejectsComfyUIWaitTimeoutWithoutRoute(t *testing.T) {
	_, err := Load(writeConfig(t, `
default_backend: default
backends: {default: {url: http://default-host:9292}}
comfyui_wait_timeout: 1m
`))
	if err == nil || !strings.Contains(err.Error(), "requires path_routes /comfyui") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsNegativeComfyUIWaitTimeout(t *testing.T) {
	_, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  gpu: {url: http://gpu-host:9292}
path_routes: {/comfyui: gpu}
comfyui_wait_timeout: -1s
`))
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsInvalidPathRoutes(t *testing.T) {
	tests := []struct {
		name      string
		route     string
		backend   string
		wantError string
	}{
		{name: "relative", route: "comfyui", backend: "default", wantError: "canonical absolute path"},
		{name: "trailing slash", route: "/comfyui/", backend: "default", wantError: "canonical absolute path"},
		{name: "unclean", route: "/one/../comfyui", backend: "default", wantError: "canonical absolute path"},
		{name: "query", route: "/comfyui?mode=test", backend: "default", wantError: "canonical absolute path"},
		{name: "undefined backend", route: "/comfyui", backend: "missing", wantError: "undefined backend"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\npath_routes:\n  " + test.route + ": " + test.backend + "\n"
			_, err := Load(writeConfig(t, contents))
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadAppliesDefaultHealthPath(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  remote: {url: http://remote-host:9292, prefix: remote/}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].HealthPath != "/health" || cfg.Backends["remote"].HealthPath != "/health" {
		t.Fatalf("health paths = %#v", cfg.Backends)
	}
}

func TestLoadSupportsConfiguredHealthPath(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292, health_path: /api/version}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].HealthPath != "/api/version" {
		t.Fatalf("health path = %q", cfg.Backends["default"].HealthPath)
	}
}

func TestLoadRejectsInvalidHealthPaths(t *testing.T) {
	tests := []struct {
		name       string
		healthPath string
	}{
		{name: "relative", healthPath: "api/version"},
		{name: "trailing slash", healthPath: "/api/version/"},
		{name: "unclean", healthPath: "/api/../version"},
		{name: "query", healthPath: "/api/version?x=1"},
		{name: "fragment", healthPath: "/api/version#one"},
		{name: "surrounding whitespace", healthPath: " /api/version"},
		{name: "embedded space", healthPath: "/api/ver sion"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := "default_backend: default\nbackends: {default: {url: http://default-host:9292, health_path: \"" + test.healthPath + "\"}}\n"
			_, err := Load(writeConfig(t, contents))
			if err == nil || !strings.Contains(err.Error(), "health_path") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadSupportsOllamaShowEnrichment(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292, ollama_show_enrichment: true}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Backends["default"].OllamaShowEnrichment {
		t.Fatal("ollama_show_enrichment was not loaded")
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nunknown: true\n"))
	if err == nil {
		t.Fatal("Load() accepted an unknown field")
	}
}

func TestLoadRejectsMissingDefaultBackend(t *testing.T) {
	_, err := Load(writeConfig(t, "backends: {default: {url: http://default-host:9292}}\n"))
	if err == nil || !strings.Contains(err.Error(), "default_backend") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsUndefinedDefaultBackend(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: missing\nbackends: {default: {url: http://default-host:9292}}\n"))
	if err == nil || !strings.Contains(err.Error(), "not defined") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadAcceptsPrefixedDefaultBackend(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292, prefix: default/}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backends["default"].Prefix != "default/" {
		t.Fatalf("default prefix = %q", cfg.Backends["default"].Prefix)
	}
}

func TestLoadRejectsDuplicateRoutingPrefixes(t *testing.T) {
	_, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
  one: {url: http://one:9292, prefix: shared/}
  two: {url: http://two:9292, prefix: shared/}
`))
	if err == nil || !strings.Contains(err.Error(), "same prefix") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadSupportsHealthAndCatalogTimeouts(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
health_timeout: 2s
catalog_timeout: 20s
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthTimeout != 2*time.Second {
		t.Fatalf("health_timeout = %v", cfg.HealthTimeout)
	}
	if cfg.CatalogTimeout != 20*time.Second {
		t.Fatalf("catalog_timeout = %v", cfg.CatalogTimeout)
	}
}

func TestLoadInheritsHealthAndCatalogTimeoutsFromRequestTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default: {url: http://default-host:9292}
request_timeout: 7s
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthTimeout != 7*time.Second {
		t.Fatalf("health_timeout inherited from request_timeout = %v", cfg.HealthTimeout)
	}
	if cfg.CatalogTimeout != 7*time.Second {
		t.Fatalf("catalog_timeout inherited from request_timeout = %v", cfg.CatalogTimeout)
	}
}

func TestLoadRejectsNonPositiveHealthTimeout(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nhealth_timeout: 0\n"))
	if err == nil || !strings.Contains(err.Error(), "health_timeout") {
		t.Fatalf("error = %v", err)
	}
	_, err = Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nhealth_timeout: -1s\n"))
	if err == nil || !strings.Contains(err.Error(), "health_timeout") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsNonPositiveCatalogTimeout(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\ncatalog_timeout: 0\n"))
	if err == nil || !strings.Contains(err.Error(), "catalog_timeout") {
		t.Fatalf("error = %v", err)
	}
	_, err = Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\ncatalog_timeout: -1s\n"))
	if err == nil || !strings.Contains(err.Error(), "catalog_timeout") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadAppliesHealthAndCatalogTimeoutEnvironmentOverrides(t *testing.T) {
	t.Setenv(envHealthTimeout, "3s")
	t.Setenv(envCatalogTimeout, "30s")
	defer func() {
		t.Setenv(envHealthTimeout, "")
		t.Setenv(envCatalogTimeout, "")
	}()
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HealthTimeout != 3*time.Second {
		t.Fatalf("health_timeout env override = %v", cfg.HealthTimeout)
	}
	if cfg.CatalogTimeout != 30*time.Second {
		t.Fatalf("catalog_timeout env override = %v", cfg.CatalogTimeout)
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	_, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\nlog_level: noisy\n"))
	if err == nil {
		t.Fatal("Load() accepted an invalid log_level")
	}
}

func TestLoadSupportsRequestHistory(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
request_history: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.RequestHistory {
		t.Fatal("request_history was not loaded")
	}
}

func TestLoadRequestHistoryDefaultsToFalse(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RequestHistory {
		t.Fatal("request_history should default to false")
	}
}

func TestLoadSupportsRequestObservation(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
request_observation: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.RequestObservation {
		t.Fatal("request_observation was not loaded")
	}
}

func TestLoadRequestObservationDefaultsToFalse(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
default_backend: default
backends:
  default:
    url: http://default-host:9292
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RequestObservation {
		t.Fatal("request_observation should default to false")
	}
}

func TestLoadRejectsInvalidDiagnosticsAPIKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr string
	}{
		{name: "too long", key: strings.Repeat("a", 4097), wantErr: "diagnostics_api_key"},
		{name: "tab", key: "key\t", wantErr: "diagnostics_api_key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\ndiagnostics_api_key: \"" + test.key + "\"\n"
			_, err := Load(writeConfig(t, contents))
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestValidateDiagnosticsAPIKeyRejectsControlCharacters(t *testing.T) {
	for _, key := range []string{"key\r", "key\n", "key\r\n", "key\x00", "key\x01", "key\x1f", "key\x7f"} {
		if err := validateDiagnosticsAPIKey(key); err == nil {
			t.Fatalf("validateDiagnosticsAPIKey(%q) accepted key with control character", key)
		}
	}
	if err := validateDiagnosticsAPIKey("valid-key"); err != nil {
		t.Fatalf("validateDiagnosticsAPIKey(%q) rejected valid key: %v", "valid-key", err)
	}
	if err := validateDiagnosticsAPIKey(""); err != nil {
		t.Fatalf("validateDiagnosticsAPIKey(empty) rejected empty key: %v", err)
	}
}

func TestLoadAcceptsValidDiagnosticsAPIKey(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\ndiagnostics_api_key: test-diagnostics-api-key-placeholder\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DiagnosticsAPIKey != "test-diagnostics-api-key-placeholder" {
		t.Fatalf("diagnostics_api_key = %q", cfg.DiagnosticsAPIKey)
	}
}

func TestDiagnosticsAPIKeyDefaultsToEmpty(t *testing.T) {
	cfg, err := Load(writeConfig(t, "default_backend: default\nbackends: {default: {url: http://default-host:9292}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DiagnosticsAPIKey != "" {
		t.Fatalf("diagnostics_api_key should default to empty, got %q", cfg.DiagnosticsAPIKey)
	}
}

func TestRedactedURLHidesUserinfo(t *testing.T) {
	if got := RedactedURL("http://alice:supersecret@example.test:9292/base"); got != "http://xxxxx@example.test:9292/base" {
		t.Fatalf("RedactedURL() = %q", got)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
