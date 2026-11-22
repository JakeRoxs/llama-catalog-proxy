//go:build linux

package proxy

import "fmt"

// backupConfigTemplate returns the shared ten-line proxy management config
// template (default_backend, local backend, management listen/TLS, preview
// token, proposal_store, and targets.local). Callers append the file-specific
// backup_store and write_token lines, so the duplicated block is defined once
// across backups_linux_test.go and mutations_linux_test.go.
func backupConfigTemplate(certPath, keyPath, previewToken, proposalDir, proposalToken, targetDir string) string {
	return fmt.Sprintf(`default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  proposal_store: {directory: %q, token: %q}
  targets:
    local: {directory: %q, filename: config.yaml}
`, certPath, keyPath, previewToken, proposalDir, proposalToken, targetDir)
}
