package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/proxy"
)

func runComfyUILeaseCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "release" {
		return errors.New("usage: llama-catalog-proxy comfyui-lease release --config path --acknowledge")
	}
	flags := flag.NewFlagSet("comfyui-lease release", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPeerConfigPath, "path to YAML configuration")
	acknowledge := flags.Bool("acknowledge", false, "confirm unload and GPU-memory release request")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !*acknowledge {
		return errors.New("--acknowledge is required")
	}
	return withPeerCLIAudit(*configPath, "comfyui_lease.release", "comfyui", "", "", stdout,
		func(out io.Writer) (string, string, string, error) {
			cfg, err := config.Load(*configPath)
			if err != nil {
				return "comfyui", "", "", err
			}
			if cfg.Peer == nil || cfg.Peer.ComfyUIResultRead == nil || cfg.Peer.ComfyUILease == nil {
				return "comfyui", "", "", errors.New("peer comfyui_result_read and comfyui_lease configuration are required")
			}
			if len(cfg.Peer.ComfyUILease.AdmissionBackends) > 0 {
				return "comfyui", "", "", errors.New("standalone release is disabled when proxy admission coordination is enabled")
			}
			transport := &http.Transport{
				Proxy: nil, DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{
				Transport: transport, Timeout: 5 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			headers := comfyUILeaseBackendHeaders(*cfg.Peer.ComfyUIResultRead)
			ctx, cancel := context.WithTimeout(context.Background(), cfg.Peer.ComfyUILease.ReleaseTimeoutDuration())
			defer cancel()
			result, releaseErr := proxy.VerifyComfyUIRelease(ctx, cfg.Peer.ComfyUIResultRead.URL, headers, cfg.Peer.ComfyUILease, client)
			if err := json.NewEncoder(out).Encode(result); err != nil {
				return "comfyui", "", "", err
			}
			return "comfyui", "", "", releaseErr
		})
}

func comfyUILeaseBackendHeaders(target config.ComfyUIResultReadConfig) http.Header {
	headers := make(http.Header, len(target.Headers)+2)
	for name, value := range target.Headers {
		headers.Set(name, value)
	}
	if target.APIKey != "" && headers.Get("Authorization") == "" && headers.Get("X-Api-Key") == "" {
		headers.Set("Authorization", "Bearer "+target.APIKey)
		headers.Set("X-Api-Key", target.APIKey)
	}
	return headers
}
