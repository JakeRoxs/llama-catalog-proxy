//go:build linux

package ingestion

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixtureClient(t *testing.T, handler http.HandlerFunc) *http.Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client := server.Client()
	transport := client.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.ServerName = "example.com"
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	transport.DisableCompression = true
	client.Transport = transport
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestDownloadArtifactFollowsTrustedRedirectsWithoutForwardingCredentials(t *testing.T) {
	data := ggufHeader(3, 0, 0)
	calls := 0
	client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			if r.Host != "huggingface.co" || r.URL.EscapedPath() != "/owner/repo/resolve/main/model.gguf" ||
				r.Header.Get("Authorization") != "Bearer fixture-token" || r.Header.Get("Accept-Encoding") != "identity" {
				t.Errorf("unexpected initial request: host=%q path=%q auth=%q", r.Host, r.URL.Path, r.Header.Get("Authorization"))
			}
			http.Redirect(w, r, "/owner/repo/resolve/commit/model.gguf", http.StatusFound)
		case 2:
			if r.Header.Get("Authorization") != "Bearer fixture-token" {
				t.Error("same-origin redirect lost authorization")
			}
			http.Redirect(w, r, "https://cas-bridge.xethub.hf.co/file?signature=fixture-secret", http.StatusFound)
		case 3:
			if r.Host != "cas-bridge.xethub.hf.co" || r.Header.Get("Authorization") != "" {
				t.Error("cross-origin redirect forwarded authorization")
			}
			http.Redirect(w, r, "https://huggingface.co/final", http.StatusFound)
		case 4:
			if r.Header.Get("Authorization") != "" {
				t.Error("authorization was restored after leaving the Hub")
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			_, _ = w.Write(data)
		default:
			t.Error("unexpected request")
		}
	})
	options := stageOptions(t.TempDir())
	options.MaxBytes = int64(len(data))
	result, err := downloadArtifact(context.Background(), options, time.Minute, "fixture-token", client)
	if err != nil || result.Publication != PublicationPublished || result.Bytes != int64(len(data)) || calls != 4 {
		t.Fatalf("result = %+v, error = %v, calls = %d", result, err, calls)
	}
}

func TestDownloadArtifactRejectsUnsafeResponses(t *testing.T) {
	data := ggufHeader(3, 0, 0)
	for _, tt := range []struct {
		name   string
		handle http.HandlerFunc
		want   error
	}{
		{"untrusted redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://evil.example/secret?signature=fixture-secret", http.StatusFound)
		}, ErrDownloadRedirect},
		{"insecure redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://huggingface.co/file", http.StatusFound)
		}, ErrDownloadRedirect},
		{"too many redirects", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/again?signature=fixture-secret", http.StatusFound)
		}, ErrDownloadRedirect},
		{"http status", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("fixture-secret"))
		}, ErrDownloadResponse},
		{"missing length", func(w http.ResponseWriter, _ *http.Request) {
			w.(http.Flusher).Flush()
			_, _ = w.Write(data)
		}, ErrDownloadResponse},
		{"compressed", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(data)
		}, ErrDownloadResponse},
		{"range", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Range", "bytes 0-23/100")
			_, _ = w.Write(data)
		}, ErrDownloadResponse},
		{"oversized", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "2000")
		}, ErrStageTooLarge},
		{"interrupted", func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "huggingface.co" {
				http.Redirect(w, r, "https://cas-bridge.xethub.hf.co/file?signature=fixture-secret", http.StatusFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(data)+10))
			_, _ = w.Write(data)
		}, ErrDownloadTransport},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options := stageOptions(t.TempDir())
			client := fixtureClient(t, tt.handle)
			result, err := downloadArtifact(context.Background(), options, time.Minute, "fixture-secret", client)
			if !errors.Is(err, tt.want) || result.Publication != PublicationNotPublished {
				t.Fatalf("result = %+v, error = %v; want %v", result, err, tt.want)
			}
			if strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("error disclosed a credential")
			}
			entries, err := os.ReadDir(options.Directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging left files: %v, %v", entries, err)
			}
		})
	}
}

func TestDownloadArtifactCancellationAndStagingPreflight(t *testing.T) {
	for _, tt := range []struct {
		name    string
		timeout bool
		want    error
	}{
		{"cancel while reading", false, context.Canceled},
		{"deadline while reading", true, context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan struct{})
			client := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
			})
			options := stageOptions(t.TempDir())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				duration := time.Minute
				if tt.timeout {
					duration = 250 * time.Millisecond
				}
				result, err := downloadArtifact(ctx, options, duration, "", client)
				if result.Publication != PublicationNotPublished {
					done <- fmt.Errorf("unexpected publication: %s", result.Publication)
					return
				}
				done <- err
			}()
			<-started
			if !tt.timeout {
				cancel()
			}
			if err := <-done; !errors.Is(err, tt.want) {
				t.Fatalf("error = %v", err)
			}
			if _, err := os.Stat(filepath.Join(options.Directory, options.Source.Filename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination exists or could not be inspected: %v", err)
			}
		})
	}
	t.Run("existing destination", func(t *testing.T) {
		client := fixtureClient(t, func(http.ResponseWriter, *http.Request) {
			t.Error("network request sent despite existing destination")
		})
		options := stageOptions(t.TempDir())
		if err := os.WriteFile(filepath.Join(options.Directory, options.Source.Filename), []byte("existing"), 0o600); err != nil {
			t.Fatal(err)
		}
		result, err := downloadArtifact(context.Background(), options, time.Minute, "", client)
		if !errors.Is(err, ErrStageExists) || result.Publication != PublicationNotPublished {
			t.Fatalf("result = %+v, error = %v", result, err)
		}
	})
}

func TestDownloadJobWithLocalTLSFixture(t *testing.T) {
	data := ggufHeader(3, 0, 0)
	client := fixtureClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = w.Write(data)
	})
	manager, err := NewJobManager(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	manager.run = func(ctx context.Context, options StageOptions, duration time.Duration, token string) (StageResult, error) {
		return downloadArtifact(ctx, options, duration, token, client)
	}
	options := stageOptions(t.TempDir())
	job, err := manager.Start(options, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	manager.wg.Wait()
	status, err := manager.Status(job.ID)
	if err != nil || status.State != "succeeded" || status.Result.Publication != PublicationPublished {
		t.Fatalf("status = %+v, error = %v", status, err)
	}
}

func TestDownloadURLAndAddressRestrictions(t *testing.T) {
	for _, value := range []string{
		"http://huggingface.co/a", "https://huggingface.co.evil.example/a",
		"https://evil.example@huggingface.co/a", "https://huggingface.co:444/a",
		"https://huggingface.co/a#fragment", "https://127.0.0.1/a",
	} {
		u, err := url.Parse(value)
		if err != nil || trustedDownloadURL(u) {
			t.Errorf("accepted URL %q", value)
		}
	}
	for _, value := range []string{"127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.169.254", "::1", "fc00::1", "::ffff:192.168.1.1"} {
		if publicAddress(netip.MustParseAddr(value)) {
			t.Errorf("accepted non-public address %s", value)
		}
	}
	for _, value := range []string{"1.1.1.1", "2606:4700:4700::1111"} {
		if !publicAddress(netip.MustParseAddr(value)) {
			t.Errorf("rejected public address %s", value)
		}
	}
}
