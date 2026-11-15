package ingestion

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

var (
	ErrDownloadInvalid   = errors.New("invalid download configuration")
	ErrDownloadRedirect  = errors.New("download redirect is unsupported")
	ErrDownloadResponse  = errors.New("download response is unsupported")
	ErrDownloadTransport = errors.New("download transport failed")
)

const maxDownloadRedirects = 8

// DownloadArtifact stages a single Hub resolve response. token is never included
// in the result or returned errors. Duration bounds the whole operation.
func DownloadArtifact(ctx context.Context, options StageOptions, duration time.Duration, token string) (StageResult, error) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext:            dialPublicHTTPS,
			ForceAttemptHTTP2:      true,
			DisableCompression:     true,
			TLSHandshakeTimeout:    10 * time.Second,
			ResponseHeaderTimeout:  30 * time.Second,
			MaxResponseHeaderBytes: 32 << 10,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	return downloadArtifact(ctx, options, duration, token, client)
}

func downloadArtifact(ctx context.Context, options StageOptions, duration time.Duration, token string, client *http.Client) (StageResult, error) {
	empty := StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}
	if ctx == nil || duration <= 0 || len(token) > 4096 || strings.ContainsAny(token, "\r\n") || client == nil {
		return empty, ErrDownloadInvalid
	}
	source, err := ParseSource(SourceInput{
		Repository: options.Source.Repository, Filename: options.Source.Filename, Revision: options.Source.Revision,
	})
	if err != nil {
		return empty, ErrStageInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	reader := &downloadReader{ctx: ctx, client: client, source: source, token: token, maxBytes: options.MaxBytes}
	result, err := StageNewArtifact(ctx, options, reader)
	if reader.body != nil {
		if closeErr := reader.body.Close(); closeErr != nil {
			err = errors.Join(err, ErrDownloadTransport)
		}
	}
	if reader.err != nil {
		err = errors.Join(err, reader.err)
	}
	return result, err
}

type downloadReader struct {
	ctx      context.Context
	client   *http.Client
	source   Source
	token    string
	maxBytes int64
	body     io.ReadCloser
	expected int64
	read     int64
	err      error
	opened   bool
}

func (r *downloadReader) Read(buffer []byte) (int, error) {
	if !r.opened {
		r.opened = true
		r.err = r.open()
		if r.err != nil {
			return 0, r.err
		}
	}
	n, err := r.body.Read(buffer)
	if int64(n) > r.expected-r.read {
		r.err = ErrDownloadResponse
		return 0, r.err
	}
	r.read += int64(n)
	if err == io.EOF && r.read == r.expected {
		if closeErr := r.body.Close(); closeErr != nil {
			r.err = ErrDownloadTransport
			return n, r.err
		}
		r.body = nil
		return n, io.EOF
	}
	if err != nil {
		r.err = ErrDownloadTransport
		return n, r.err
	}
	return n, nil
}

func (r *downloadReader) open() error {
	current := &url.URL{
		Scheme: "https", Host: "huggingface.co",
		Path: "/" + r.source.Repository + "/resolve/" + r.source.Revision + "/" + r.source.Filename,
	}
	sendToken := true
	for redirects := 0; ; redirects++ {
		request, err := http.NewRequestWithContext(r.ctx, http.MethodGet, current.String(), nil)
		if err != nil {
			return ErrDownloadInvalid
		}
		request.Header.Set("Accept-Encoding", "identity")
		if sendToken && r.token != "" {
			request.Header.Set("Authorization", "Bearer "+r.token)
		}
		response, err := r.client.Do(request)
		if err != nil {
			return ErrDownloadTransport
		}
		switch response.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			next, locationErr := response.Location()
			closeErr := response.Body.Close()
			if closeErr != nil {
				return ErrDownloadTransport
			}
			if locationErr != nil || redirects >= maxDownloadRedirects || !trustedDownloadURL(next) {
				return ErrDownloadRedirect
			}
			if !strings.EqualFold(next.Hostname(), "huggingface.co") {
				sendToken = false
			}
			current = next
		case http.StatusOK:
			encoding := response.Header.Get("Content-Encoding")
			if response.ContentLength <= 0 || response.Header.Get("Content-Range") != "" ||
				(encoding != "" && !strings.EqualFold(encoding, "identity")) {
				return closeDownloadResponse(response, ErrDownloadResponse)
			}
			if response.ContentLength > r.maxBytes {
				return closeDownloadResponse(response, ErrStageTooLarge)
			}
			r.body, r.expected = response.Body, response.ContentLength
			return nil
		default:
			return closeDownloadResponse(response, ErrDownloadResponse)
		}
	}
}

func closeDownloadResponse(response *http.Response, err error) error {
	if closeErr := response.Body.Close(); closeErr != nil {
		return errors.Join(err, ErrDownloadTransport)
	}
	return err
}

func trustedDownloadURL(u *url.URL) bool {
	if u == nil || len(u.String()) > 16384 || u.Scheme != "https" || u.User != nil ||
		u.Opaque != "" || u.Fragment != "" || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if !strings.EqualFold(u.Host, host) && !strings.EqualFold(u.Host, host+":443") {
		return false
	}
	switch host {
	case "huggingface.co", "cas-bridge.xethub.hf.co",
		"cas-server.xethub.hf.co", "cas-server.xethub-eu.hf.co",
		"transfer.xethub.hf.co", "transfer.xethub-eu.hf.co",
		"us.aws.cdn.hf.co", "us.gcp.cdn.hf.co", "cdn-lfs-us-1.hf.co",
		"cdn-lfs-eu-1.hf.co", "cdn-lfs.huggingface.co":
		return true
	default:
		return false
	}
}

var nonPublicNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func publicAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || (ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip)) {
		return false
	}
	for _, prefix := range nonPublicNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func dialPublicHTTPS(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port != "443" {
		return nil, ErrDownloadTransport
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, ErrDownloadTransport
	}
	for _, ip := range addresses {
		if !publicAddress(ip) {
			return nil, ErrDownloadTransport
		}
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	for _, ip := range addresses {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
	}
	return nil, ErrDownloadTransport
}
