package privateorigin

import (
	"errors"
	"net"
	"net/url"
)

func Parse(value string) (*url.URL, error) {
	origin, err := url.Parse(value)
	if err != nil || origin == nil || (origin.Scheme != "http" && origin.Scheme != "https") ||
		origin.Host == "" || origin.User != nil || origin.Opaque != "" ||
		(origin.Path != "" && origin.Path != "/") || origin.RawQuery != "" ||
		origin.ForceQuery || origin.Fragment != "" || origin.RawFragment != "" {
		return nil, errors.New("endpoint must be an HTTP(S) origin without credentials, path, query, or fragment")
	}
	ip := net.ParseIP(origin.Hostname())
	if ip == nil || !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()) {
		return nil, errors.New("endpoint must use a loopback or private-network IP address")
	}
	return origin, nil
}
