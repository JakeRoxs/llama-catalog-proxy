package federation

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"time"
)

const (
	maxCRLBundleSize      = 1 << 20
	maxFederationCertAge  = 30 * 24 * time.Hour
	crlClockSkewAllowance = 5 * time.Minute
)

type CRLPolicyTimeStatus struct {
	State     string
	UpdatedAt time.Time
	ExpiresAt time.Time
	MaxAge    time.Duration
}

func CRLPolicyTime(crls []*x509.RevocationList, maxAge time.Duration, now time.Time) CRLPolicyTimeStatus {
	status := CRLPolicyTimeStatus{State: "not_configured", MaxAge: maxAge}
	if len(crls) == 0 {
		return status
	}
	status.State = "fresh"
	for _, crl := range crls {
		if crl == nil {
			status.State = "unavailable"
			continue
		}
		if status.UpdatedAt.IsZero() || crl.ThisUpdate.Before(status.UpdatedAt) {
			status.UpdatedAt = crl.ThisUpdate
		}
		if status.ExpiresAt.IsZero() || crl.NextUpdate.Before(status.ExpiresAt) {
			status.ExpiresAt = crl.NextUpdate
		}
		if ValidateCRLFreshness(crl, maxAge, now) != nil && status.State == "fresh" {
			status.State = "stale"
		}
	}
	return status
}

func LoadCRLs(path string, maxAge time.Duration) ([]*x509.RevocationList, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrPairingStore
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxCRLBundleSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > maxCRLBundleSize {
		return nil, ErrPairingStore
	}
	var crls []*x509.RevocationList
	remaining := bytes.TrimSpace(data)
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "X509 CRL" || len(block.Headers) != 0 {
			return nil, ErrPairingStore
		}
		crl, err := x509.ParseRevocationList(block.Bytes)
		if err != nil || crl.Number == nil || crl.ThisUpdate.IsZero() || crl.NextUpdate.IsZero() {
			return nil, ErrPairingStore
		}
		for _, extension := range crl.Extensions {
			if extension.Critical {
				return nil, ErrPairingStore
			}
		}
		for _, entry := range crl.RevokedCertificateEntries {
			for _, extension := range entry.Extensions {
				if extension.Critical {
					return nil, ErrPairingStore
				}
			}
		}
		if err := ValidateCRLFreshness(crl, maxAge, time.Now()); err != nil {
			return nil, ErrPairingStore
		}
		crls = append(crls, crl)
		if len(crls) > 64 {
			return nil, ErrPairingStore
		}
		remaining = bytes.TrimSpace(rest)
	}
	if len(crls) == 0 {
		return nil, ErrPairingStore
	}
	return crls, nil
}

func ValidateCRLFreshness(crl *x509.RevocationList, maxAge time.Duration, now time.Time) error {
	if crl == nil || maxAge < time.Minute || maxAge > 30*24*time.Hour || crl.ThisUpdate.IsZero() || crl.NextUpdate.IsZero() {
		return ErrPairingStore
	}
	if crl.ThisUpdate.After(now.Add(crlClockSkewAllowance)) || !now.Before(crl.NextUpdate) ||
		now.Sub(crl.ThisUpdate) > maxAge || crl.NextUpdate.Before(crl.ThisUpdate) || crl.NextUpdate.Sub(crl.ThisUpdate) > maxAge {
		return ErrPairingStore
	}
	return nil
}

func VerifyCertificateRevocation(chains [][]*x509.Certificate, crls []*x509.RevocationList, maxAge time.Duration, now time.Time) error {
	if len(chains) == 0 || len(crls) == 0 {
		return ErrPairingStore
	}
	for _, chain := range chains {
		if len(chain) < 2 {
			return ErrPairingStore
		}
		for index := 0; index < len(chain)-1; index++ {
			certificate, issuer := chain[index], chain[index+1]
			var selected *x509.RevocationList
			for _, crl := range crls {
				if !bytes.Equal(crl.RawIssuer, issuer.RawSubject) || crl.CheckSignatureFrom(issuer) != nil {
					continue
				}
				if selected == nil || crl.Number.Cmp(selected.Number) > 0 {
					selected = crl
				} else if crl.Number.Cmp(selected.Number) == 0 && !bytes.Equal(crl.Raw, selected.Raw) {
					return ErrPairingStore
				}
			}
			if selected == nil || ValidateCRLFreshness(selected, maxAge, now) != nil || isCertificateRevoked(selected, certificate.SerialNumber) {
				return ErrPairingStore
			}
		}
	}
	return nil
}

func isCertificateRevoked(crl *x509.RevocationList, serial *big.Int) bool {
	if serial == nil {
		return true
	}
	for _, entry := range crl.RevokedCertificateEntries {
		if entry.SerialNumber.Cmp(serial) == 0 {
			return true
		}
	}
	for _, entry := range crl.RevokedCertificates {
		if entry.SerialNumber.Cmp(serial) == 0 {
			return true
		}
	}
	return false
}

func ValidateFederationCertificateLifetime(cert *x509.Certificate) error {
	if cert == nil || !cert.NotAfter.After(cert.NotBefore) || cert.NotAfter.Sub(cert.NotBefore) > maxFederationCertAge {
		return errors.New("federation identity certificate must be valid for no more than 30 days")
	}
	return nil
}
