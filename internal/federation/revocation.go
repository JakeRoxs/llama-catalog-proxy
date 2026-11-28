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

// readCRLFile opens, reads (bounded by maxCRLBundleSize+1), and closes the CRL
// file at path. It returns ErrPairingStore on open/read/close failure, empty
// input, or input exceeding the maximum bundle size. File close/error order is
// preserved: close is always attempted and both read and close errors are checked.
func readCRLFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrPairingStore
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxCRLBundleSize+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > maxCRLBundleSize {
		return nil, ErrPairingStore
	}
	return data, nil
}

// validateNoCriticalExtensions returns ErrPairingStore when any CRL top-level
// extension or any revoked-certificate-entry extension is marked critical.
func validateNoCriticalExtensions(crl *x509.RevocationList) error {
	for _, extension := range crl.Extensions {
		if extension.Critical {
			return ErrPairingStore
		}
	}
	for _, entry := range crl.RevokedCertificateEntries {
		for _, extension := range entry.Extensions {
			if extension.Critical {
				return ErrPairingStore
			}
		}
	}
	return nil
}

// parseCRLBlock decodes a single PEM block, verifies its type and headers,
// parses the revocation list, rejects critical extensions, and validates
// freshness against maxAge using the current clock. It returns the parsed CRL
// and the unparsed remainder of the input.
func parseCRLBlock(remaining []byte, maxAge time.Duration) (*x509.RevocationList, []byte, error) {
	block, rest := pem.Decode(remaining)
	if block == nil || block.Type != "X509 CRL" || len(block.Headers) != 0 {
		return nil, nil, ErrPairingStore
	}
	crl, err := x509.ParseRevocationList(block.Bytes)
	if err != nil || crl.Number == nil || crl.ThisUpdate.IsZero() || crl.NextUpdate.IsZero() {
		return nil, nil, ErrPairingStore
	}
	if err := validateNoCriticalExtensions(crl); err != nil {
		return nil, nil, err
	}
	if err := ValidateCRLFreshness(crl, maxAge, time.Now()); err != nil {
		return nil, nil, ErrPairingStore
	}
	return crl, rest, nil
}

// LoadCRLs reads a PEM bundle of revocation lists from path and returns the
// parsed lists. File reading, per-block parsing, freshness, critical-extension,
// and count limits are delegated to pure helpers; LoadCRLs owns iteration,
// ordering, and the 64-list cap.
func LoadCRLs(path string, maxAge time.Duration) ([]*x509.RevocationList, error) {
	data, err := readCRLFile(path)
	if err != nil {
		return nil, err
	}
	var crls []*x509.RevocationList
	remaining := bytes.TrimSpace(data)
	for len(remaining) > 0 {
		crl, rest, err := parseCRLBlock(remaining, maxAge)
		if err != nil {
			return nil, err
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

// selectCRLForIssuer returns the CRL issued by (and signed by) issuer with the
// highest Number. Two CRLs with the same Number but different Raw content are
// an error; identical duplicates keep the first-seen CRL.
func selectCRLForIssuer(crls []*x509.RevocationList, issuer *x509.Certificate) (*x509.RevocationList, error) {
	var selected *x509.RevocationList
	for _, crl := range crls {
		if !bytes.Equal(crl.RawIssuer, issuer.RawSubject) || crl.CheckSignatureFrom(issuer) != nil {
			continue
		}
		if selected == nil || crl.Number.Cmp(selected.Number) > 0 {
			selected = crl
		} else if crl.Number.Cmp(selected.Number) == 0 && !bytes.Equal(crl.Raw, selected.Raw) {
			return nil, ErrPairingStore
		}
	}
	return selected, nil
}

// verifyCertificateAgainstCRL returns ErrPairingStore when no CRL was selected,
// the selected CRL is not fresh, or the certificate serial is revoked.
func verifyCertificateAgainstCRL(selected *x509.RevocationList, certificate *x509.Certificate, maxAge time.Duration, now time.Time) error {
	if selected == nil || ValidateCRLFreshness(selected, maxAge, now) != nil || isCertificateRevoked(selected, certificate.SerialNumber) {
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
			selected, err := selectCRLForIssuer(crls, issuer)
			if err != nil {
				return ErrPairingStore
			}
			if err := verifyCertificateAgainstCRL(selected, certificate, maxAge, now); err != nil {
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
