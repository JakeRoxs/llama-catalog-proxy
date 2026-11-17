//go:build !linux

package modelconfig

import "time"

func CreateOwnerGrant(string, OwnerGrant) error    { return ErrOwnerGrantStore }
func ListOwnerGrants(string) ([]OwnerGrant, error) { return nil, ErrOwnerGrantStore }
func OwnerGrantAuthorizes(string, string, string, string, string, string) (bool, error) {
	return false, ErrOwnerGrantStore
}
func OwnerGrantAuthorizesAt(string, string, string, string, string, string, time.Time) (bool, error) {
	return false, ErrOwnerGrantStore
}
func RevokeOwnerGrant(string, string) error { return ErrOwnerGrantStore }
func ValidateOwnerGrantStore(string) error  { return ErrOwnerGrantStore }
