// Package identity holds shared, bounded identifier validation.
package identity

import "unicode/utf8"

const MaxModelComponentBytes = 256

func ValidModelComponent(value string) bool {
	return utf8.ValidString(value) && len(value) <= MaxModelComponentBytes
}

func ValidNodeID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
			c == '.' || c == '_' || c == '~' || c == '-') {
			return false
		}
	}
	return true
}
