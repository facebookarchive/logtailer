package sshd

import (
	"crypto/md5"
	"fmt"

	"golang.org/x/crypto/ssh"
)

// AuthorizedKey represents an ssh public key
type AuthorizedKey struct {
	ssh.PublicKey
	Comment string
}

// ParseAuthorizedKey attempts to parse an ssh public key
func ParseAuthorizedKey(in []byte) (*AuthorizedKey, error) {
	key, comment, _, _, err := ssh.ParseAuthorizedKey(in)
	if err != nil {
		return nil, err
	}
	return &AuthorizedKey{key, comment}, nil
}

// Fingerprint implements the RFC4716 key fingerprint for ssh keys
func (k *AuthorizedKey) Fingerprint() []byte {
	result := make([]byte, 0, 48)
	for i, octet := range md5.Sum(k.Marshal()) {
		if i != 0 {
			result = append(result, ':')
		}
		result = append(result, []byte(fmt.Sprintf("%02x", octet))...)
	}
	return result
}
