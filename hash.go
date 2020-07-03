package main

import sha3 "golang.org/x/crypto/sha3"

func Hash(data []byte) []byte {
	buf := make([]byte, 8)
	sha3.ShakeSum128(buf, data)
	return buf
}
