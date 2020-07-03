package main

import (
	json "encoding/json"
)

func Normalize(data []byte) []byte {
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		// Invalid json, do nothing
		return data
	}
	marshaled, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return marshaled
}
