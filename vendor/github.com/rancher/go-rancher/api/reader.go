package api

import "encoding/json"

func (a *ApiContext) Read(obj interface{}) error {
	decoder := json.NewDecoder(a.r.Body)
	return decoder.Decode(obj)
}
