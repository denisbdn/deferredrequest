package jsonout

import (
	"encoding/json"
	"net/http"
)

const (
	Ok = iota
	BadParam
	NotFound
	ServerError
)

type Response struct {
	Errno int         `json:"errno"`
	Error string      `json:"error,omitempty"`
	Data  interface{} `json:"data,omitempty"`
}

func (res Response) PrintJson(w http.ResponseWriter) {
	js, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(js)
}
