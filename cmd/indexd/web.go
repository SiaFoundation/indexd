package main

import (
	"net/http"

	"strings"
)

type webRouter struct {
	ui  http.Handler
	api http.Handler
}

func (wr webRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, "/api"):
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api") // strip the prefix
		wr.api.ServeHTTP(w, req)
	default:
		wr.ui.ServeHTTP(w, req)
	}
}
