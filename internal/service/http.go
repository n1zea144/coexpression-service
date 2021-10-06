package service

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/molecular-profiles/co-expressions/fetch", httpsrv.handleConsume).Methods("POST")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	CoExpressionService *CoExpressionService
}

func newHTTPServer() *httpServer {
	return &httpServer{
		CoExpressionService: NewCoExpressionService(),
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ServiceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req.MolecularProfileA = r.URL.Query().Get("molecularProfileA")
	req.MolecularProfileB = r.URL.Query().Get("molecularProfileB")
	var threshold float64
	if threshold, err = strconv.ParseFloat(r.URL.Query().Get("threshold"), 32); err != nil {
		threshold = 2.0
	}
	req.Threshold = threshold
	err = s.CoExpressionService.fetchCoExpressions(w, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
