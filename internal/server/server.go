package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer - Receive a string and return a pointer to a http.Server
func NewHTTPServer(addr string) *http.Server {

	httpserver := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpserver.handleProduce).Methods("POST")
	r.HandleFunc("/", httpserver.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}

}

// httpServer struct
type httpServer struct {
	Log *Log
}

// newHTTPServer - Return a pointer to a httpServer
func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// handleProce - HandlerFunc to response the producer
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{
		Offset: offset,
	}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleConsume - HandlerFunc to response the consumer
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{
		Record: record,
	}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

// ProduceRequest - Struct
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse - Struct
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest - Struct
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse - Struct
type ConsumeResponse struct {
	Record Record `json:"record"`
}
