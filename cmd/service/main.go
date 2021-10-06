package main

import (
	"github.com/n1zea144/coexpression-service/internal/service"
	"log"
)

func main() {
	srv := service.NewHTTPServer(":1970")
	log.Fatal(srv.ListenAndServe())
}
