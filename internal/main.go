package main

import (
	"fmt"
	"log"

	"github.com/go.distributed/internal/server"
)

func main() {
	server := server.NewHTTPServer(":8080")
	fmt.Println("Running server...")
	log.Fatal(server.ListenAndServe())
}
