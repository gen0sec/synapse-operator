// Command backend is what the end-to-end test puts behind the proxy: an HTTP
// server that says which backend it is.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	name := os.Getenv("NAME")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", name)
		fmt.Fprintf(w, "backend=%s host=%s path=%s\n", name, r.Host, r.URL.Path)
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
