// Command backend is what the end-to-end test puts behind the proxy: an HTTP
// server that says which backend it is, and what it was sent in the one
// request header the test has the proxy set.
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
		w.Header().Set("X-Seen-From-Gateway", r.Header.Get("X-From-Gateway"))
		fmt.Fprintf(w, "backend=%s host=%s path=%s\n", name, r.Host, r.URL.Path)
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
