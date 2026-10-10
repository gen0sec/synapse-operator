// Command backend is what the end-to-end test puts behind the proxy: an HTTP
// server that says which backend it is, what it was sent in the request
// headers the test has the proxy set and add to, and which request headers
// it was sent at all.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
)

func main() {
	name := os.Getenv("NAME")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", name)
		w.Header().Set("X-Seen-From-Gateway", r.Header.Get("X-From-Gateway"))
		// Every value, in the order they came: a header that was added to
		// has the client's own first.
		w.Header().Set("X-Seen-Added", strings.Join(r.Header.Values("X-Added"), "|"))
		// The names alone, in lower case: enough to tell what the proxy
		// put in on the way.
		names := make([]string, 0, len(r.Header))
		for name := range r.Header {
			names = append(names, strings.ToLower(name))
		}
		sort.Strings(names)
		w.Header().Set("X-Seen-Headers", strings.Join(names, ","))
		fmt.Fprintf(w, "backend=%s host=%s path=%s\n", name, r.Host, r.URL.Path)
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
