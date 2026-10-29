// Command backend is what the end-to-end test puts behind the proxy: an HTTP
// server that says which backend it is, what it was sent in the request
// headers the test has the proxy set and add to, and which request headers
// it was sent at all. With SERVE_DIR set it also serves the files of that
// directory under /file/, which is how the test reads a file of a pod whose
// images have no shell. PORT, 8080 unless set, is the port it listens on.
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
	if dir := os.Getenv("SERVE_DIR"); dir != "" {
		http.Handle("/file/", http.StripPrefix("/file/", http.FileServer(http.Dir(dir))))
	}
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
