package controllers

import (
	"os"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"synapse-operator/internal/testenv"
)

// The local API server is shared by the tests that need one and started by
// the first of them: most tests in this package run on the fake client and
// should not pay for it.
var (
	apiServerOnce sync.Once
	apiServerCfg  *rest.Config
	apiServerStop func() error
	apiServerErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if apiServerStop != nil {
		_ = apiServerStop()
	}
	os.Exit(code)
}

func apiServer(t *testing.T) *rest.Config {
	t.Helper()
	apiServerOnce.Do(func() {
		apiServerCfg, apiServerStop, apiServerErr = testenv.Start()
	})
	if apiServerErr != nil {
		t.Fatal(apiServerErr)
	}
	return apiServerCfg
}

// eventually polls check until it returns "" or the deadline passes, and
// fails the test with the last reason it gave.
func eventually(t *testing.T, what string, check func() string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		reason := check()
		if reason == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s", what, reason)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
