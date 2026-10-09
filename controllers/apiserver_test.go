package controllers

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// change reads obj, applies mutate to it and writes it back; changeStatus
// does the same for its status. Both do it again when the object was written
// in between. A controller that is running writes to the objects a test
// edits, at moments of its own choosing: a read followed by an update loses
// to it now and then.
func change(t *testing.T, k8s client.Client, obj client.Object, mutate func()) {
	t.Helper()
	rewrite(t, k8s, obj, mutate, func(ctx context.Context) error { return k8s.Update(ctx, obj) })
}

func changeStatus(t *testing.T, k8s client.Client, obj client.Object, mutate func()) {
	t.Helper()
	rewrite(t, k8s, obj, mutate, func(ctx context.Context) error { return k8s.Status().Update(ctx, obj) })
}

func rewrite(t *testing.T, k8s client.Client, obj client.Object, mutate func(), write func(context.Context) error) {
	t.Helper()
	ctx := context.Background()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		mutate()
		return write(ctx)
	})
	if err != nil {
		t.Fatalf("update %T %s: %v", obj, obj.GetName(), err)
	}
}
