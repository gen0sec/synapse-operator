//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// A sticky key Secret that a user created before the proxy has no owner
// reference, so an edit to it reaches the operator only through its own watch.
// Nothing else touches the proxy here, and the controller's resync is hours
// away, so a reaction within seconds can only come from that watch.
func TestAdoptedStickyKeyEditIsReconciled(t *testing.T) {
	const (
		name  = "adopted"
		react = 45 * time.Second
	)
	c := newCluster(t)
	eventually(t, soon, "the operator is available", c.operatorAvailable)
	c.clear(t)
	c.create(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}})

	secretName := name + "-sticky-key"
	c.create(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: proxyNamespace, Name: secretName},
		Data:       map[string][]byte{"sticky.key": bytes.Repeat([]byte("k"), 40)},
	})
	c.extraProxy(t, name)

	eventually(t, soon, "the proxy runs on the adopted key", func() string {
		p, err := c.named(proxyNamespace, name)
		if err != nil {
			return err.Error()
		}
		cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
		if cond == nil || cond.Reason == "StickyKeyUnusable" {
			return fmt.Sprintf("Ready is %+v", cond)
		}
		var d appsv1.Deployment
		if err := c.k8s.Get(c.ctx, types.NamespacedName{Namespace: proxyNamespace, Name: name}, &d); err != nil {
			return err.Error()
		}
		return ""
	})
	s, err := c.secret(proxyNamespace, secretName)
	if err != nil {
		t.Fatal(err)
	}
	if ref := metav1.GetControllerOf(s); ref != nil {
		t.Fatalf("the Secret was taken over by %+v, it should stay the user's", ref)
	}
	// Let the reconciles of the first rollout end.
	time.Sleep(3 * time.Second)

	setKey := func(key []byte) {
		t.Helper()
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			s, err := c.secret(proxyNamespace, secretName)
			if err != nil {
				return err
			}
			s.Data = map[string][]byte{"sticky.key": key}
			return c.k8s.Update(c.ctx, s)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	setKey([]byte("too short"))
	took := eventually(t, react, "an adopted key made too short is reported", func() string {
		return c.notReadyBecause(name, "StickyKeyUnusable")
	})
	t.Logf("the shortened key was reported after %s", took)

	setKey(bytes.Repeat([]byte("k"), 40))
	eventually(t, react, "an adopted key made valid again is no longer refused", func() string {
		p, err := c.named(proxyNamespace, name)
		if err != nil {
			return err.Error()
		}
		cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
		if cond == nil || cond.Reason == "StickyKeyUnusable" {
			return fmt.Sprintf("Ready is %+v", cond)
		}
		return ""
	})
}
