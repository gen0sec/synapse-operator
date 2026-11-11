//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
)

// spec.config may not set proxy.enabled: the operator always runs a
// SynapseProxy as a proxy. The refusal shows on Ready, and goes once the key
// does, which pins that this key was its cause.
func TestProxyEnabledInSpecConfigIsRefused(t *testing.T) {
	const name = "owned-key"
	c := newCluster(t)
	eventually(t, soon, "the operator is available", c.operatorAvailable)
	c.clear(t)
	c.create(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: proxyNamespace}})
	c.extraProxy(t, name)
	eventually(t, soon, "the proxy without the key is not refused", func() string {
		return c.notRefusedForConfig(name)
	})

	setConfig := func(raw string) {
		t.Helper()
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			p, err := c.named(proxyNamespace, name)
			if err != nil {
				return err
			}
			if raw == "" {
				p.Spec.Config = nil
			} else {
				p.Spec.Config = &runtime.RawExtension{Raw: []byte(raw)}
			}
			return c.k8s.Update(c.ctx, p)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	setConfig(`{"proxy": {"enabled": false}}`)
	eventually(t, soon, "proxy.enabled in spec.config is refused", func() string {
		if msg := c.notReadyBecause(name, "ConfigInvalid"); msg != "" {
			return msg
		}
		p, err := c.named(proxyNamespace, name)
		if err != nil {
			return err.Error()
		}
		cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
		if !strings.Contains(cond.Message, "spec.config.proxy.enabled") || !strings.Contains(cond.Message, "Forbidden") {
			return fmt.Sprintf("the message does not name the key: %q", cond.Message)
		}
		return ""
	})

	setConfig("")
	eventually(t, soon, "the refusal clears once the key is removed", func() string {
		return c.notRefusedForConfig(name)
	})
}

// notRefusedForConfig reports "" when Ready is not False for ConfigInvalid
// and the status is of the current spec.
func (c *cluster) notRefusedForConfig(name string) string {
	p, err := c.named(proxyNamespace, name)
	if err != nil {
		return err.Error()
	}
	cond := meta.FindStatusCondition(p.Status.Conditions, conditionReady)
	switch {
	case cond == nil:
		return "no Ready condition"
	case cond.ObservedGeneration != p.Generation:
		return "status of an earlier spec"
	case cond.Reason == "ConfigInvalid":
		return fmt.Sprintf("still refused: %s", cond.Message)
	}
	return ""
}
