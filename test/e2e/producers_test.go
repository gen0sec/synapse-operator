//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	producersDeployment = "synapse-operator-producers"
	// Written out, as the other names a user sees: a rename is a change for
	// whoever watches these leases.
	controllerLease = "86a223f3.synapse.gen0sec.com"
	producersLease  = "producers.synapse.gen0sec.com"
)

// The default install ships the producers, and they come up without the
// API-key Secret: idle, not crash-looping, each Deployment on its own lease.
func TestProducersIdleWithoutKey(t *testing.T) {
	c := newCluster(t)
	cfg, err := clientcmd.BuildConfigFromFlags("", env(t, "E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 30 * time.Second
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = c.k8s.Get(c.ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "synapse-operator-api"}, &corev1.Secret{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("the API-key Secret is meant to be absent in this test, got %v", err)
	}

	eventually(t, soon, "the producers Deployment is available", func() string {
		var d appsv1.Deployment
		if err := c.k8s.Get(c.ctx, client.ObjectKey{Namespace: operatorNamespace, Name: producersDeployment}, &d); err != nil {
			return err.Error()
		}
		if d.Status.ObservedGeneration != d.Generation || d.Status.UpdatedReplicas < 1 || d.Status.AvailableReplicas != d.Status.Replicas {
			return fmt.Sprintf("the Deployment is at %+v", d.Status)
		}
		return ""
	})

	var pods corev1.PodList
	if err := c.k8s.List(c.ctx, &pods, client.InNamespace(operatorNamespace), client.MatchingLabels{
		"app.kubernetes.io/name": "synapse-operator", "app.kubernetes.io/component": "producers",
	}); err != nil || len(pods.Items) == 0 {
		t.Fatalf("the producers' pods: %d, %v", len(pods.Items), err)
	}
	for _, pod := range pods.Items {
		for _, st := range pod.Status.ContainerStatuses {
			if st.RestartCount != 0 {
				t.Errorf("%s restarted %d times", pod.Name, st.RestartCount)
			}
		}
		eventually(t, soon, "the producers say they are idle", func() string {
			raw, err := cs.CoreV1().Pods(operatorNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(c.ctx)
			if err != nil {
				return err.Error()
			}
			if got := strings.Count(string(raw), "no API key"); got != 2 {
				return fmt.Sprintf("%d lines about a missing API key, want one per producer", got)
			}
			return ""
		})
	}

	eventually(t, soon, "each Deployment holds its own lease", func() string {
		for _, name := range []string{controllerLease, producersLease} {
			var l coordinationv1.Lease
			if err := c.k8s.Get(c.ctx, client.ObjectKey{Namespace: operatorNamespace, Name: name}, &l); err != nil {
				return err.Error()
			}
			if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
				return name + " has no holder"
			}
		}
		return ""
	})
}
