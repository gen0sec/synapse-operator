package controllers

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
)

// LeaderGate is the shared-status write gate. With >1 proxy replica
// each pod still renders its OWN upstreams.yaml and SIGHUPs its OWN
// synapse (never gated); only the lease holder writes shared cluster
// status (GatewayClass/Gateway/HTTPRoute status, Ingress
// .status.loadBalancer) so the replicas don't churn those objects.
type LeaderGate struct{ flag atomic.Bool }

// IsLeader is the func plugged into IngressReconciler.IsLeader.
func (g *LeaderGate) IsLeader() bool { return g.flag.Load() }

// leaderRunnable contends for a Lease and keeps LeaderGate in sync.
// It runs on EVERY replica (NeedLeaderElection=false) — it is the
// shared-status election, independent of the manager's own
// --leader-elect (which would otherwise stop non-leaders rendering).
type leaderRunnable struct {
	cfg          *rest.Config
	ns, name, id string
	gate         *LeaderGate
}

// NewStatusLeaderElection wires a Lease-based election for shared
// status writes. ns/id default to "default"/hostname when empty.
func NewStatusLeaderElection(cfg *rest.Config, ns, name, id string, gate *LeaderGate) ctrlManagerRunnable {
	if ns == "" {
		ns = "default"
	}
	if id == "" {
		id, _ = os.Hostname()
	}
	return &leaderRunnable{cfg: cfg, ns: ns, name: name, id: id, gate: gate}
}

// NeedLeaderElection=false ⇒ controller-runtime starts this Runnable
// on all replicas even when the manager's own leader election is on.
func (lr *leaderRunnable) NeedLeaderElection() bool { return false }

func (lr *leaderRunnable) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("status-leader")
	cs, err := kubernetes.NewForConfig(lr.cfg)
	if err != nil {
		return err
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: lr.name, Namespace: lr.ns},
		Client:     cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: lr.id},
	}
	log.Info("contending for shared-status lease", "lease", lr.ns+"/"+lr.name, "identity", lr.id)
	for ctx.Err() == nil {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					lr.gate.flag.Store(true)
					log.Info("acquired shared-status leadership; this replica writes status")
				},
				OnStoppedLeading: func() {
					lr.gate.flag.Store(false)
					log.Info("lost shared-status leadership; status writes now deferred to the new leader")
				},
			},
		})
		// RunOrDie returns when leadership is lost or ctx is done;
		// loop to re-contend so a former leader can reacquire.
		lr.gate.flag.Store(false)
	}
	return nil
}
