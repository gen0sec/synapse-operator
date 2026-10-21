package controllers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// hostClaimsLabel marks a ConfigMap as a claim ledger written by this
// operator, and hostClaimsKey is the key its body is under.
const (
	hostClaimsLabel = "synapse.gen0sec.com/host-claims"
	hostClaimsKey   = "claims"
)

func (r *IngressReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

// loadLedger reads the ledger live. A ledger that is not there is empty, and
// says so. One that is there but not controlled by OwnerRef is somebody
// else's, and no age is taken from it.
func (r *IngressReconciler) loadLedger(ctx context.Context) (*corev1.ConfigMap, *hostClaims, error) {
	var reader client.Reader = r.Client
	if r.LedgerReader != nil {
		reader = r.LedgerReader
	}
	cm := &corev1.ConfigMap{}
	err := reader.Get(ctx, r.HostClaims, cm)
	switch {
	case apierrors.IsNotFound(err):
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.HostClaims.Namespace, Name: r.HostClaims.Name}}
		_ = r.claimOutput(cm) // new: only sets the controller
		ledger, _ := parseHostClaims("", false)
		return cm, ledger, nil
	case err != nil:
		return nil, nil, fmt.Errorf("read host claim ledger %s: %w", r.HostClaims, err)
	}
	if err := r.claimOutput(cm); err != nil {
		return nil, nil, fmt.Errorf("host claim ledger: %w", err)
	}
	ledger, malformed := parseHostClaims(cm.Data[hostClaimsKey], true)
	if malformed > 0 {
		mHostClaimLedgerErrors.WithLabelValues("malformed").Add(float64(malformed))
	}
	return cm, ledger, nil
}

// ageClaims replaces the creation time of each claim with when the ledger
// first saw it, and records the claims the ledger has not seen. Without a
// ledger named, claims are returned as they are.
//
// A ledger that cannot be read is an error: aged by anything else, a claim
// could be older than it is. One that cannot be written is not: the claims
// are aged by the ledger as it would have been, *notRecorded is set, and the
// next render tries again.
func (r *IngressReconciler) ageClaims(ctx context.Context, claims []hostClaim, notRecorded *bool) ([]hostClaim, error) {
	if r.HostClaims.Name == "" {
		return claims, nil
	}
	var (
		cm      *corev1.ConfigMap
		ledger  *hostClaims
		dropped int
		err     error
	)
	if !r.RecordClaims {
		if cm, ledger, err = r.loadLedger(ctx); err != nil {
			return nil, err
		}
		return ledger.age(claims, ledger.now(r.now())), nil
	}
	if len(claims) == 0 {
		// Nothing to age or record. A ledger that is not there has nothing to
		// mark absent, so a proxy with no claims makes no live read on every
		// render. A stale answer only delays the marking to a later render.
		var cm corev1.ConfigMap
		if apierrors.IsNotFound(r.Get(ctx, r.HostClaims, &cm)) {
			return claims, nil
		}
	}
	var readErr error
	werr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if cm, ledger, readErr = r.loadLedger(ctx); readErr != nil {
			return nil
		}
		var changed bool
		changed, dropped = ledger.record(claims, ledger.now(r.now()))
		if !changed {
			return nil
		}
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels[hostClaimsLabel] = "true"
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[hostClaimsKey] = ledger.body()
		if cm.ResourceVersion != "" {
			return r.Update(ctx, cm)
		}
		if err := r.Create(ctx, cm); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Written since it was read: read it again.
				return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, cm.Name, err)
			}
			return err
		}
		return nil
	})
	if readErr != nil {
		return nil, readErr
	}
	if werr != nil {
		ctrl.LoggerFrom(ctx).WithName("ingress").Error(werr, "host claim ledger not written", "ledger", r.HostClaims.String())
		mHostClaimLedgerErrors.WithLabelValues("write").Inc()
		*notRecorded = true
	}
	if dropped > 0 {
		mHostClaimLedgerErrors.WithLabelValues("full").Add(float64(dropped))
		r.emit(cm, corev1.EventTypeWarning, "HostClaimLedgerFull",
			"the host claim ledger is full: %d of the newest claims were dropped and are aged afresh", dropped)
	}
	return ledger.age(claims, ledger.now(r.now())), nil
}
