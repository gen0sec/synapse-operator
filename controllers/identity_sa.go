package controllers

import (
	"context"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
)

const (
	saGroupAll      = "system:serviceaccounts"
	saGroupNSPrefix = "system:serviceaccounts:"
)

// serviceAccountName is the pod's service account, "default" when unset, as
// Kubernetes admission fills it in.
func serviceAccountName(p *corev1.Pod) string {
	if p.Spec.ServiceAccountName == "" {
		return "default"
	}
	return p.Spec.ServiceAccountName
}

// saPrivilege answers whether a ServiceAccount holds wildcard or cluster-admin
// power through the cluster's RBAC bindings.
type saPrivilege struct {
	accounts   map[string]bool // "<ns>/<name>"
	namespaces map[string]bool // group system:serviceaccounts:<ns>
	all        bool            // group system:serviceaccounts
}

func (p saPrivilege) privileged(ns, name string) bool {
	return p.all || p.namespaces[ns] || p.accounts[ns+"/"+name]
}

// newSAPrivilege derives the privileged set. A binding counts when its role is
// cluster-admin or has a rule with a "*" verb or resource. A RoleBinding only
// counts for subjects in its own namespace, where it grants.
func newSAPrivilege(
	crbs []rbacv1.ClusterRoleBinding,
	rbs []rbacv1.RoleBinding,
	roles []rbacv1.Role,
	clusterRoles []rbacv1.ClusterRole,
) saPrivilege {
	p := saPrivilege{accounts: map[string]bool{}, namespaces: map[string]bool{}}

	clusterWild := map[string]bool{"cluster-admin": true}
	for _, cr := range clusterRoles {
		if rulesWildcard(cr.Rules) {
			clusterWild[cr.Name] = true
		}
	}
	roleWild := map[string]bool{} // "<ns>/<name>"
	for _, r := range roles {
		if rulesWildcard(r.Rules) {
			roleWild[r.Namespace+"/"+r.Name] = true
		}
	}

	add := func(subjects []rbacv1.Subject, bindingNS string) {
		for _, s := range subjects {
			switch s.Kind {
			case rbacv1.ServiceAccountKind:
				ns := s.Namespace
				if ns == "" {
					ns = bindingNS
				}
				if bindingNS != "" && ns != bindingNS {
					continue
				}
				p.accounts[ns+"/"+s.Name] = true
			case rbacv1.GroupKind:
				switch {
				case s.Name == saGroupAll:
					if bindingNS == "" {
						p.all = true
					} else {
						p.namespaces[bindingNS] = true
					}
				case strings.HasPrefix(s.Name, saGroupNSPrefix):
					ns := strings.TrimPrefix(s.Name, saGroupNSPrefix)
					if bindingNS == "" || bindingNS == ns {
						p.namespaces[ns] = true
					}
				}
			}
		}
	}

	for _, b := range crbs {
		if b.RoleRef.Kind == "ClusterRole" && clusterWild[b.RoleRef.Name] {
			add(b.Subjects, "")
		}
	}
	for _, b := range rbs {
		wild := false
		switch b.RoleRef.Kind {
		case "ClusterRole":
			wild = clusterWild[b.RoleRef.Name]
		case "Role":
			wild = roleWild[b.Namespace+"/"+b.RoleRef.Name]
		}
		if wild {
			add(b.Subjects, b.Namespace)
		}
	}
	return p
}

func rulesWildcard(rules []rbacv1.PolicyRule) bool {
	for _, r := range rules {
		if slices.Contains(r.Verbs, rbacv1.VerbAll) || slices.Contains(r.Resources, rbacv1.ResourceAll) {
			return true
		}
	}
	return false
}

// stampServiceAccountPrivilege sets saPrivileged on each entry from the
// current RBAC objects. A failed list leaves the flags false (logged), so a
// producer without the RBAC read grant still publishes identity.
func (r *IdentityProducerReconciler) stampServiceAccountPrivilege(ctx context.Context, entries []identityEntry) {
	var (
		crbs  rbacv1.ClusterRoleBindingList
		rbs   rbacv1.RoleBindingList
		roles rbacv1.RoleList
		crs   rbacv1.ClusterRoleList
	)
	for _, l := range []struct {
		name string
		list func() error
	}{
		{"clusterrolebindings", func() error { return r.List(ctx, &crbs) }},
		{"rolebindings", func() error { return r.List(ctx, &rbs) }},
		{"roles", func() error { return r.List(ctx, &roles) }},
		{"clusterroles", func() error { return r.List(ctx, &crs) }},
	} {
		if err := l.list(); err != nil {
			r.Log.Error(err, "identity-producer: list "+l.name+" failed (sa_privileged degraded to false)")
			return
		}
	}
	p := newSAPrivilege(crbs.Items, rbs.Items, roles.Items, crs.Items)
	for i := range entries {
		entries[i].saPrivileged = p.privileged(entries[i].namespace, entries[i].serviceAccount)
	}
}
