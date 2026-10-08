package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// What the operator is granted to run the SynapseProxy controllers. Nothing
// generates this file and nothing in the code refers to it, so the tests here
// are what holds the two together. The controllers run as the ServiceAccount
// it binds, against a real API server. Holding the role, they must be refused
// nothing, and read everything it lets them read; with any one of its writes
// taken away, they must be refused something.
const (
	proxyRoleManifest    = "../config/proxy-controller/rbac.yaml"
	operatorRoleManifest = "../config/rbac.yaml"
)

var rbacTestRuns atomic.Int64

// readManifest returns the documents of a YAML file, by kind.
func readManifest(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	docs := map[string][]byte{}
	dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var doc runtime.RawExtension
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			return docs
		} else if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var kind metav1.TypeMeta
		if err := json.Unmarshal(doc.Raw, &kind); err != nil || kind.Kind == "" {
			continue
		}
		if docs[kind.Kind] != nil {
			t.Fatalf("%s holds more than one %s", path, kind.Kind)
		}
		docs[kind.Kind] = doc.Raw
	}
}

func loadProxyRole(t *testing.T) (*rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding) {
	t.Helper()
	docs := readManifest(t, proxyRoleManifest)
	role, binding := &rbacv1.ClusterRole{}, &rbacv1.ClusterRoleBinding{}
	for kind, into := range map[string]any{"ClusterRole": role, "ClusterRoleBinding": binding} {
		if docs[kind] == nil {
			t.Fatalf("%s holds no %s", proxyRoleManifest, kind)
		}
		if err := json.Unmarshal(docs[kind], into); err != nil {
			t.Fatalf("%s: %s: %v", proxyRoleManifest, kind, err)
		}
	}
	if len(docs) != 2 {
		t.Fatalf("%s holds more than the role and its binding", proxyRoleManifest)
	}
	return role, binding
}

// A grant is one thing a role allows: a verb on a resource.
//
// Reading is one grant, not three. An informer asks for list and watch, or
// for watch alone from a server that streams the list, so which of the two is
// used depends on the cluster; and whoever may list a resource can read every
// object of it anyway, so get adds nothing.
type grant struct{ group, resource, verb string }

const readGrant = "read"

var readVerbs = []string{"get", "list", "watch"}

func (g grant) String() string {
	if g.group == "" {
		return g.verb + " " + g.resource
	}
	return g.verb + " " + g.resource + "." + g.group
}

func (g grant) verbs() []string {
	if g.verb == readGrant {
		return readVerbs
	}
	return []string{g.verb}
}

// grantsOf flattens a role into its grants. It refuses what would make that
// lossy: wildcards, resource names, and get, list and watch not kept together.
func grantsOf(t *testing.T, role *rbacv1.ClusterRole) []grant {
	t.Helper()
	var out []grant
	add := func(g grant) {
		if slices.Contains(out, g) {
			t.Errorf("%s is granted twice", g)
		}
		out = append(out, g)
	}
	for _, rule := range role.Rules {
		if len(rule.ResourceNames) > 0 || len(rule.NonResourceURLs) > 0 {
			t.Fatalf("a rule with resourceNames or nonResourceURLs is not something these tests understand: %+v", rule)
		}
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				reads := 0
				for _, verb := range rule.Verbs {
					switch {
					case group == "*" || resource == "*" || verb == "*":
						t.Fatalf("a wildcard grants what nobody listed: %+v", rule)
					case slices.Contains(readVerbs, verb):
						reads++
					default:
						add(grant{group, resource, verb})
					}
				}
				switch reads {
				case 0:
				case len(readVerbs):
					add(grant{group, resource, readGrant})
				default:
					t.Errorf("%s in %q: get, list and watch go together or not at all", resource, group)
				}
			}
		}
	}
	return out
}

func roleOf(name string, grants []grant) *rbacv1.ClusterRole {
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}}
	for _, g := range grants {
		role.Rules = append(role.Rules, rbacv1.PolicyRule{
			APIGroups: []string{g.group}, Resources: []string{g.resource}, Verbs: g.verbs(),
		})
	}
	return role
}

func bindingOf(name string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: "synapse-os", Name: name}},
	}
}

// asked is what one client asked of the API server: the resources it listed
// or watched, and the requests it was refused, as "METHOD /path".
type asked struct {
	mu      sync.Mutex
	read    map[grant]bool
	refused []string
}

func (a *asked) record(req *http.Request, status int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case status == http.StatusForbidden:
		a.refused = append(a.refused, req.Method+" "+req.URL.Path)
	case status == http.StatusOK && req.Method == http.MethodGet:
		if group, resource, ok := collectionOf(req.URL.Path); ok {
			a.read[grant{group, resource, readGrant}] = true
		}
	}
}

func (a *asked) refusals() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.refused)
}

func (a *asked) didRead(g grant) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.read[g]
}

// collectionOf names the resource a path is the collection of, in one
// namespace or in all of them: what a list or a watch asks for.
func collectionOf(path string) (group, resource string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(parts) > 2 && parts[0] == "api":
		parts = parts[2:]
	case len(parts) > 3 && parts[0] == "apis":
		group, parts = parts[1], parts[3:]
	default:
		return "", "", false
	}
	if len(parts) == 3 && parts[0] == "namespaces" {
		parts = parts[2:]
	}
	return group, parts[0], len(parts) == 1
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func rbacAdmin(t *testing.T) client.Client {
	t.Helper()
	k8s, err := client.New(apiServer(t), client.Options{Scheme: proxyTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	return k8s
}

// operatorHolding returns a client configuration for the ServiceAccount the
// binding names, holding the role and nothing else, and the record of what it
// asks. The record sits in the transport, so it sees every request: the
// informers' and the event recorder's as well as the reconcilers'.
func operatorHolding(t *testing.T, admin client.Client, role *rbacv1.ClusterRole, binding *rbacv1.ClusterRoleBinding, withheld ...grant) (*rest.Config, *asked) {
	t.Helper()
	ctx := context.Background()
	for _, obj := range []client.Object{role.DeepCopy(), binding.DeepCopy()} {
		err := admin.Create(ctx, obj)
		if apierrors.IsAlreadyExists(err) {
			// A repeated run: the manifest's own names are not unique.
			existing := obj.DeepCopyObject().(client.Object)
			if err = admin.Get(ctx, client.ObjectKeyFromObject(obj), existing); err == nil {
				obj.SetResourceVersion(existing.GetResourceVersion())
				err = admin.Update(ctx, obj)
			}
		}
		if err != nil {
			t.Fatalf("%T %s: %v", obj, obj.GetName(), err)
		}
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != rbacv1.ServiceAccountKind {
		t.Fatalf("the binding is to %+v, want one ServiceAccount", binding.Subjects)
	}
	sa := binding.Subjects[0]
	user := "system:serviceaccount:" + sa.Namespace + ":" + sa.Name

	may := func(g grant) bool {
		resource, subresource, _ := strings.Cut(g.resource, "/")
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   user,
			Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + sa.Namespace, "system:authenticated"},
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group: g.group, Resource: resource, Subresource: subresource, Verb: g.verbs()[len(g.verbs())-1],
			},
		}}
		if err := admin.Create(ctx, review); err != nil {
			t.Fatal(err)
		}
		return review.Status.Allowed
	}
	// The API server learns of a role and its binding a moment after they
	// are written. A controller started before that is refused everything.
	held := grantsOf(t, role)
	eventually(t, "the role takes effect", func() string {
		for _, g := range held {
			if !may(g) {
				return user + " may not " + g.String()
			}
		}
		return ""
	})
	for _, g := range withheld {
		if may(g) {
			t.Fatalf("%s may %s without the role granting it", user, g)
		}
	}

	seen := &asked{read: map[grant]bool{}}
	cfg := rest.CopyConfig(apiServer(t))
	cfg.Impersonate = rest.ImpersonationConfig{UserName: user}
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTripper(func(req *http.Request) (*http.Response, error) {
			resp, err := next.RoundTrip(req)
			if err == nil {
				seen.record(req, resp.StatusCode)
			}
			return resp, err
		})
	})
	return cfg, seen
}

// runProxyControllers starts both of a SynapseProxy's controllers as whoever
// cfg acts as: on the whole cluster, as the operator runs, or on the one
// namespace given. The returned function stops them and gives the manager's
// error.
func runProxyControllers(t *testing.T, cfg *rest.Config, namespace ...string) (stop func() error) {
	t.Helper()
	options := ctrl.Options{
		Scheme:                 proxyTestScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr(true)},
	}
	for _, ns := range namespace {
		options.Cache.DefaultNamespaces = map[string]cache.Config{ns: {}}
	}
	mgr, err := ctrl.NewManager(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	// As the operator registers them, so that the role is tested against
	// what the operator runs.
	if err := SetupProxyControllers(mgr, "cluster.local"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	var once sync.Once
	var stopErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			stopErr = <-stopped
		})
		return stopErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// proxyStory takes one SynapseProxy through everything the two controllers
// do for it. The test plays the rest of the cluster: the user, the load
// balancer, the certificate issuer. Each step ends by waiting for what the
// controllers owe in return.
type proxyStory struct {
	t        *testing.T
	ctx      context.Context
	k8s      client.Client
	ns       string
	operator *asked
}

// wait polls until done returns "", and reports true; or until the operator
// has been refused something, and reports false.
func (s *proxyStory) wait(what string, done func() string) bool {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if len(s.operator.refusals()) > 0 {
			return false
		}
		reason := done()
		if reason == "" {
			return true
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%s: %s", what, reason)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *proxyStory) create(obj client.Object) {
	s.t.Helper()
	if err := s.k8s.Create(s.ctx, obj); err != nil {
		s.t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
	}
}

// events counts the events of one reason about one object, repeats included.
func (s *proxyStory) events(kind, name, reason string) int32 {
	s.t.Helper()
	var list corev1.EventList
	if err := s.k8s.List(s.ctx, &list, client.InNamespace(s.ns)); err != nil {
		s.t.Fatal(err)
	}
	var n int32
	for _, e := range list.Items {
		if e.InvolvedObject.Kind == kind && e.InvolvedObject.Name == name && (reason == "" || e.Reason == reason) {
			n += max(e.Count, 1)
		}
	}
	return n
}

// run reports whether the story reached its end. It stops early, with false,
// as soon as the operator is refused anything.
func (s *proxyStory) run() bool {
	s.t.Helper()
	key := client.ObjectKey{Namespace: s.ns, Name: "edge"}
	get := func(name string, into client.Object) string {
		if err := s.k8s.Get(s.ctx, client.ObjectKey{Namespace: s.ns, Name: name}, into); err != nil {
			return err.Error()
		}
		return ""
	}
	configSecret := func() (string, string) {
		var d appsv1.Deployment
		if reason := get("edge", &d); reason != "" {
			return "", reason
		}
		return volumeNamed(s.t, &d, "config").Secret.SecretName, ""
	}

	// A proxy with everything around it that the controllers read: the
	// Secret its API key is in, a class that binds an Ingress to it, that
	// Ingress's backend and its certificate.
	s.create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s.ns}})
	s.create(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "creds"},
		Data:       map[string][]byte{"API_KEY": []byte("k-1")},
	})
	s.create(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "app"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "app"},
			Ports:    []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}},
		},
	})
	tls := tlsSecret(s.ns, "shop-tls", "CRT", "KEY")
	s.create(tls)
	edge := &synapsev1alpha1.SynapseProxy{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.ns, Name: "edge"},
		Spec: synapsev1alpha1.SynapseProxySpec{
			Image:     "example.test/synapse:1.0.0",
			Listeners: []synapsev1alpha1.Listener{{Name: "http", Port: 80, Protocol: "HTTP"}},
			Service:   &synapsev1alpha1.ProxyServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			Platform: &synapsev1alpha1.PlatformSpec{
				APIKeySecretRef: &synapsev1alpha1.SecretKeyReference{Name: "creds", Key: "API_KEY"},
			},
		},
	}
	class := classFor(s.ns, edge)
	s.create(class)
	ing := routedIngress("shop", ptr(s.ns), "shop.example.com")
	ing.Namespace = s.ns
	ing.Spec.Rules[0].HTTP.Paths[0].PathType = ptr(networkingv1.PathTypePrefix)
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"shop.example.com"}, SecretName: "shop-tls"}}
	s.create(ing)
	s.create(edge)
	// The API server is shared, and every later test that runs these
	// controllers on the whole cluster would take this proxy on as well.
	s.t.Cleanup(func() {
		for _, obj := range []client.Object{edge, ing, class} {
			if err := s.k8s.Delete(s.ctx, obj); client.IgnoreNotFound(err) != nil {
				s.t.Errorf("delete %T %s: %v", obj, obj.GetName(), err)
			}
		}
	})

	first := ""
	if !s.wait("the proxy's objects are written, and both controllers have said so", func() string {
		var reason string
		if first, reason = configSecret(); reason != "" {
			return reason
		}
		var p synapsev1alpha1.SynapseProxy
		if reason := get("edge", &p); reason != "" {
			return reason
		}
		switch {
		case p.Status.ConfigHash == "":
			return "the proxy has no status yet"
		case s.events("SynapseProxy", "edge", "") == 0:
			return "no event about the proxy"
		case s.events("Ingress", "shop", "Programmed") == 0:
			return "no event about the Ingress"
		}
		return ""
	}) {
		return false
	}

	// The load balancer gives the proxy's Service an address.
	var svc corev1.Service
	if reason := get("edge", &svc); reason != "" {
		s.t.Fatal(reason)
	}
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}}
	if err := s.k8s.Status().Update(s.ctx, &svc); err != nil {
		s.t.Fatal(err)
	}
	if !s.wait("the address is on the proxy and on its Ingress", func() string {
		var p synapsev1alpha1.SynapseProxy
		if reason := get("edge", &p); reason != "" {
			return reason
		}
		var got networkingv1.Ingress
		if reason := get("shop", &got); reason != "" {
			return reason
		}
		switch {
		case !slices.Contains(p.Status.Addresses, "203.0.113.10"):
			return fmt.Sprintf("the proxy's addresses are %v", p.Status.Addresses)
		case len(got.Status.LoadBalancer.Ingress) != 1 || got.Status.LoadBalancer.Ingress[0].IP != "203.0.113.10":
			return fmt.Sprintf("the Ingress's addresses are %+v", got.Status.LoadBalancer.Ingress)
		}
		return ""
	}) {
		return false
	}

	// The user changes the spec: a new configuration, and the old one goes.
	if err := s.k8s.Get(s.ctx, key, edge); err != nil {
		s.t.Fatal(err)
	}
	edge.Spec.Logging = &synapsev1alpha1.LoggingSpec{Level: "debug"}
	if err := s.k8s.Update(s.ctx, edge); err != nil {
		s.t.Fatal(err)
	}
	if !s.wait("the pods get a new configuration and the old one is removed", func() string {
		now, reason := configSecret()
		switch {
		case reason != "":
			return reason
		case now == first:
			return "the pods still mount " + now
		}
		err := s.k8s.Get(s.ctx, client.ObjectKey{Namespace: s.ns, Name: first}, &corev1.Secret{})
		if !apierrors.IsNotFound(err) {
			return fmt.Sprintf("the old configuration %s is still there (%v)", first, err)
		}
		return ""
	}) {
		return false
	}

	// The Ingress changes without changing how many routes there are, so
	// the event about it is the one already raised, once more.
	if reason := get("shop", ing); reason != "" {
		s.t.Fatal(reason)
	}
	ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number = 8080
	if err := s.k8s.Update(s.ctx, ing); err != nil {
		s.t.Fatal(err)
	}
	if !s.wait("the changed Ingress is rendered, and reported once more", func() string {
		var cm corev1.ConfigMap
		if reason := get("edge-upstreams", &cm); reason != "" {
			return reason
		}
		switch {
		case !strings.Contains(cm.Data[UpstreamsKey], ":8080"):
			return "the routes are\n" + cm.Data[UpstreamsKey]
		case s.events("Ingress", "shop", "Programmed") < 2:
			return "the event about the Ingress was not repeated"
		}
		return ""
	}) {
		return false
	}

	// The certificate is renewed.
	if reason := get("shop-tls", tls); reason != "" {
		s.t.Fatal(reason)
	}
	tls.Data[corev1.TLSCertKey] = []byte("CRT-RENEWED")
	if err := s.k8s.Update(s.ctx, tls); err != nil {
		s.t.Fatal(err)
	}
	return s.wait("the renewed certificate reaches the proxy", func() string {
		var certs corev1.Secret
		if reason := get("edge-certs", &certs); reason != "" {
			return reason
		}
		if got := string(certs.Data["shop.example.com.crt"]); got != "CRT-RENEWED" {
			return fmt.Sprintf("the proxy's certificate is %q", got)
		}
		return ""
	})
}

func newProxyStory(t *testing.T, admin client.Client, operator *asked) *proxyStory {
	return &proxyStory{
		t: t, ctx: context.Background(), k8s: admin, operator: operator,
		ns: fmt.Sprintf("proxy-rbac-%d", rbacTestRuns.Add(1)),
	}
}

// The role is handed to the ServiceAccount the operator runs as.
func TestProxyRole_IsBoundToTheOperator(t *testing.T) {
	role, binding := loadProxyRole(t)
	var operator corev1.ServiceAccount
	if err := json.Unmarshal(readManifest(t, operatorRoleManifest)["ServiceAccount"], &operator); err != nil {
		t.Fatalf("%s: ServiceAccount: %v", operatorRoleManifest, err)
	}
	if got, want := binding.RoleRef, (rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name}); got != want {
		t.Errorf("the binding refers to %+v, want %+v", got, want)
	}
	want := []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: operator.Namespace, Name: operator.Name}}
	if !slices.Equal(binding.Subjects, want) {
		t.Errorf("the binding is to %+v, want the operator's ServiceAccount %+v", binding.Subjects, want)
	}
}

// Holding the role and nothing else, the operator is refused nothing; and
// there is nothing it may read that it does not.
func TestProxyRole_IsEnoughToRunAProxy(t *testing.T) {
	role, binding := loadProxyRole(t)
	admin := rbacAdmin(t)
	cfg, operator := operatorHolding(t, admin, role, binding)
	stop := runProxyControllers(t, cfg)

	finished := newProxyStory(t, admin, operator).run()
	if err := stop(); err != nil {
		t.Errorf("manager: %v", err)
	}
	if got := operator.refusals(); len(got) > 0 {
		t.Fatalf("the operator was refused:\n  %s", strings.Join(got, "\n  "))
	}
	if !finished {
		t.Fatal("the story stopped early")
	}
	for _, g := range grantsOf(t, role) {
		if g.verb == readGrant && !operator.didRead(g) {
			t.Errorf("the operator may %s and never did", g)
		}
	}
}

// Nothing the role lets the operator change is there for nothing: without
// any one such grant, it is refused something on the way through the same
// story.
//
// Reads are held to this in the test above, by what was asked for. That does
// not work here: a create through server-side apply is a PATCH on the wire,
// and the finalizers grant is checked by the API server on the operator's
// behalf. And withholding does not work there: a manager that cannot read a
// resource it indexes never finishes starting, and cannot be stopped.
func TestProxyRole_GrantsNoWriteItDoesNotUse(t *testing.T) {
	if testing.Short() {
		t.Skip("starts both controllers once for every grant")
	}
	role, _ := loadProxyRole(t)
	admin := rbacAdmin(t)
	grants := grantsOf(t, role)
	for i, without := range grants {
		if without.verb == readGrant {
			continue
		}
		t.Run(without.String(), func(t *testing.T) {
			name := fmt.Sprintf("proxy-role-%d", rbacTestRuns.Add(1))
			rest := slices.Delete(slices.Clone(grants), i, i+1)
			cfg, operator := operatorHolding(t, admin, roleOf(name, rest), bindingOf(name), without)
			// On the story's namespace only, so that what is refused is
			// refused for this proxy and not for one another test left.
			story := newProxyStory(t, admin, operator)
			stop := runProxyControllers(t, cfg, story.ns)
			finished := story.run()
			if err := stop(); err != nil {
				t.Errorf("manager: %v", err)
			}
			if finished {
				t.Errorf("the operator did everything it does for a proxy without being allowed to %s", without)
			}
			// With one grant gone, that grant is all it can be refused for.
			// The finalizers one is the exception: it is asked for when an
			// owned object is created, whichever object that is.
			resource, subresource, _ := strings.Cut(without.resource, "/")
			for _, refused := range operator.refusals() {
				t.Log("refused: " + refused)
				switch {
				case !strings.Contains(refused, "/namespaces/"+story.ns+"/"):
					t.Errorf("refused %s, which is not in %s", refused, story.ns)
				case subresource == "finalizers":
				case !strings.Contains(refused+"/", "/"+resource+"/") || !strings.HasSuffix(refused, subresource):
					t.Errorf("refused %s, which is not about %s", refused, without)
				}
			}
		})
	}
}
