package controllers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
)

const stickyKeyName = "edge-sticky-key"

func (e *proxyEnv) stickySecret() (*corev1.Secret, bool) {
	e.t.Helper()
	var s corev1.Secret
	ok := e.get(stickyKeyName, &s)
	return &s, ok
}

// The key exists before the Deployment and before the routes: the routes file
// names its path, and a pod that starts without the volume would refuse it.
func TestStickyKey_IsCreatedBeforeTheDeployment(t *testing.T) {
	e := newProxyEnv(t)
	p := e.proxy()
	e.reconcile(p) // the routes are not rendered yet

	s, ok := e.stickySecret()
	if !ok {
		t.Fatal("no sticky key Secret after the first reconcile")
	}
	if !metav1.IsControlledBy(s, p) {
		t.Errorf("the Secret is not controlled by the proxy: %v", s.OwnerReferences)
	}
	if s.Type != corev1.SecretTypeOpaque {
		t.Errorf("type = %s, want Opaque", s.Type)
	}
	if len(s.Data["sticky.key"]) != 32 || len(s.Data) != 1 {
		t.Errorf("data = %d keys, sticky.key has %d bytes; want that one key, 32 bytes", len(s.Data), len(s.Data["sticky.key"]))
	}
	if s.Labels[proxyLabel] != "edge" || s.Labels[proxyPartLabel] != "sticky-key" {
		t.Errorf("labels = %v", s.Labels)
	}
	var d appsv1.Deployment
	if e.get("edge", &d) {
		t.Error("the Deployment exists although the routes were not rendered")
	}

	first := append([]byte(nil), s.Data["sticky.key"]...)
	e.renderRoutes(p)
	e.reconcile(p)
	e.reconcile(p)
	again, _ := e.stickySecret()
	if !bytes.Equal(again.Data["sticky.key"], first) {
		t.Error("the key changed on a later reconcile")
	}
	if got := volumeNamed(t, e.deployment(), "sticky-key").Secret; got == nil || got.SecretName != stickyKeyName {
		t.Errorf("the Deployment does not mount %s: %+v", stickyKeyName, got)
	}
}

func unownedKey(e *proxyEnv, n int) *corev1.Secret {
	data := bytes.Repeat([]byte{'k'}, n)
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: stickyKeyName, Labels: map[string]string{"mine": "yes"}},
		Data:       map[string][]byte{"sticky.key": data},
	}
	e.create(s)
	return s
}

// A key the user made, for instance to share it between two proxies, is used
// as it is: not rewritten, not claimed, not deleted with the proxy.
func TestStickyKey_AdoptsAnUnownedSecret(t *testing.T) {
	e := newProxyEnv(t)
	unownedKey(e, 40)
	p := e.running()

	s, _ := e.stickySecret()
	if !bytes.Equal(s.Data["sticky.key"], bytes.Repeat([]byte{'k'}, 40)) {
		t.Errorf("the key was rewritten: %d bytes", len(s.Data["sticky.key"]))
	}
	if len(s.OwnerReferences) != 0 || s.Labels["mine"] != "yes" {
		t.Errorf("the Secret was claimed or relabelled: %v %v", s.OwnerReferences, s.Labels)
	}
	if got := volumeNamed(t, e.deployment(), "sticky-key").Secret; got == nil || got.SecretName != stickyKeyName {
		t.Errorf("the Deployment does not mount the adopted Secret: %+v", got)
	}
	wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonRollingOut)
}

func TestStickyKey_RefusesAShortUnownedSecret(t *testing.T) {
	e := newProxyEnv(t)
	unownedKey(e, 10)
	p := e.proxy()
	e.renderRoutes(p)
	e.reconcile(p)

	c := wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonStickyKeyUnusable)
	if !strings.Contains(c.Message, stickyKeyName) || !strings.Contains(c.Message, "10") {
		t.Errorf("message does not name the Secret and its size: %s", c.Message)
	}
	s, _ := e.stickySecret()
	if !bytes.Equal(s.Data["sticky.key"], bytes.Repeat([]byte{'k'}, 10)) || len(s.OwnerReferences) != 0 {
		t.Errorf("the user's Secret was touched: %+v", s)
	}
	var d appsv1.Deployment
	if e.get("edge", &d) {
		t.Error("a Deployment was applied that mounts a key Synapse would refuse")
	}
}

func TestStickyKey_RewritesItsOwnShortSecret(t *testing.T) {
	e := newProxyEnv(t)
	events := record.NewFakeRecorder(16)
	e.proxies.Recorder = events
	p := e.running()

	s, _ := e.stickySecret()
	change(t, e.k8s, s, func() { s.Data["sticky.key"] = []byte("short") })
	e.reconcile(p)

	s, _ = e.stickySecret()
	if len(s.Data["sticky.key"]) != 32 || !metav1.IsControlledBy(s, p) {
		t.Errorf("not rewritten: %d bytes, owners %v", len(s.Data["sticky.key"]), s.OwnerReferences)
	}
	if got := eventsOf(events, "StickyKeyReplaced"); len(got) != 1 || !strings.HasPrefix(got[0], "Normal ") {
		t.Errorf("events = %v, want one Normal StickyKeyReplaced", got)
	}
}

func TestStickyKey_RefusesASecretOfAnotherOwner(t *testing.T) {
	e := newProxyEnv(t)
	theirs := unownedKey(e, 40)
	change(t, e.k8s, theirs, func() {
		theirs.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "uid-other", Controller: ptr(true),
		}}
	})
	p := e.proxy()
	e.renderRoutes(p)
	e.reconcile(p)

	c := wantCondition(t, p, ConditionReady, metav1.ConditionFalse, ReasonNameCollision)
	if !strings.Contains(c.Message, stickyKeyName) {
		t.Errorf("message does not say what is in the way: %s", c.Message)
	}
	var d appsv1.Deployment
	if e.get("edge", &d) {
		t.Error("the Deployment was applied beside a collision")
	}
}

// The config Secrets are pruned by a label selector; the key must not match.
func TestStickyKey_SurvivesConfigSecretPruning(t *testing.T) {
	e := newProxyEnv(t)
	p := e.running()

	if err := e.proxies.pruneConfigSecrets(e.ctx, p, "edge-config-not-the-current-one"); err != nil {
		t.Fatal(err)
	}
	if names := e.configSecrets(); len(names) != 0 {
		t.Fatalf("pruning left %v: the test does not prune", names)
	}
	if _, ok := e.stickySecret(); !ok {
		t.Error("pruning deleted the sticky key")
	}
}

func TestRenderUpstreamsV2_StickySaysTheSecretFile(t *testing.T) {
	m := newRenderModel()
	m.sticky = true
	m.stickySecretFile = "/etc/synapse/sticky/sticky.key"
	out := renderUpstreamsV2(m)
	doc := parseV2(t, out)
	if !doc.Proxy.StickySessions.Enabled || doc.Proxy.StickySessions.SecretFile != "/etc/synapse/sticky/sticky.key" {
		t.Errorf("sticky_sessions = %+v\n%s", doc.Proxy.StickySessions, out)
	}
	if strings.Contains(out, "\n    key:") {
		t.Errorf("the hash key proxy.sticky_sessions.key is not ours to write:\n%s", out)
	}
}

func TestRoutes_AStickyIngressNamesTheKeyFile(t *testing.T) {
	ing := annotated(ownerIngress("synapse-os", "shop", "shop.example.com", "/", metav1.Now().Time), "sticky-sessions", "true")
	routes, _, _ := renderOwned(t, ing)
	doc := parseV2(t, routes)
	if !doc.Proxy.StickySessions.Enabled || doc.Proxy.StickySessions.SecretFile != "/etc/synapse/sticky/sticky.key" {
		t.Errorf("sticky_sessions = %+v\n%s", doc.Proxy.StickySessions, routes)
	}
}

// Without a key file there is nothing sticky sessions could sign with, and
// Synapse refuses a file that turns them on without one.
func TestRender_TheOlderModesDropStickySessions(t *testing.T) {
	objs := olderModeObjects()
	for _, o := range objs {
		switch o := o.(type) {
		case *networkingv1.Ingress:
			if o.Name == "shop" {
				annotated(o, "server-alias", "www.shop.example.com", "sticky-sessions", "true")
			}
		case *gwv1.HTTPRoute:
			o.Annotations = map[string]string{"synapse.gen0sec.com/sticky-sessions": "true"}
		}
	}
	events := record.NewFakeRecorder(64)
	routes := renderOlderModeWith(t, objs, events)
	if strings.Contains(routes, "sticky_sessions") {
		t.Errorf("sticky_sessions is written without a key file:\n%s", routes)
	}
	got := eventsOf(events, "StickySessionsUnavailable")
	if len(got) != 2 {
		t.Fatalf("events = %v, want one for the Ingress and one for the HTTPRoute", got)
	}
	for _, g := range got {
		if !strings.HasPrefix(g, "Warning ") || !strings.Contains(g, "signing key file") {
			t.Errorf("event = %q", g)
		}
	}
}

// The key is a secret. It lives in its own Secret and nowhere else: not in
// the config Secret, the routes, an event or a log line.
// This passes at once; it pins the invariant against a later change that logs
// or renders the Secret.
func TestStickyKey_NeverLeaves(t *testing.T) {
	e := newProxyEnv(t)
	var logs bytes.Buffer
	log := funcr.New(func(prefix, args string) { fmt.Fprintln(&logs, prefix, args) }, funcr.Options{Verbosity: 10})
	e.ctx = logr.NewContext(context.Background(), log)
	events := record.NewFakeRecorder(64)
	e.proxies.Recorder = events
	e.routes.Recorder = events

	p := e.running()
	s, _ := e.stickySecret()
	// Once more, with the key in place, and once after it is rewritten.
	e.renderRoutes(p)
	e.reconcile(p)
	key := append([]byte(nil), s.Data["sticky.key"]...)
	change(t, e.k8s, s, func() { s.Data["sticky.key"] = []byte("short") })
	e.reconcile(p)
	s, _ = e.stickySecret()
	newKey := s.Data["sticky.key"]

	var where []string
	scan := func(what string, data []byte) {
		for _, k := range [][]byte{key, newKey} {
			for _, form := range [][]byte{k, []byte(base64.StdEncoding.EncodeToString(k)),
				[]byte(base64.RawURLEncoding.EncodeToString(k)), []byte(hex.EncodeToString(k))} {
				if bytes.Contains(data, form) {
					where = append(where, what)
				}
			}
		}
	}
	var secrets corev1.SecretList
	var maps corev1.ConfigMapList
	if err := e.k8s.List(e.ctx, &secrets, client.InNamespace(e.ns)); err != nil {
		t.Fatal(err)
	}
	if err := e.k8s.List(e.ctx, &maps, client.InNamespace(e.ns)); err != nil {
		t.Fatal(err)
	}
	for _, o := range secrets.Items {
		if o.Name == stickyKeyName {
			continue
		}
		for k, v := range o.Data {
			scan("Secret "+o.Name+"/"+k, v)
		}
	}
	for _, o := range maps.Items {
		for k, v := range o.Data {
			scan("ConfigMap "+o.Name+"/"+k, []byte(v))
		}
	}
	scan("log", logs.Bytes())
	for len(events.Events) > 0 {
		scan("event", []byte(<-events.Events))
	}
	if len(where) > 0 {
		t.Errorf("the sticky key appears in: %v", where)
	}
	if logs.Len() == 0 {
		t.Log("nothing was logged; the log check had nothing to catch")
	}
}
