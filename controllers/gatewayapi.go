package controllers

import (
	"cmp"
	"fmt"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// Gateway API for a SynapseProxy.
//
// A GatewayClass of ours whose parametersRef names a SynapseProxy hands its
// Gateways to that proxy, as an IngressClass hands over its Ingresses. The
// HTTPRoutes attached to those Gateways become routes of the proxy.
//
// The rule throughout: what is programmed is what was asked for, and what
// cannot be is said in the object's status. Nothing is served more broadly
// than written, and nothing is dropped without a condition saying so. Synapse
// routes on host, path and method; a match on a header or a query parameter,
// and any filter but setting a header, has nowhere to go yet.
//
// What cannot be done is one of two things; see rules. A rule that cannot be
// carried out keeps its requests, which are then answered with an error. A
// match that cannot be evaluated is left out.
//
// These follow from how Synapse routes, and are not per the API:
//   - Routes belong to a host, not to a listener. One attached to a single
//     listener of a Gateway is served on every listener of the proxy.
//   - There is no host that stands for every other. A route has to end up
//     with a host name, its own or its listener's.
//   - A host is written as plain paths or as expressions, and what it can
//     carry depends on which; see emitGatewayMatches.

// gatewayInputs is what one translation reads: everything of the kinds
// involved, as listed, and lookups for what is referred to by name.
type gatewayInputs struct {
	proxy    *synapsev1alpha1.SynapseProxy
	classes  []gwv1.GatewayClass
	gateways []gwv1.Gateway
	routes   []gwv1.HTTPRoute
	grants   []gwv1beta1.ReferenceGrant
	// namespaces maps each namespace to its labels, for the listeners that
	// choose by them which namespaces may attach routes.
	namespaces map[string]map[string]string
	// service and secret return nil for what does not exist.
	service func(namespace, name string) *corev1.Service
	secret  func(namespace, name string) *corev1.Secret
	// addresses is where the proxy is reached.
	addresses     []string
	clusterDomain string
	// live holds every SynapseProxy there is, and scope the namespace the
	// operator is limited to, or "". The translation reads neither; they
	// are for telling what of ours nothing serves any more.
	live  map[types.NamespacedName]bool
	scope string
}

// gatewayStatuses is what the translation found out about each object, in
// the shape its status takes.
type gatewayStatuses struct {
	classes  map[string][]metav1.Condition
	gateways map[types.NamespacedName]gwv1.GatewayStatus
	// routes holds, per route, the status of the parents that are ours. A
	// route none of whose parents is ours is not in it.
	routes map[types.NamespacedName][]gwv1.RouteParentStatus
}

const (
	// reasonHostnameConflict is ours: the API has no reason for a host that
	// something of another kind already routes.
	reasonHostnameConflict = "HostnameConflict"

	gatewayGroup = gwv1.GroupName
)

// boundGatewayClasses returns the names of the GatewayClasses that hand
// their Gateways to proxy.
func boundGatewayClasses(classes []gwv1.GatewayClass, proxy *synapsev1alpha1.SynapseProxy) map[string]bool {
	bound := map[string]bool{}
	for i := range classes {
		if named, ok := classProxy(&classes[i]); ok && named == client.ObjectKeyFromObject(proxy) {
			bound[classes[i].Name] = true
		}
	}
	return bound
}

// classProxy returns the SynapseProxy a GatewayClass hands its Gateways to,
// and false for a class that is not ours or names none.
func classProxy(gc *gwv1.GatewayClass) (types.NamespacedName, bool) {
	p := gc.Spec.ParametersRef
	if string(gc.Spec.ControllerName) != ControllerName || p == nil {
		return types.NamespacedName{}, false
	}
	if string(p.Group) != synapsev1alpha1.GroupVersion.Group || string(p.Kind) != "SynapseProxy" {
		return types.NamespacedName{}, false
	}
	// A SynapseProxy is namespaced; a reference without a namespace names
	// none.
	if p.Namespace == nil {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: string(*p.Namespace), Name: p.Name}, true
}

// unserved returns what of ours no proxy serves: the classes that name a
// SynapseProxy that is not there, each with the name it gives, and the
// Gateways of those classes. Their status was written while a proxy served
// them, and nothing else will take it back.
//
// An operator limited to a namespace (scope) sees the proxies of that one
// only. A proxy it does not see elsewhere is not one that is gone.
func unserved(classes []gwv1.GatewayClass, gateways []gwv1.Gateway, live map[types.NamespacedName]bool, scope string) (map[string]types.NamespacedName, map[types.NamespacedName]bool) {
	orphanClasses := map[string]types.NamespacedName{}
	for i := range classes {
		named, ok := classProxy(&classes[i])
		if ok && !live[named] && (scope == "" || named.Namespace == scope) {
			orphanClasses[classes[i].Name] = named
		}
	}
	orphanGateways := map[types.NamespacedName]bool{}
	for i := range gateways {
		if _, orphan := orphanClasses[string(gateways[i].Spec.GatewayClassName)]; orphan {
			orphanGateways[client.ObjectKeyFromObject(&gateways[i])] = true
		}
	}
	return orphanClasses, orphanGateways
}

func condition(kind string, ok bool, reason, message string, generation int64) metav1.Condition {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{Type: kind, Status: status, Reason: reason, Message: message, ObservedGeneration: generation}
}

// gwListener is a Gateway's listener as far as the proxy can serve it.
type gwListener struct {
	spec *gwv1.Listener
	// usable says the proxy listens there and what it needs is in place.
	usable   bool
	hostname string
	attached int32
}

// gwMatch is one way a request can match one rule, on one host.
type gwMatch struct {
	host  string
	kind  gwv1.PathMatchType
	value string
	// regex is a regular expression's value as it is written for Synapse.
	regex  string
	method string

	// refused says the rule cannot be carried out. The match takes its
	// place among the host's, and no route is written for it.
	refused     bool
	servers     []backend
	reqHeaders  []string
	respHeaders []string
	// what names the match in a message: "rule 0, match 1".
	what string

	// For telling apart two matches that are equally specific: the older
	// route wins, then the one that sorts first by name, then the order
	// they are written in.
	created  metav1.Time
	route    types.NamespacedName
	position int
}

// translateGateways adds to m the routes and certificates of the Gateways
// handed to in.proxy, and returns the status of every object it looked at.
// It is a function of its inputs: the order they are listed in does not
// change the result.
func translateGateways(in gatewayInputs, m *renderModel) gatewayStatuses {
	out := gatewayStatuses{
		classes:  map[string][]metav1.Condition{},
		gateways: map[types.NamespacedName]gwv1.GatewayStatus{},
		routes:   map[types.NamespacedName][]gwv1.RouteParentStatus{},
	}
	bound := boundGatewayClasses(in.classes, in.proxy)
	for i := range in.classes {
		if gc := &in.classes[i]; bound[gc.Name] {
			out.classes[gc.Name] = []metav1.Condition{condition(string(gwv1.GatewayClassConditionStatusAccepted), true,
				string(gwv1.GatewayClassReasonAccepted), "served by SynapseProxy "+in.proxy.Namespace+"/"+in.proxy.Name, gc.Generation)}
		}
	}

	// Hosts something else routes already: Ingresses are rendered first.
	taken := map[string]bool{}
	for host := range m.hosts {
		taken[host] = true
	}
	for host := range m.passthroughHosts {
		taken[host] = true
	}

	listeners := map[types.NamespacedName][]*gwListener{}
	gateways := map[types.NamespacedName]*gwv1.Gateway{}
	for i := range in.gateways {
		gw := &in.gateways[i]
		if !bound[string(gw.Spec.GatewayClassName)] {
			continue
		}
		key := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		gateways[key] = gw
		for j := range gw.Spec.Listeners {
			listeners[key] = append(listeners[key], &gwListener{spec: &gw.Spec.Listeners[j]})
		}
	}

	// Listener status is filled in after the routes, which is when it is
	// known how many attached; whether a listener is usable is needed first.
	listenerConditions := map[*gwListener][]metav1.Condition{}
	for _, key := range sortedKeys(gateways) {
		gw := gateways[key]
		_, refusal := gatewayProblem(gw)
		for _, l := range listeners[key] {
			l.usable, listenerConditions[l] = in.listener(gw, l, m, refusal)
			if l.spec.Hostname != nil {
				l.hostname = strings.ToLower(string(*l.spec.Hostname))
			}
		}
	}

	// What a route's rules come to is known once every route's matches on
	// a host are seen together, so the routes are gone through twice.
	type attached struct {
		rt        *gwv1.HTTPRoute
		parents   []gwv1.RouteParentStatus
		accepted  []int
		hosts     int
		conflicts []string
		partial   []string
		rules     ruleSet
	}
	var all []attached
	// In any order: the matches are put in theirs when they are emitted.
	var matches []gwMatch
	for i := range in.routes {
		rt := &in.routes[i]
		var parents []gwv1.RouteParentStatus
		hosts := map[string]bool{}
		var accepted []int
		var partial []string
		for _, ref := range rt.Spec.ParentRefs {
			gwKey, ok := parentGateway(rt, ref)
			if !ok || gateways[gwKey] == nil {
				continue
			}
			attach := in.attach(rt, ref, listeners[gwKey])
			for host := range attach.hosts {
				hosts[host] = true
			}
			if attach.reason == "" {
				accepted = append(accepted, len(parents))
			}
			if attach.note != "" && !slices.Contains(partial, attach.note) {
				partial = append(partial, attach.note)
			}
			parents = append(parents, gwv1.RouteParentStatus{
				ParentRef:      ref,
				ControllerName: gwv1.GatewayController(ControllerName),
				Conditions: []metav1.Condition{condition(string(gwv1.RouteConditionAccepted), attach.reason == "",
					cmp.Or(attach.reason, string(gwv1.RouteReasonAccepted)), cmp.Or(attach.message, "attached"), rt.Generation)},
			})
		}
		if len(parents) == 0 {
			continue
		}

		// Hosts an Ingress already routes are not this route's to take.
		var conflicts []string
		for _, host := range sortedKeys(hosts) {
			if taken[host] {
				conflicts = append(conflicts, host)
				delete(hosts, host)
			}
		}
		rules := in.rules(rt, sortedKeys(hosts))
		matches = append(matches, rules.matches...)
		all = append(all, attached{rt: rt, parents: parents, accepted: accepted, hosts: len(hosts), conflicts: conflicts, partial: partial, rules: rules})
	}
	done := emitGatewayMatches(m, matches)

	for _, a := range all {
		rt, parents, rules := a.rt, a.parents, a.rules
		key := types.NamespacedName{Namespace: rt.Namespace, Name: rt.Name}
		notes := slices.Concat(rules.unsupported, done.notes[key], a.partial)
		for _, n := range a.accepted {
			p := &parents[n]
			switch {
			case a.hosts == 0:
				p.Conditions[0] = condition(string(gwv1.RouteConditionAccepted), false, reasonHostnameConflict,
					"already routed by an Ingress: "+strings.Join(a.conflicts, ", "), rt.Generation)
			case done.served[key] == 0 && len(notes) > 0:
				p.Conditions[0] = condition(string(gwv1.RouteConditionAccepted), false, string(gwv1.RouteReasonUnsupportedValue),
					strings.Join(notes, "; "), rt.Generation)
			case len(notes) > 0 || len(a.conflicts) > 0:
				said := notes
				if len(a.conflicts) > 0 {
					said = append(slices.Clone(said), "already routed by an Ingress: "+strings.Join(a.conflicts, ", "))
				}
				p.Conditions = append(p.Conditions, condition(string(gwv1.RouteConditionPartiallyInvalid), true,
					string(gwv1.RouteReasonUnsupportedValue), "not programmed: "+strings.Join(said, "; "), rt.Generation))
			}
		}
		for n := range parents {
			resolved := condition(string(gwv1.RouteConditionResolvedRefs), rules.refReason == "",
				cmp.Or(rules.refReason, string(gwv1.RouteReasonResolvedRefs)), cmp.Or(rules.refMessage, "every backend was found"), rt.Generation)
			parents[n].Conditions = append(parents[n].Conditions, resolved)
		}
		out.routes[key] = parents
	}

	for _, key := range sortedKeys(gateways) {
		gw := gateways[key]
		status := gwv1.GatewayStatus{}
		usable := 0
		for _, l := range listeners[key] {
			if l.usable {
				usable++
			}
			status.Listeners = append(status.Listeners, gwv1.ListenerStatus{
				Name:           l.spec.Name,
				SupportedKinds: []gwv1.RouteGroupKind{{Group: ptrTo(gwv1.Group(gatewayGroup)), Kind: "HTTPRoute"}},
				AttachedRoutes: l.attached,
				Conditions:     listenerConditions[l],
			})
		}
		reason, refusal := gatewayProblem(gw)
		switch {
		case refusal != "":
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionAccepted), false,
				string(reason), refusal, gw.Generation))
		case usable == 0:
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionAccepted), false,
				string(gwv1.GatewayReasonListenersNotValid), "the proxy can serve none of the listeners", gw.Generation))
		case usable < len(listeners[key]):
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionAccepted), true,
				string(gwv1.GatewayReasonListenersNotValid), "the proxy can serve some of the listeners; see each one", gw.Generation))
		default:
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionAccepted), true,
				string(gwv1.GatewayReasonAccepted), "served by SynapseProxy "+in.proxy.Namespace+"/"+in.proxy.Name, gw.Generation))
		}
		switch {
		case usable == 0:
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionProgrammed), false,
				string(gwv1.GatewayReasonInvalid), "no listener is programmed", gw.Generation))
		case len(in.addresses) == 0:
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionProgrammed), false,
				string(gwv1.GatewayReasonAddressNotAssigned), "the proxy's Service has no address yet", gw.Generation))
		default:
			status.Conditions = append(status.Conditions, condition(string(gwv1.GatewayConditionProgrammed), true,
				string(gwv1.GatewayReasonProgrammed), "rendered into the proxy's routes", gw.Generation))
		}
		for _, a := range in.addresses {
			kind := gwv1.HostnameAddressType
			if net.ParseIP(a) != nil {
				kind = gwv1.IPAddressType
			}
			status.Addresses = append(status.Addresses, gwv1.GatewayStatusAddress{Type: &kind, Value: a})
		}
		out.gateways[key] = status
	}
	return out
}

func ptrTo[T any](v T) *T { return &v }

// sortedKeys returns a map's keys in the order they print in.
func sortedKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b K) int { return cmp.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return keys
}

// listener says whether the proxy can serve a Gateway's listener, with the
// conditions that go into its status. A usable HTTPS listener's certificates
// are added to m.
//
// refusal is what the Gateway as a whole asks for that is not done, when
// there is such a thing: none of its listeners is then served.
func (in gatewayInputs) listener(gw *gwv1.Gateway, l *gwListener, m *renderModel, refusal string) (bool, []metav1.Condition) {
	spec := l.spec
	refs := condition(string(gwv1.ListenerConditionResolvedRefs), true, string(gwv1.ListenerReasonResolvedRefs), "nothing is missing", gw.Generation)
	// finish puts the three conditions together. A listener is programmed
	// when it is accepted and nothing it needs is missing.
	finish := func(reason gwv1.ListenerConditionReason, message string, missing bool) (bool, []metav1.Condition) {
		ok := reason == gwv1.ListenerReasonAccepted
		accepted := condition(string(gwv1.ListenerConditionAccepted), ok, string(reason), message, gw.Generation)
		usable := ok && !missing
		programmed := condition(string(gwv1.ListenerConditionProgrammed), usable, string(gwv1.ListenerReasonProgrammed), "rendered into the proxy's routes", gw.Generation)
		if !usable {
			programmed.Reason, programmed.Message = string(gwv1.ListenerReasonInvalid), "not programmed; see the other conditions"
		}
		return usable, []metav1.Condition{accepted, refs, programmed}
	}

	if refusal != "" {
		return finish(gwv1.ListenerReasonUnsupportedValue, "the Gateway is not served: "+refusal, false)
	}
	var want string
	switch spec.Protocol {
	case gwv1.HTTPProtocolType:
		want = "HTTP"
	case gwv1.HTTPSProtocolType:
		want = "TLS"
	default:
		return finish(gwv1.ListenerReasonUnsupportedProtocol, fmt.Sprintf("protocol %s is not served; HTTP and HTTPS are", spec.Protocol), false)
	}
	if !slices.ContainsFunc(in.proxy.Spec.Listeners, func(pl synapsev1alpha1.Listener) bool {
		return pl.Port == int32(spec.Port) && pl.Protocol == want
	}) {
		return finish(gwv1.ListenerReasonPortUnavailable, fmt.Sprintf("the SynapseProxy has no %s listener on port %d", want, spec.Port), false)
	}
	if spec.TLS != nil && spec.TLS.Mode != nil && *spec.TLS.Mode != gwv1.TLSModeTerminate {
		return finish(gwv1.ListenerReasonUnsupportedValue, "TLS is terminated at the proxy; passing it through is not served", false)
	}
	// Neither is something to leave out quietly: a listener that was to
	// ask clients for a certificate and does not is open to everyone.
	if spec.TLS != nil && len(spec.TLS.Options) > 0 {
		return finish(gwv1.ListenerReasonUnsupportedValue, "TLS options are not supported", false)
	}
	if spec.Protocol == gwv1.HTTPSProtocolType && asksForClientCertificates(gw) {
		return finish(gwv1.ListenerReasonUnsupportedValue,
			"the Gateway asks for client certificates (spec.tls.frontend), and the proxy validates none", false)
	}
	// Reported, and no more: the HTTPRoutes it also allows still attach.
	if kinds := allowedKinds(spec); kinds != "" {
		refs = condition(string(gwv1.ListenerConditionResolvedRefs), false, string(gwv1.ListenerReasonInvalidRouteKinds), kinds, gw.Generation)
	}
	if spec.Protocol != gwv1.HTTPSProtocolType {
		return finish(gwv1.ListenerReasonAccepted, "served by the proxy", false)
	}

	if spec.TLS == nil || len(spec.TLS.CertificateRefs) == 0 {
		refs = condition(string(gwv1.ListenerConditionResolvedRefs), false, string(gwv1.ListenerReasonInvalidCertificateRef), "an HTTPS listener needs a certificate", gw.Generation)
		return finish(gwv1.ListenerReasonAccepted, "served by the proxy", true)
	}
	type certificate struct{ namespace, name string }
	var certificates []certificate
	for _, ref := range spec.TLS.CertificateRefs {
		ns := gw.Namespace
		if ref.Namespace != nil {
			ns = string(*ref.Namespace)
		}
		reason, message := gwv1.ListenerConditionReason(""), ""
		switch {
		case (ref.Group != nil && *ref.Group != "" && *ref.Group != "core") || (ref.Kind != nil && *ref.Kind != "Secret"):
			reason, message = gwv1.ListenerReasonInvalidCertificateRef, "a certificate has to be a Secret"
		case ns != gw.Namespace && !in.granted(gatewayGroup, "Gateway", gw.Namespace, "", "Secret", ns, string(ref.Name)):
			reason, message = gwv1.ListenerReasonRefNotPermitted,
				fmt.Sprintf("no ReferenceGrant in %s lets a Gateway in %s use Secret %s", ns, gw.Namespace, ref.Name)
		default:
			sec := in.secret(ns, string(ref.Name))
			if sec == nil || sec.Type != corev1.SecretTypeTLS || len(sec.Data[corev1.TLSCertKey]) == 0 || len(sec.Data[corev1.TLSPrivateKeyKey]) == 0 {
				reason, message = gwv1.ListenerReasonInvalidCertificateRef, fmt.Sprintf("Secret %s/%s is not a TLS Secret with a certificate and a key", ns, ref.Name)
			}
		}
		if reason != "" {
			// One certificate that cannot be used leaves the listener
			// without any: half of what was asked for is not served.
			refs = condition(string(gwv1.ListenerConditionResolvedRefs), false, string(reason), message, gw.Generation)
			return finish(gwv1.ListenerReasonAccepted, "served by the proxy", true)
		}
		certificates = append(certificates, certificate{ns, string(ref.Name)})
	}
	host := ""
	if spec.Hostname != nil {
		host = strings.ToLower(string(*spec.Hostname))
	}
	for _, c := range certificates {
		if stem, hostBound := certStem(host, c.namespace, c.name); hostBound {
			m.addCert(host, stem, c.namespace, c.name)
		} else {
			m.addCert("", stem, c.namespace, c.name)
		}
	}
	return finish(gwv1.ListenerReasonAccepted, "served by the proxy", false)
}

// gatewayProblem returns what a Gateway asks for that is not done and that
// all of it depends on, as the reason and the message of its Accepted
// condition; or two empty strings.
func gatewayProblem(gw *gwv1.Gateway) (gwv1.GatewayConditionReason, string) {
	switch spec := gw.Spec; {
	case len(spec.Addresses) > 0:
		return gwv1.GatewayReasonUnsupportedAddress, "spec.addresses is not supported: the addresses are those of the proxy's Service"
	case spec.Infrastructure != nil && spec.Infrastructure.ParametersRef != nil:
		return gwv1.GatewayReasonInvalidParameters, "spec.infrastructure.parametersRef is not supported: the proxy is configured by its SynapseProxy"
	case spec.TLS != nil && spec.TLS.Backend != nil && spec.TLS.Backend.ClientCertificateRef != nil:
		return gwv1.GatewayReasonInvalid, "spec.tls.backend is not supported: the proxy presents no certificate to a backend"
	}
	return "", ""
}

// asksForClientCertificates reports whether a Gateway wants the clients of
// its HTTPS listeners, of any of them, to present a certificate.
func asksForClientCertificates(gw *gwv1.Gateway) bool {
	if gw.Spec.TLS == nil || gw.Spec.TLS.Frontend == nil {
		return false
	}
	f := gw.Spec.TLS.Frontend
	return f.Default.Validation != nil || slices.ContainsFunc(f.PerPort, func(p gwv1.TLSPortConfig) bool { return p.TLS.Validation != nil })
}

// allowedKinds returns what is wrong with the kinds of route a listener says
// it allows, or "". Only HTTPRoutes are served.
func allowedKinds(l *gwv1.Listener) string {
	if l.AllowedRoutes == nil {
		return ""
	}
	var other []string
	for _, k := range l.AllowedRoutes.Kinds {
		if (k.Group != nil && string(*k.Group) != gatewayGroup) || k.Kind != "HTTPRoute" {
			other = append(other, string(k.Kind))
		}
	}
	if len(other) == 0 {
		return ""
	}
	return "only HTTPRoutes are served, not " + strings.Join(other, ", ")
}

// granted reports whether a ReferenceGrant in toNamespace lets an object of
// the given kind in fromNamespace refer to the named object there.
func (in gatewayInputs) granted(fromGroup, fromKind, fromNamespace, toGroup, toKind, toNamespace, toName string) bool {
	for i := range in.grants {
		g := &in.grants[i]
		if g.Namespace != toNamespace {
			continue
		}
		from := slices.ContainsFunc(g.Spec.From, func(f gwv1beta1.ReferenceGrantFrom) bool {
			return string(f.Group) == fromGroup && string(f.Kind) == fromKind && string(f.Namespace) == fromNamespace
		})
		to := slices.ContainsFunc(g.Spec.To, func(t gwv1beta1.ReferenceGrantTo) bool {
			return string(t.Group) == toGroup && string(t.Kind) == toKind && (t.Name == nil || string(*t.Name) == toName)
		})
		if from && to {
			return true
		}
	}
	return false
}

// parentGateway returns the Gateway a parentRef of a route names, and false
// when it names something that is not a Gateway.
func parentGateway(rt *gwv1.HTTPRoute, ref gwv1.ParentReference) (types.NamespacedName, bool) {
	if (ref.Group != nil && string(*ref.Group) != gatewayGroup) || (ref.Kind != nil && *ref.Kind != "Gateway") {
		return types.NamespacedName{}, false
	}
	ns := rt.Namespace
	if ref.Namespace != nil {
		ns = string(*ref.Namespace)
	}
	return types.NamespacedName{Namespace: ns, Name: string(ref.Name)}, true
}

// attachment is where a route attaches through one parentRef: the hosts it
// is served on there, or the reason it is not.
type attachment struct {
	hosts           map[string]bool
	reason, message string
	// note is what of the attachment is not served, when part of it is.
	note string
}

// attach works out which listeners of a Gateway a route attaches to through
// one parentRef, counts it on each, and returns the hosts it gets from them.
func (in gatewayInputs) attach(rt *gwv1.HTTPRoute, ref gwv1.ParentReference, all []*gwListener) attachment {
	var named, allowed []*gwListener
	for _, l := range all {
		if ref.SectionName != nil && *ref.SectionName != l.spec.Name {
			continue
		}
		if ref.Port != nil && *ref.Port != l.spec.Port {
			continue
		}
		named = append(named, l)
		if l.usable && in.allows(l.spec, rt.Namespace, rt.Namespace == namespaceOf(ref, rt)) {
			allowed = append(allowed, l)
		}
	}
	switch {
	case len(named) == 0:
		return attachment{reason: string(gwv1.RouteReasonNoMatchingParent), message: "the Gateway has no such listener"}
	case len(allowed) == 0:
		return attachment{reason: string(gwv1.RouteReasonNotAllowedByListeners),
			message: "no listener that the proxy serves allows routes from namespace " + rt.Namespace}
	}

	a := attachment{hosts: map[string]bool{}}
	everyHost := false
	for _, l := range allowed {
		var hosts []string
		switch {
		case len(rt.Spec.Hostnames) == 0 && l.hostname == "":
			// Every host there is: nothing Synapse can be told.
			everyHost = true
			continue
		case len(rt.Spec.Hostnames) == 0:
			hosts = []string{l.hostname}
		default:
			for _, h := range rt.Spec.Hostnames {
				if host := hostIntersection(l.hostname, strings.ToLower(string(h))); host != "" {
					hosts = append(hosts, host)
				}
			}
		}
		if len(hosts) > 0 {
			l.attached++
		}
		for _, h := range hosts {
			a.hosts[h] = true
		}
	}
	const noHost = "neither the route nor its listener names a host, and Synapse routes by host name"
	switch {
	case len(a.hosts) > 0 && everyHost:
		a.note = "not served on a listener that names no host: " + noHost
	case len(a.hosts) > 0:
	case everyHost:
		a.reason = string(gwv1.RouteReasonUnsupportedValue)
		a.message = noHost
	default:
		a.reason = string(gwv1.RouteReasonNoMatchingListenerHostname)
		a.message = "none of the route's hostnames is one a listener serves"
	}
	return a
}

func namespaceOf(ref gwv1.ParentReference, rt *gwv1.HTTPRoute) string {
	if ref.Namespace != nil {
		return string(*ref.Namespace)
	}
	return rt.Namespace
}

// allows reports whether a listener lets routes from a namespace attach.
// sameNamespace says the route is in the Gateway's own.
func (in gatewayInputs) allows(l *gwv1.Listener, routeNamespace string, sameNamespace bool) bool {
	if allowedKinds(l) != "" && !slices.ContainsFunc(l.AllowedRoutes.Kinds, func(k gwv1.RouteGroupKind) bool { return k.Kind == "HTTPRoute" }) {
		return false
	}
	from := gwv1.NamespacesFromSame
	var selector *metav1.LabelSelector
	if l.AllowedRoutes != nil && l.AllowedRoutes.Namespaces != nil {
		if l.AllowedRoutes.Namespaces.From != nil {
			from = *l.AllowedRoutes.Namespaces.From
		}
		selector = l.AllowedRoutes.Namespaces.Selector
	}
	switch from {
	case gwv1.NamespacesFromAll:
		return true
	case gwv1.NamespacesFromSame:
		return sameNamespace
	case gwv1.NamespacesFromSelector:
		// One that is not there selects nothing.
		s, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return false
		}
		nsLabels, known := in.namespaces[routeNamespace]
		return known && s.Matches(labels.Set(nsLabels))
	}
	return false
}

// hostIntersection returns the host a route is served on when its hostname
// meets a listener's, or "" when they have nothing in common. Either may be
// a wildcard, `*.example.com`, which covers every name under example.com.
func hostIntersection(listener, route string) string {
	covers := func(wildcard, name string) bool {
		suffix := strings.TrimPrefix(wildcard, "*")
		return strings.HasPrefix(wildcard, "*.") && strings.HasSuffix(name, suffix)
	}
	switch {
	case listener == "" || listener == route:
		return route
	case covers(listener, route):
		// Covers a wildcard below it as well: the narrower one is served.
		return route
	case covers(route, listener):
		return listener
	}
	return ""
}

// ruleSet is what a route's rules come to on the hosts it attached with.
type ruleSet struct {
	matches []gwMatch
	// unsupported says, for everything that is not served, what and why.
	unsupported []string
	// refReason is why a backend could not be used, when one could not.
	refReason, refMessage string
}

// rules turns a route's rules into matches on each of its hosts.
//
// Two things can be wrong, and they are not treated alike.
//
// A rule the proxy cannot carry out — a filter it does not have, a backend
// that cannot be used — keeps its matches. Its requests are its own: the
// API has them answered with an error, not served by whichever other rule
// also matches them. The matches are marked refused, take their place among
// the host's, and no route is written for them.
//
// A match the proxy cannot evaluate, on a header or a query parameter, is
// left out, and what it would have matched is served as if it were not
// there. There is no way to hold its place without holding more than it
// asked for.
func (in gatewayInputs) rules(rt *gwv1.HTTPRoute, hosts []string) ruleSet {
	var set ruleSet
	position := 0
	for ri, rule := range rt.Spec.Rules {
		what := fmt.Sprintf("rule %d", ri)
		if rule.Name != nil {
			what = fmt.Sprintf("rule %q", *rule.Name)
		}
		req, resp, problem := headerModifiers(rule.Filters)
		switch {
		case problem != "":
		case rule.Timeouts != nil:
			problem = "timeouts are not supported"
		case rule.Retry != nil:
			problem = "retries are not supported"
		case rule.SessionPersistence != nil:
			problem = "session persistence is not supported"
		}
		var servers []backend
		if problem == "" {
			// With none and no problem, its backends could not be used,
			// or are to get no traffic. Which is in set.refReason.
			servers, problem = in.backends(rt, rule, &set)
		}
		if problem != "" {
			set.unsupported = append(set.unsupported, what+": "+problem)
		}
		refused := problem != "" || len(servers) == 0

		ruleMatches := rule.Matches
		if len(ruleMatches) == 0 {
			ruleMatches = []gwv1.HTTPRouteMatch{{}}
		}
		for mi, mt := range ruleMatches {
			where := fmt.Sprintf("%s, match %d", what, mi)
			if len(mt.Headers) > 0 || len(mt.QueryParams) > 0 {
				set.unsupported = append(set.unsupported, where+": matching on a header or a query parameter is not supported")
				continue
			}
			kind, value := gwv1.PathMatchPathPrefix, "/"
			if mt.Path != nil {
				if mt.Path.Type != nil {
					kind = *mt.Path.Type
				}
				if mt.Path.Value != nil && *mt.Path.Value != "" {
					value = *mt.Path.Value
				}
			}
			written := ""
			if kind == gwv1.PathMatchRegularExpression {
				var err error
				if written, err = canonicalRegex(value); err != nil {
					set.unsupported = append(set.unsupported, fmt.Sprintf("%s: %q cannot be used as a regular expression: %v", where, value, err))
					continue
				}
			}
			method := ""
			if mt.Method != nil {
				method = string(*mt.Method)
			}
			for _, host := range hosts {
				set.matches = append(set.matches, gwMatch{
					host: host, kind: kind, value: value, regex: written, method: method,
					refused: refused, servers: servers, reqHeaders: req, respHeaders: resp, what: where,
					created: rt.CreationTimestamp, route: types.NamespacedName{Namespace: rt.Namespace, Name: rt.Name}, position: position,
				})
			}
			position++
		}
	}
	return set
}

// headerModifiers returns the headers a rule's filters set, as the
// "Name: value" lines Synapse takes, or what in the filters cannot be done.
//
// Synapse replaces a header's value. That is what `set` asks for. `add`
// asks for a value beside the ones a header has, and would lose them.
func headerModifiers(filters []gwv1.HTTPRouteFilter) (req, resp []string, problem string) {
	lines := func(f *gwv1.HTTPHeaderFilter) ([]string, string) {
		switch {
		case f == nil:
			return nil, ""
		case len(f.Remove) > 0:
			return nil, "removing a header is not supported"
		case len(f.Add) > 0:
			return nil, "adding to a header is not supported; set replaces its value"
		}
		var out []string
		for _, h := range f.Set {
			out = append(out, fmt.Sprintf("%s: %s", h.Name, h.Value))
		}
		return out, ""
	}
	for i := range filters {
		var add []string
		switch filters[i].Type {
		case gwv1.HTTPRouteFilterRequestHeaderModifier:
			add, problem = lines(filters[i].RequestHeaderModifier)
			req = append(req, add...)
		case gwv1.HTTPRouteFilterResponseHeaderModifier:
			add, problem = lines(filters[i].ResponseHeaderModifier)
			resp = append(resp, add...)
		default:
			problem = fmt.Sprintf("the %s filter is not supported", filters[i].Type)
		}
		if problem != "" {
			return nil, nil, problem
		}
	}
	return req, resp, ""
}

// backends resolves a rule's backendRefs to the servers its requests go to.
//
// It returns none when one of them cannot be used, and records why in set.
// The API has the share of requests that would have gone to that one
// answered with an error. Synapse has no way to fail a share of them, and
// sending them to the others gives those backends traffic nobody asked them
// to take; so the rule is not served. A rule that asks for something
// unsupported is a problem, which is returned.
func (in gatewayInputs) backends(rt *gwv1.HTTPRoute, rule gwv1.HTTPRouteRule, set *ruleSet) ([]backend, string) {
	if len(rule.BackendRefs) == 0 {
		return nil, "it has no backend"
	}
	weighted := slices.ContainsFunc(rule.BackendRefs, func(b gwv1.HTTPBackendRef) bool { return b.Weight != nil })
	var servers []backend
	unusable := false
	for _, br := range rule.BackendRefs {
		if len(br.Filters) > 0 {
			return nil, "filters on a backend are not supported"
		}
		ref := br.BackendObjectReference
		ns := rt.Namespace
		if ref.Namespace != nil {
			ns = string(*ref.Namespace)
		}
		reason, message := gwv1.RouteConditionReason(""), ""
		var svc *corev1.Service
		switch {
		case (ref.Group != nil && *ref.Group != "" && *ref.Group != "core") || (ref.Kind != nil && *ref.Kind != "Service"):
			reason, message = gwv1.RouteReasonInvalidKind, fmt.Sprintf("backend %s is not a Service", ref.Name)
		case ref.Port == nil:
			reason, message = gwv1.RouteReasonUnsupportedValue, fmt.Sprintf("backend %s names no port", ref.Name)
		case ns != rt.Namespace && !in.granted(gatewayGroup, "HTTPRoute", rt.Namespace, "", "Service", ns, string(ref.Name)):
			reason, message = gwv1.RouteReasonRefNotPermitted,
				fmt.Sprintf("no ReferenceGrant in %s lets an HTTPRoute in %s use Service %s", ns, rt.Namespace, ref.Name)
		default:
			if svc = in.service(ns, string(ref.Name)); svc == nil {
				reason, message = gwv1.RouteReasonBackendNotFound, fmt.Sprintf("Service %s/%s does not exist", ns, ref.Name)
			}
		}
		if reason != "" {
			if set.refReason == "" {
				set.refReason, set.refMessage = string(reason), message+"; the rule that names it is not served"
			}
			unusable = true
			continue
		}
		// A cluster IP when there is one: Synapse then has no name to look
		// up, and the address is still right when the pods behind it change.
		host := fmt.Sprintf("%s.%s.svc.%s", ref.Name, ns, cmp.Or(in.clusterDomain, "cluster.local"))
		if ip := svc.Spec.ClusterIP; ip != "" && ip != corev1.ClusterIPNone {
			host = ip
		}
		b := backend{addr: net.JoinHostPort(host, strconv.Itoa(int(*ref.Port)))}
		if weighted {
			weight := int32(1)
			if br.Weight != nil {
				weight = *br.Weight
			}
			if weight <= 0 {
				continue // asked to get no traffic
			}
			b.weight = uint32(weight)
		}
		servers = append(servers, b)
	}
	if unusable {
		return nil, ""
	}
	return servers, ""
}

// rank orders matches the way the API says a request picks between them: an
// exact path first, then the longest prefix, then a match on a method over
// one without, then the older route. A regular expression, which the API
// leaves open, goes between exact and prefix.
func (a gwMatch) rank(b gwMatch) int {
	kind := func(k gwv1.PathMatchType) int {
		return slices.Index([]gwv1.PathMatchType{gwv1.PathMatchExact, gwv1.PathMatchRegularExpression, gwv1.PathMatchPathPrefix}, k)
	}
	length := func(m gwMatch) int {
		if m.kind == gwv1.PathMatchPathPrefix {
			return -len(strings.TrimRight(m.value, "/"))
		}
		return 0
	}
	hasMethod := func(m gwMatch) int {
		if m.method != "" {
			return 0
		}
		return 1
	}
	return cmp.Or(
		cmp.Compare(kind(a.kind), kind(b.kind)),
		cmp.Compare(length(a), length(b)),
		cmp.Compare(hasMethod(a), hasMethod(b)),
		a.created.Compare(b.created.Time),
		cmp.Compare(a.route.String(), b.route.String()),
		cmp.Compare(a.position, b.position),
	)
}

// overlaps reports whether some request can match both a and b. It may say
// yes when there is none, never no when there is one: what a regular
// expression matches is not worked out.
func (a gwMatch) overlaps(b gwMatch) bool {
	if a.method != "" && b.method != "" && a.method != b.method {
		return false
	}
	// under reports whether path is the prefix or below it.
	under := func(path, prefix string) bool {
		p := strings.TrimRight(prefix, "/")
		return path == p || strings.HasPrefix(path, p+"/")
	}
	exactA, exactB := a.kind == gwv1.PathMatchExact, b.kind == gwv1.PathMatchExact
	switch {
	case a.kind == gwv1.PathMatchRegularExpression || b.kind == gwv1.PathMatchRegularExpression:
		return true
	case exactA && exactB:
		return a.value == b.value
	case exactA:
		return under(a.value, b.value)
	case exactB:
		return under(b.value, a.value)
	}
	return under(strings.TrimRight(a.value, "/"), b.value) || under(strings.TrimRight(b.value, "/"), a.value)
}

// plain reports whether the match is one Synapse's plain paths express: a
// path prefix, any method, with somewhere to send the request.
func (a gwMatch) plain() bool {
	return a.kind == gwv1.PathMatchPathPrefix && a.method == "" && !a.refused
}

// expression is the match as Synapse evaluates it.
func (a gwMatch) expression() string {
	var parts []string
	switch a.kind {
	case gwv1.PathMatchExact:
		parts = append(parts, "http.request.path eq "+wfString(a.value))
	case gwv1.PathMatchRegularExpression:
		// In a group, or the anchor would hold for the first alternative
		// only and `/a|/b` would match every path with /b in it.
		parts = append(parts, "http.request.path matches "+wfRegex("^(?:"+a.regex+")"))
	default:
		if p := strings.TrimRight(a.value, "/"); p != "" {
			parts = append(parts, fmt.Sprintf("(http.request.path eq %s or http.request.path matches %s)",
				wfString(p), wfRegex("^"+regexp.QuoteMeta(p)+"/")))
		}
	}
	if a.method != "" {
		parts = append(parts, "http.request.method eq "+wfString(a.method))
	}
	if len(parts) == 0 {
		// Every request. An expression has to say something.
		return `http.request.path matches "^/"`
	}
	return strings.Join(parts, " and ")
}

// emitted is what became of the matches once each host's were seen
// together: how many of a route's are served, and what is not and why.
type emitted struct {
	served map[types.NamespacedName]int
	notes  map[types.NamespacedName][]string
}

// emitGatewayMatches adds the matches to m, host by host.
//
// A host with nothing but path prefixes is written as plain paths, which
// Synapse resolves by the longest one: what the API asks. On any other host
// every match becomes an expression, and each excludes those ranked above
// it that a request could match as well. Synapse tries a host's expressions
// in no order one can rely on, and before its plain paths; made exclusive,
// at most one of them is true for a request, and it is the one the API says
// wins. A refused match is excluded like any other, and has no route.
//
// What a host can carry depends on how it is written, and that is decided
// here, with every route's matches on it in view:
//   - Synapse finds a route's headers by the request's path, among the
//     plain paths. An expression's are never found. So on a host written as
//     expressions a rule that sets headers cannot be carried out.
//   - Synapse up to 0.8.7 does not try the expressions of a wildcard host.
//     Such a host is written as plain paths, and a match those cannot
//     express is left out.
func emitGatewayMatches(m *renderModel, matches []gwMatch) emitted {
	out := emitted{served: map[types.NamespacedName]int{}, notes: map[types.NamespacedName][]string{}}
	note := func(mt gwMatch, why string) {
		out.notes[mt.route] = append(out.notes[mt.route], fmt.Sprintf("%s on %s: %s", mt.what, mt.host, why))
	}
	byHost := map[string][]gwMatch{}
	for _, mt := range matches {
		byHost[mt.host] = append(byHost[mt.host], mt)
	}
	for _, host := range sortedKeys(byHost) {
		ms := byHost[host]
		slices.SortStableFunc(ms, gwMatch.rank)
		if strings.HasPrefix(host, "*.") {
			ms = slices.DeleteFunc(ms, func(mt gwMatch) bool {
				if !mt.plain() && !mt.refused {
					note(mt, "only a path prefix, for any method, is served on a wildcard host")
				}
				return !mt.plain()
			})
		}
		if !slices.ContainsFunc(ms, func(mt gwMatch) bool { return !mt.plain() }) {
			for _, mt := range ms {
				// Its own headers, even when it has none: without a list
				// of its own a path takes the nearest one above it, and
				// one with request headers only has them sent back in
				// its responses.
				if m.addRoute(host, mt.value, mt.servers, annSettings{}, mt.reqHeaders, mt.respHeaders) {
					m.hosts[host][plainPathKey(mt.value)].ownHeaders = true
					out.served[mt.route]++
				}
			}
			continue
		}
		var above []gwMatch
		var seen []string
		for i, mt := range ms {
			own := "(" + mt.expression() + ")"
			if slices.Contains(seen, own) {
				continue // the same match, written twice: the first one has it
			}
			seen = append(seen, own)
			if !mt.refused && len(mt.reqHeaders)+len(mt.respHeaders) > 0 {
				mt.refused = true
				note(mt, "headers cannot be set on a host that has an exact, a method or a regular-expression match")
			}
			var excluded []string
			for _, higher := range above {
				if higher.overlaps(mt) {
					excluded = append(excluded, "("+higher.expression()+")")
				}
			}
			above = append(above, mt)
			if mt.refused {
				continue
			}
			expr := own
			if len(excluded) > 0 {
				expr = fmt.Sprintf("%s and not (%s)", own, strings.Join(excluded, " or "))
			}
			if m.addExprRoute(host, fmt.Sprintf("gateway:%03d", i), expr, mt.servers, nil, nil) {
				out.served[mt.route]++
			}
		}
	}
	return out
}
