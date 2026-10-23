package controllers

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func nodeWith(spec corev1.NodeSpec, addrs ...corev1.NodeAddress) corev1.Node {
	return corev1.Node{Spec: spec, Status: corev1.NodeStatus{Addresses: addrs}}
}

func TestHomeNetFromCluster(t *testing.T) {
	const supernets = "10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,fc00::/7,fe80::/10"
	lbSvc := corev1.Service{
		Spec: corev1.ServiceSpec{
			ExternalIPs:    []string{"203.0.113.7"},
			LoadBalancerIP: "203.0.113.8",
		},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
			Ingress: []corev1.LoadBalancerIngress{{IP: "203.0.113.6"}, {Hostname: "lb.example"}},
		}},
	}
	tests := []struct {
		name  string
		nodes []corev1.Node
		svcs  []corev1.Service
		want  string
	}{
		{name: "empty input yields the supernets", want: "[" + supernets + "]"},
		{
			name: "node internal and external IPs, other address types ignored",
			nodes: []corev1.Node{nodeWith(corev1.NodeSpec{},
				corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "10.1.2.3"},
				corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "198.51.100.4"},
				corev1.NodeAddress{Type: corev1.NodeHostName, Address: "node-a"},
				corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "not-an-ip"})},
			want: "[10.0.0.0/8,10.1.2.3/32,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,198.51.100.4/32,fc00::/7,fe80::/10]",
		},
		{
			name: "IPv6 node address becomes a /128",
			nodes: []corev1.Node{nodeWith(corev1.NodeSpec{},
				corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "2001:db8::1"})},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,2001:db8::1/128,fc00::/7,fe80::/10]",
		},
		{
			name: "PodCIDR and PodCIDRs, invalid ones dropped",
			nodes: []corev1.Node{nodeWith(corev1.NodeSpec{
				PodCIDR:  "198.18.0.0/24",
				PodCIDRs: []string{"198.18.0.0/24", "2001:db8:1::/64", "garbage"},
			})},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,198.18.0.0/24,2001:db8:1::/64,fc00::/7,fe80::/10]",
		},
		{
			name: "service ingress IP, externalIPs and loadBalancerIP",
			svcs: []corev1.Service{lbSvc},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,203.0.113.6/32,203.0.113.7/32,203.0.113.8/32,fc00::/7,fe80::/10]",
		},
		{
			name: "duplicates across nodes and services collapse",
			nodes: []corev1.Node{
				nodeWith(corev1.NodeSpec{}, corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "203.0.113.6"}),
				nodeWith(corev1.NodeSpec{}, corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "203.0.113.6"}),
			},
			svcs: []corev1.Service{lbSvc, lbSvc},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,203.0.113.6/32,203.0.113.7/32,203.0.113.8/32,fc00::/7,fe80::/10]",
		},
		{
			name: "service IPs are trimmed",
			svcs: []corev1.Service{{Spec: corev1.ServiceSpec{ExternalIPs: []string{" 203.0.113.9 "}}}},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.168.0.0/16,203.0.113.9/32,fc00::/7,fe80::/10]",
		},
		{
			name: "output is sorted regardless of input order",
			nodes: []corev1.Node{
				nodeWith(corev1.NodeSpec{}, corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "192.0.2.9"}),
				nodeWith(corev1.NodeSpec{}, corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}),
			},
			want: "[10.0.0.0/8,100.64.0.0/10,172.16.0.0/12,192.0.2.10/32,192.0.2.9/32,192.168.0.0/16,fc00::/7,fe80::/10]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := homeNetFromCluster(tc.nodes, tc.svcs); got != tc.want {
				t.Fatalf("homeNetFromCluster:\n got  %s\n want %s", got, tc.want)
			}
		})
	}
}

func TestLoadBalancerIPs(t *testing.T) {
	svc := corev1.Service{
		Spec: corev1.ServiceSpec{ExternalIPs: []string{"203.0.113.7"}, LoadBalancerIP: "203.0.113.8"},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
			Ingress: []corev1.LoadBalancerIngress{{IP: "203.0.113.6"}, {Hostname: "lb.example"}},
		}},
	}
	want := []string{"203.0.113.6", "203.0.113.7", "203.0.113.8"}
	if got := loadBalancerIPs(svc); !reflect.DeepEqual(got, want) {
		t.Fatalf("loadBalancerIPs = %v, want %v", got, want)
	}
	if got := loadBalancerIPs(corev1.Service{}); len(got) != 0 {
		t.Fatalf("empty service gave %v", got)
	}
}
