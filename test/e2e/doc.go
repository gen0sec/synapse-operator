// Package e2e runs a SynapseProxy on a real cluster: the operator in a pod,
// Synapse in the pods the operator creates for it, and requests sent through
// the cluster's load balancer.
//
// The test is built only with the e2e tag, and expects a cluster prepared for
// it. `make e2e` (test/e2e/run.sh) starts one, in a container, and runs it.
package e2e
