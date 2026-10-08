// Package v1alpha1 contains the API types of the synapse.gen0sec.com group.
// +kubebuilder:object:generate=true
// +groupName=synapse.gen0sec.com
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group and version of the types in this package.
	GroupVersion = schema.GroupVersion{Group: "synapse.gen0sec.com", Version: "v1alpha1"}

	// SchemeBuilder registers the types in this package with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this package to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
