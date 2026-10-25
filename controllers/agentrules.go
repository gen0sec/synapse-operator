package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The ConfigValid reasons of an agent whose spec.ids.rules cannot be read.
const (
	ReasonRulesSourceNotFound = "RulesSourceNotFound"
	ReasonRulesKeyNotFound    = "RulesKeyNotFound"
	ReasonRulesTooLarge       = "RulesTooLarge"
	ReasonRulesSourceInvalid  = "RulesSourceInvalid"
)

const (
	// agentRulesLimit caps the rule files together. They live in the config
	// Secret beside config.yaml, and a Secret may not reach 1 MiB.
	agentRulesLimit = 900 << 10
	// agentRuleFileLimit keeps config.yaml, which lists them all, small.
	agentRuleFileLimit = 256
	// agentSecretLimit is under the 1 MiB the API server allows a Secret.
	agentSecretLimit = 1_000_000

	agentRuleSuffix = ".rules"
	// agentRuleStemLimit keeps a file name, which is a Secret key, under the
	// 253 characters a key may have.
	agentRuleStemLimit = 100
)

// agentRuleFile is one rule file to copy into the config Secret.
type agentRuleFile struct {
	Name string
	Data []byte
}

// agentRuleFileName names a rule file after the source it came from, its key
// and a hash of its content. A changed rule is therefore a new name: the
// rendered rule_paths changes with it, and with it config.yaml, which is the
// file Synapse watches to reload its rules.
func agentRuleFileName(index int, key string, data []byte) string {
	stem := strings.TrimSuffix(key, agentRuleSuffix)
	if len(stem) > agentRuleStemLimit {
		stem = stem[:agentRuleStemLimit]
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("rules.%d.%s.%s%s", index, stem, hex.EncodeToString(sum[:])[:12], agentRuleSuffix)
}

// ruleReaders are where the sources are read from: ConfigMaps live, since
// they are many and not the agent's, and Secrets from the cache that the API
// key already needs.
type ruleReaders struct {
	configMaps, secrets client.Reader
}

// readRuleSource returns the keys of one source. A reason other than "" is
// something the user has to fix; an error is worth a retry.
func readRuleSource(ctx context.Context, rd ruleReaders, agent *synapsev1alpha1.SynapseAgent, src synapsev1alpha1.AgentIDSRuleSource) (kind, name string, data map[string][]byte, reason, message string, err error) {
	data = map[string][]byte{}
	switch {
	case src.ConfigMapRef != nil:
		kind, name = "ConfigMap", src.ConfigMapRef.Name
		var cm corev1.ConfigMap
		if err := rd.configMaps.Get(ctx, client.ObjectKey{Namespace: agent.Namespace, Name: name}, &cm); err != nil {
			if apierrors.IsNotFound(err) {
				return kind, name, nil, ReasonRulesSourceNotFound, fmt.Sprintf("ConfigMap %q not found", name), nil
			}
			return kind, name, nil, "", "", err
		}
		for k, v := range cm.Data {
			data[k] = []byte(v)
		}
		for k, v := range cm.BinaryData {
			data[k] = v
		}
	case src.SecretRef != nil:
		kind, name = "Secret", src.SecretRef.Name
		var secret corev1.Secret
		if err := rd.secrets.Get(ctx, client.ObjectKey{Namespace: agent.Namespace, Name: name}, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return kind, name, nil, ReasonRulesSourceNotFound, fmt.Sprintf("Secret %q not found", name), nil
			}
			return kind, name, nil, "", "", err
		}
		// An agent's config Secret holds copies of rule files: copying it
		// again would grow it on every reconcile.
		if owner := metav1.GetControllerOf(&secret); name == agentConfigSecretName(agent) ||
			(owner != nil && owner.Kind == "SynapseAgent" && owner.APIVersion == synapsev1alpha1.GroupVersion.String()) {
			return kind, name, nil, ReasonRulesSourceInvalid,
				fmt.Sprintf("Secret %q is the configuration of a SynapseAgent, which holds copies of rules; name the Secret or ConfigMap the rules come from", name), nil
		}
		data = secret.Data
	}
	return kind, name, data, "", "", nil
}

// resolveAgentRules reads the sources of spec.ids.rules, in order.
func resolveAgentRules(ctx context.Context, rd ruleReaders, agent *synapsev1alpha1.SynapseAgent, sources []synapsev1alpha1.AgentIDSRuleSource) (files []agentRuleFile, reason, message string, err error) {
	seen := map[string]bool{}
	for i, src := range sources {
		at := fmt.Sprintf("spec.ids.rules[%d]", i)
		kind, name, data, reason, message, err := readRuleSource(ctx, rd, agent, src)
		if err != nil {
			return nil, "", "", err
		}
		if reason != "" {
			return nil, reason, at + ": " + message, nil
		}

		keys := src.Keys
		if len(keys) == 0 {
			for k := range data {
				if strings.HasSuffix(k, agentRuleSuffix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
		}
		for _, k := range keys {
			content, ok := data[k]
			if !ok {
				return nil, ReasonRulesKeyNotFound, fmt.Sprintf("%s: %s %q has no key %q", at, kind, name, k), nil
			}
			fileName := agentRuleFileName(i, k, content)
			if seen[fileName] {
				continue
			}
			seen[fileName] = true
			files = append(files, agentRuleFile{Name: fileName, Data: content})
		}
	}
	if len(files) > agentRuleFileLimit {
		return nil, ReasonRulesTooLarge, fmt.Sprintf("spec.ids.rules: %d rule files, over the limit of %d", len(files), agentRuleFileLimit), nil
	}
	if total := ruleBytes(files); total > agentRulesLimit {
		return nil, ReasonRulesTooLarge, fmt.Sprintf("spec.ids.rules: the rule files add up to %d KiB, over the limit of %d KiB; move the rest to rules managed on the platform", total>>10, agentRulesLimit>>10), nil
	}
	return files, "", "", nil
}

func ruleBytes(files []agentRuleFile) int {
	total := 0
	for _, f := range files {
		total += len(f.Data)
	}
	return total
}

// checkAgentSecretSize is for after the render. The API server caps the sum
// of a Secret's values at 1 MiB, and config.yaml, which lists every rule
// file, is one of them.
func checkAgentSecretSize(config []byte, files []agentRuleFile) (reason, message string) {
	rules := ruleBytes(files)
	if len(config)+rules <= agentSecretLimit {
		return "", ""
	}
	return ReasonRulesTooLarge, fmt.Sprintf("spec.ids.rules: the config.yaml (%d bytes) and the rule files (%d bytes) add up to %d bytes, over the limit of %d bytes for the Secret that holds them",
		len(config), rules, len(config)+rules, agentSecretLimit)
}

// agentRuleFileNames lists the names of files, for the render.
func agentRuleFileNames(files []agentRuleFile) []string {
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.Name
	}
	return names
}
