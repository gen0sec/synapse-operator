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
	"sigs.k8s.io/controller-runtime/pkg/client"

	synapsev1alpha1 "synapse-operator/api/v1alpha1"
)

// The ConfigValid reasons of an agent whose spec.ids.rules cannot be read.
const (
	ReasonRulesSourceNotFound = "RulesSourceNotFound"
	ReasonRulesKeyNotFound    = "RulesKeyNotFound"
	ReasonRulesTooLarge       = "RulesTooLarge"
)

const (
	// agentRulesLimit caps the rule files together. They live in the config
	// Secret beside config.yaml, and a Secret may not reach 1 MiB.
	agentRulesLimit = 900 << 10

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

// resolveAgentRules reads the sources of spec.ids.rules, in order. A reason
// other than "" is something the user has to fix; an error is worth a retry.
func resolveAgentRules(ctx context.Context, c client.Reader, namespace string, sources []synapsev1alpha1.AgentIDSRuleSource) (files []agentRuleFile, reason, message string, err error) {
	seen := map[string]bool{}
	total := 0
	for i, src := range sources {
		at := fmt.Sprintf("spec.ids.rules[%d]", i)
		var kind, name string
		data := map[string][]byte{}
		switch {
		case src.ConfigMapRef != nil:
			kind, name = "ConfigMap", src.ConfigMapRef.Name
			var cm corev1.ConfigMap
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, ReasonRulesSourceNotFound, fmt.Sprintf("%s: ConfigMap %q not found", at, name), nil
				}
				return nil, "", "", err
			}
			for k, v := range cm.Data {
				data[k] = []byte(v)
			}
		case src.SecretRef != nil:
			kind, name = "Secret", src.SecretRef.Name
			var secret corev1.Secret
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &secret); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, ReasonRulesSourceNotFound, fmt.Sprintf("%s: Secret %q not found", at, name), nil
				}
				return nil, "", "", err
			}
			data = secret.Data
		default:
			continue
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
			total += len(content)
			files = append(files, agentRuleFile{Name: fileName, Data: content})
		}
	}
	if total > agentRulesLimit {
		return nil, ReasonRulesTooLarge, fmt.Sprintf("spec.ids.rules: the rule files add up to %d KiB, over the limit of %d KiB; move the rest to rules managed on the platform", total>>10, agentRulesLimit>>10), nil
	}
	return files, "", "", nil
}

// agentRuleFileNames lists the names of files, for the render.
func agentRuleFileNames(files []agentRuleFile) []string {
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.Name
	}
	return names
}
