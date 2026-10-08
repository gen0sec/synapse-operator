.PHONY: release help generate manifests verify-generated vet test

# Matches the k8s.io libraries in go.mod.
CONTROLLER_TOOLS_VERSION ?= v0.22.0
CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

# Optional release-note appended to the tag subject (e.g. MESSAGE="bump k8s deps").
# The tag is always annotated (-m) so it works when tag.gpgsign is enabled, which
# forces signed/annotated tags and rejects a lightweight `git tag vX.Y.Z`.
MESSAGE ?=
TAG_MSG := $(if $(MESSAGE),operator v$(VERSION): $(MESSAGE),operator v$(VERSION))

help:
	@echo "Available targets:"
	@echo "  release VERSION=x.y.z [MESSAGE=\"...\"] - Release operator: commit, tag v*, and push"
	@echo "  generate                                - Regenerate deepcopy code for the API types"
	@echo "  manifests                               - Regenerate the CRDs in config/crd/bases"
	@echo "  verify-generated                        - Fail if generated files are out of date"
	@echo "  vet                                     - Run go vet"
	@echo "  test                                    - Run all tests"
	@echo "  help                                    - Show this help message"

release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=x.y.z [MESSAGE=\"...\"]"; \
		exit 1; \
	fi
	@echo "Releasing operator version $(VERSION)..."
	@git commit --allow-empty -m "chore: release operator $(VERSION)"
	@git tag -m "$(TAG_MSG)" v$(VERSION)
	@git push origin main
	@git push origin tag v$(VERSION)
	@echo "Operator version $(VERSION) released successfully!"

generate:
	$(CONTROLLER_GEN) object paths=./api/...

manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases

# `git status`, not `git diff`: a generated file nobody committed is stale too.
GENERATED = config/crd ':(glob)api/**/zz_generated.*.go'

verify-generated: generate manifests
	@if [ -n "$$(git status --porcelain -- $(GENERATED))" ]; then \
		git status --short -- $(GENERATED); \
		echo "Generated files are out of date: run 'make generate manifests' and commit the result."; \
		exit 1; \
	fi

vet:
	go vet ./...

test:
	go test ./...
