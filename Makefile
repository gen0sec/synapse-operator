.PHONY: release help

# Optional release-note appended to the tag subject (e.g. MESSAGE="bump k8s deps").
# The tag is always annotated (-m) so it works when tag.gpgsign is enabled, which
# forces signed/annotated tags and rejects a lightweight `git tag vX.Y.Z`.
MESSAGE ?=
TAG_MSG := $(if $(MESSAGE),operator v$(VERSION): $(MESSAGE),operator v$(VERSION))

help:
	@echo "Available targets:"
	@echo "  release VERSION=x.y.z [MESSAGE=\"...\"] - Release operator: commit, tag v*, and push"
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
