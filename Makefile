.PHONY: release help

help:
	@echo "Available targets:"
	@echo "  release VERSION=x.y.z - Release operator: commit, tag v*, and push"
	@echo "  help                             - Show this help message"

release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Releasing operator version $(VERSION)..."
	@git commit --allow-empty -m "chore: release operator $(VERSION)"
	@git tag v$(VERSION)
	@git push origin main
	@git push origin tag v$(VERSION)
	@echo "Operator version $(VERSION) released successfully!"
