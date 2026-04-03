.PHONY: build test lint format check quality clean tidy harvest

# Go build settings
GOFLAGS ?= -trimpath
LDFLAGS ?= -s -w

build:
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" ./...

test:
	go test -v -race -count=1 -coverprofile=coverage.out ./...

lint:
	golangci-lint run ./...

format:
	gofumpt -w .
	goimports -w .

tidy:
	go mod tidy

check: format tidy lint test build
	@echo "All checks passed."

quality:
	@echo "=== Quality Gate ==="
	@test -f LICENSE || { echo "ERROR: LICENSE missing. Fix: add MIT LICENSE file"; exit 1; }
	@! grep -rn "TODO\|FIXME\|HACK\|console\.log\|println\|print(" internal/ cmd/ 2>/dev/null | grep -v "_test.go" || { echo "ERROR: debug output or TODO found. Fix: remove before ship"; exit 1; }
	@! grep -rn "password=\|secret=\|api_key=\|sk-\|ghp_" internal/ cmd/ 2>/dev/null | grep -v '\$${' || { echo "ERROR: hardcoded secrets. Fix: use env vars with no default"; exit 1; }
	@test ! -f PRD.md || ! grep -q "\[ \]" PRD.md || { echo "ERROR: unchecked acceptance criteria in PRD.md"; exit 1; }
	@test ! -f CLAUDE.md || [ $$(wc -l < CLAUDE.md) -le 50 ] || { echo "ERROR: CLAUDE.md is $$(wc -l < CLAUDE.md) lines (max 50). Fix: remove build details, use pointers only"; exit 1; }
	@echo "OK: automated quality checks passed"
	@echo "Manual checks required: README quickstart, demo GIF, input validation, ADR >=1"

clean:
	go clean -cache -testcache
	rm -f coverage.out

harvest:
	@echo "=== Harvest ==="
	@mkdir -p docs
	@echo "# Harvest: $$(basename $$(pwd))" > docs/harvest.md
	@echo "" >> docs/harvest.md
	@echo "## メトリクス" >> docs/harvest.md
	@echo "| 項目 | 値 |" >> docs/harvest.md
	@echo "|------|-----|" >> docs/harvest.md
	@echo "| コミット数 | $$(git log --oneline --no-merges | wc -l) |" >> docs/harvest.md
	@echo "| ADR数 | $$(ls docs/adr/*.md 2>/dev/null | wc -l) |" >> docs/harvest.md
	@echo "| CLAUDE.md行数 | $$(wc -l < CLAUDE.md 2>/dev/null || echo 0) |" >> docs/harvest.md
	@echo "| settings.json | $$(test -f .claude/settings.json && echo YES || echo NO) |" >> docs/harvest.md
	@echo "| startup.sh | $$(test -f startup.sh && echo YES || echo NO) |" >> docs/harvest.md
	@echo "| lefthook.yml | $$(test -f lefthook.yml && echo YES || echo NO) |" >> docs/harvest.md
	@echo "| CI | $$(test -f .github/workflows/ci.yml && echo YES || echo NO) |" >> docs/harvest.md
	@echo "" >> docs/harvest.md
	@echo "Harvest report generated: docs/harvest.md (supplement manually)"
