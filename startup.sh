#!/usr/bin/env bash
set -euo pipefail

START_DEV=false
SKIP_CHECKS=false

for arg in "$@"; do
  case "$arg" in
    --dev) START_DEV=true ;;
    --skip-checks) SKIP_CHECKS=true ;;
  esac
done

echo "=== Session Startup ==="

[ -d ".git" ] || { echo "ERROR: Not in git repository"; exit 1; }

echo "=== Tool auto-install ==="
if [ -f "go.mod" ]; then
  echo "Detected: Go"
  command -v golangci-lint &>/dev/null || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest 2>/dev/null || echo "WARN: golangci-lint install failed"; }
  command -v gofumpt &>/dev/null || { echo "Installing gofumpt..."; go install mvdan.cc/gofumpt@latest 2>/dev/null || echo "WARN: gofumpt install failed"; }
fi
command -v lefthook &>/dev/null || { echo "Installing lefthook..."; go install github.com/evilmartians/lefthook@latest 2>/dev/null || echo "WARN: lefthook install failed"; }
if command -v lefthook &>/dev/null && [ -f "lefthook.yml" ]; then
  lefthook install 2>/dev/null && echo "lefthook hooks installed." || echo "WARN: lefthook install failed"
fi
echo "Tool check complete."

echo "=== Recent commits ==="
git log --oneline -10

if [ "$SKIP_CHECKS" = true ]; then
  echo "=== Health check SKIPPED (--skip-checks) ==="
else
  echo "=== Health check ==="
  if make check 2>&1 | tail -10; then
    echo "All checks passed. Ready to work."
  else
    echo "WARN: Checks failed. Review issues before proceeding."
  fi
fi

echo ""
echo "=== Session started at $(date -u +"%Y-%m-%dT%H:%M:%SZ") ==="
echo "Ready to work. State management: git log + GitHub Issues."
