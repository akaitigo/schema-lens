# Harvest: schema-lens

## メトリクス
| 項目 | 値 |
|------|-----|
| コミット数 | 6 |
| Issue数 | 5 (全closed) |
| PR数 | 5 (全merged, + 2 dependabot) |
| テスト数 | 73 |
| テストパッケージ数 | 6 |
| ADR数 | 1 |
| CLAUDE.md行数 | 36 |
| settings.json | YES |
| startup.sh | YES |
| lefthook.yml | YES |
| CI | YES |

## 振り返り

### うまくいったこと
- **Connector抽象化**: Strategy パターンでDB実装を分離。SQLiteインメモリでの統合テストが容易
- **golangci-lint v2対応**: v1→v2の設定移行に苦戦したが、最終的にCI/ローカル完全一致
- **エージェント並列実装**: analyzer/profiler/reporterの3パッケージをエージェントに委譲し、効率的に完走

### 課題と対策
| 課題 | 対策 |
|------|------|
| .gitignore の `schema-lens` が cmd/ ディレクトリにマッチ | `/schema-lens` に修正（ルートのみ） |
| golangci-lint v1→v2 移行で設定フォーマット大幅変更 | `version: "2"`, `linters.settings`, `linters.exclusions` に移行 |
| CI Go バージョンとローカルの不一致 | `go-version-file: go.mod` で自動追従 |
| `go mod tidy` でcobraが消える | .gitignore修正でcmd/が見えるようになり解決 |

### テンプレート改善提案
1. **`.gitignore` テンプレート**: バイナリ名のgitignoreエントリは `/binary-name`（先頭スラッシュ付き）にすべき。`binary-name` だとサブディレクトリにもマッチする
2. **golangci-lint設定**: テンプレートをv2形式に更新すべき。`version: "2"`, `linters.settings`（非`linters-settings`）, `linters.exclusions`（非`issues.exclude-rules`）
3. **CI Go version**: `go-version: "1.23"` ハードコード → `go-version-file: go.mod` に変更すべき
4. **golangci-lint install**: `go install .../cmd/golangci-lint@latest` → `go install .../v2/cmd/golangci-lint@latest` に変更すべき
