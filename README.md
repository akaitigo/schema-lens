# SchemaLens

[![CI](https://github.com/akaitigo/schema-lens/actions/workflows/ci.yml/badge.svg)](https://github.com/akaitigo/schema-lens/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/akaitigo/schema-lens)](https://goreportcard.com/report/github.com/akaitigo/schema-lens)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> 既存DBのスキーマ品質を自動分析し、改善提案とマイグレーションSQLを生成するCLIツール

## Quick Start

```bash
go install github.com/akaitigo/schema-lens/cmd/schema-lens@latest

# スキーマ分析 + 品質スコア表示
schema-lens analyze --dsn "postgres://user:pass@localhost:5432/mydb"

# データプロファイリング + 改善提案付き
schema-lens analyze --dsn "sqlite://mydb.sqlite" --profile --suggest

# マイグレーションSQL出力（DRY RUN）
schema-lens migrate --dsn "mysql://user:pass@tcp(localhost)/mydb" --dry-run

# JSON/Markdown出力
schema-lens analyze --dsn "file:test.db" --suggest --format json
schema-lens analyze --dsn "file:test.db" --suggest --format markdown
```

## Features

### スキーマ品質スコアリング

4カテゴリで0〜100点のスコアリング:

| カテゴリ | チェック内容 | 加重 |
|---|---|---|
| **正規化** | 繰り返しカラム、過剰JSON、同名カラム重複 | 30% |
| **命名規約** | snake_case/camelCase混在、単数/複数不統一、予約語 | 20% |
| **型適正性** | VARCHAR(255)デフォルト、TEXT/BLOB過剰、INT-as-BOOLEAN | 25% |
| **インデックス** | FK未インデックス、左前方一致冗長、重複インデックス | 25% |

### データプロファイリング

実データをサンプリングし、各カラムの実態を可視化:

- **NULL率** — NULL行数 / サンプルサイズ
- **カーディナリティ** — ユニーク値数 / 全行数
- **型乖離検出** — `VARCHAR(255)`だが実際は最大12文字、`INT`だが全値が0/1 等

### マイグレーションSQL自動生成

検出した問題に対するALTER TABLE文を自動生成。DRY RUNモードでSQLを確認後、安全に適用可能。

## Supported Databases

| DB | Driver | Schema Source |
|---|---|---|
| PostgreSQL | lib/pq | information_schema + pg_indexes |
| MySQL | go-sql-driver/mysql | information_schema |
| SQLite | modernc.org/sqlite (CGO-free) | sqlite_master + PRAGMA |

## Architecture

```
cmd/schema-lens/     CLI (cobra)
internal/
  connector/         DB接続・スキーマ抽出 (Connector interface)
  analyzer/          品質スコアリング (4カテゴリ)
  profiler/          データプロファイリング
  reporter/          レポート生成・SQL生成・出力フォーマット
```

## Tech Stack

- Go 1.25+
- [cobra](https://github.com/spf13/cobra) — CLI framework
- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) — CGO-free SQLite driver
- golangci-lint v2 — linting

## Development

```bash
# ビルド
make build

# テスト
make test

# lint
make lint

# 全チェック
make check
```

## License

MIT
