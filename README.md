# SchemaLens

> 既存DBのスキーマ品質を自動分析し、改善提案とマイグレーションSQLを生成するCLIツール

## Quick Start

```bash
go install github.com/akaitigo/schema-lens/cmd/schema-lens@latest
schema-lens analyze --dsn "postgres://user:pass@localhost:5432/mydb"
```

## Features

- **スキーマ品質スコアリング** — 正規化・命名規約・型適正性を自動チェック
- **データプロファイリング** — NULL率・カーディナリティ・値分布を可視化
- **マイグレーションSQL生成** — 改善提案に基づくALTER文を自動生成（DRY RUN対応）

## Supported Databases

| DB | Status |
|---|---|
| PostgreSQL | Supported |
| MySQL | Supported |
| SQLite | Supported |

## Tech Stack

- Go 1.23
- cobra (CLI framework)

## License

MIT
