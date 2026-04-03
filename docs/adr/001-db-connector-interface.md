# ADR-001: データベースコネクタインターフェース設計

## ステータス
Accepted

## コンテキスト
SchemaLensは複数のデータベース（PostgreSQL, MySQL, SQLite）に対応する必要がある。各DBのスキーマ情報取得方法はそれぞれ異なる（information_schema, pg_catalog, sqlite_master/PRAGMA）。

## 決定
`Connector` インターフェースを定義し、各DB固有の実装を分離する。

```go
type Connector interface {
    Connect(ctx context.Context, dsn string) error
    ExtractSchema(ctx context.Context) (*SchemaInfo, error)
    SampleData(ctx context.Context, table string, limit int) ([]map[string]any, error)
    Close() error
}
```

DSN文字列のスキーム部分（`postgres://`, `mysql://`, `sqlite://`, `file:`）で自動的に適切なコネクタを選択する。

## 根拠
- **Strategy パターン**: 各DB実装を独立してテスト・拡張可能
- **DSNベース判定**: ユーザーが明示的にDB種別を指定する必要がない
- **database/sql使用**: Go標準のdatabase/sqlを使い、ドライバは各実装で管理
- **読み取り専用**: スキーマ解析は読み取りのみ。書き込み権限不要

## 代替案
1. **ORM（GORM等）使用** — 却下。スキーマメタデータ取得にはORMの抽象化が邪魔になる
2. **DB種別をフラグで指定** — 却下。DSNから自動判定できる情報を冗長に要求する必要はない

## 影響
- 新しいDB対応は `Connector` インターフェースの実装追加のみ
- テストはSQLite（インメモリ）で統一的に実施可能
