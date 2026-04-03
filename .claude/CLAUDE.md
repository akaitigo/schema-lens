# SchemaLens — 内部ガイド

## アーキテクチャ
- cmd/schema-lens/main.go — CLIエントリポイント（cobra）
- internal/connector/ — DB接続抽象化。Connector interfaceを実装
- internal/analyzer/ — スキーマ解析エンジン。正規化・命名・型チェック
- internal/profiler/ — データサンプリング＆プロファイリング
- internal/reporter/ — レポート出力（table/JSON/Markdown）

## DB対応
- PostgreSQL: information_schema + pg_catalog
- MySQL: information_schema
- SQLite: sqlite_master + PRAGMA

## テスト戦略
- internal/*: ユニットテスト（モック不要な純粋ロジック中心）
- SQLite を使った統合テスト（外部DB不要）

## 設計判断
- 読み取り専用接続のみ（書き込み権限不要）
- サンプリングサイズはデフォルト1,000行、--sample-sizeで変更可
- 出力形式: --format table|json|markdown
