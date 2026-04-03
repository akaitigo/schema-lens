# Changelog

## [1.0.0] - 2026-04-04

### Added
- **DB Connector**: PostgreSQL / MySQL / SQLite対応のConnectorインターフェース
- **スキーマ品質スコアリング**: 正規化・命名規約・型適正性・インデックスの4カテゴリ
- **データプロファイリング**: カラム別NULL率・カーディナリティ・型乖離検出
- **改善提案**: 優先度付き改善提案リストの自動生成
- **マイグレーションSQL**: ALTER TABLE / CREATE INDEX / DROP INDEX の自動生成
- **CLI**: `analyze` / `migrate` サブコマンド
- **出力フォーマット**: table / JSON / Markdown
- **DRY RUN**: マイグレーションSQLの確認モード
- **CI/CD**: GitHub Actions (lint, test, build)
- **ADR**: コネクタインターフェース設計 (ADR-001)
