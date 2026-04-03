package connector

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// NewSQLiteForTest creates an in-memory SQLite database with the given DDL for testing.
func NewSQLiteForTest(t *testing.T, ddl string) *SQLiteConnector {
	t.Helper()
	ctx := context.Background()
	conn := &SQLiteConnector{}
	if err := conn.Connect(ctx, "file::memory:"); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	for _, stmt := range splitStatements(ddl) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := conn.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("failed to execute DDL: %v\nStatement: %s", err, stmt)
		}
	}
	return conn
}

func splitStatements(ddl string) []string {
	return strings.Split(ddl, ";")
}

// DB returns the underlying *sql.DB for test setup purposes.
func (c *SQLiteConnector) DB() *sql.DB {
	return c.db
}
