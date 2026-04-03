package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/akaitigo/schema-lens/internal/connector"
)

func TestRootCmd_Version(t *testing.T) {
	t.Parallel()

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("--version failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, version) {
		t.Errorf("expected version output to contain %q, got %q", version, output)
	}
}

func TestAnalyzeCmd_MissingDSN(t *testing.T) {
	t.Parallel()

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"analyze"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --dsn is missing, got nil")
	}
	if !strings.Contains(err.Error(), "--dsn is required") {
		t.Errorf("expected error to mention --dsn, got %q", err.Error())
	}
}

func TestAnalyzeCmd_InvalidDSN(t *testing.T) {
	t.Parallel()

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"analyze", "--dsn", "invalid://not-a-real-database"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("expected unsupported database error, got %q", err.Error())
	}
}

// setupSharedMemDB creates a named shared-cache in-memory SQLite DB with the
// given DDL statements. It returns the DSN and a cleanup function. The setup
// connection is kept alive so the shared-cache DB persists until cleanup.
func setupSharedMemDB(t *testing.T, ddlStatements []string) string {
	t.Helper()
	ctx := context.Background()
	dbName := fmt.Sprintf("test_%s_%p", t.Name(), t)
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", dbName)

	setupConn := &connector.SQLiteConnector{}
	if err := setupConn.Connect(ctx, dsn); err != nil {
		t.Fatalf("failed to connect setup DB: %v", err)
	}
	t.Cleanup(func() { setupConn.Close() })

	db := setupConn.DB()
	for _, stmt := range ddlStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("failed to execute setup DDL: %v\nStatement: %s", err, stmt)
		}
	}

	return dsn
}

func TestAnalyzeCmd_SQLiteInMemory_SuggestMode(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			title VARCHAR(255) NOT NULL,
			is_active INTEGER DEFAULT 1
		)`,
		"INSERT INTO items (id, title, is_active) VALUES (1, 'Widget', 1)",
		"INSERT INTO items (id, title, is_active) VALUES (2, 'Gadget', 0)",
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"analyze", "--dsn", dsn, "--suggest"})

	// The output goes to os.Stdout, so we verify that execution succeeds
	// without error. The formatReport function writes to os.Stdout directly.
	if err := cmd.Execute(); err != nil {
		t.Fatalf("analyze --suggest command failed: %v", err)
	}
}

func TestAnalyzeCmd_SQLiteInMemory_WithProfile(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			is_active INTEGER DEFAULT 1
		)`,
		"INSERT INTO products (id, name, is_active) VALUES (1, 'Alpha', 1)",
		"INSERT INTO products (id, name, is_active) VALUES (2, 'Beta', 0)",
		"INSERT INTO products (id, name, is_active) VALUES (3, 'Gamma', 1)",
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"analyze", "--dsn", dsn, "--suggest", "--profile"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("analyze --suggest --profile command failed: %v", err)
	}
}

func TestMigrateCmd_MissingDSN(t *testing.T) {
	t.Parallel()

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"migrate"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --dsn is missing for migrate, got nil")
	}
	if !strings.Contains(err.Error(), "--dsn is required") {
		t.Errorf("expected error to mention --dsn, got %q", err.Error())
	}
}

func TestAnalyzeCmd_SchemaOnlyMode(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE test_t (
			id INTEGER PRIMARY KEY,
			val VARCHAR(50) NOT NULL
		)`,
	})

	// Without --suggest or --profile, the command renders schema-only output.
	cmd := newRootCmd()
	cmd.SetArgs([]string{"analyze", "--dsn", dsn})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("analyze (schema-only) failed: %v", err)
	}
}

func TestAnalyzeCmd_JSONFormat(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE test_t (
			id INTEGER PRIMARY KEY,
			val VARCHAR(50) NOT NULL
		)`,
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"analyze", "--dsn", dsn, "--suggest", "--format", "json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("analyze --format json failed: %v", err)
	}
}

func TestAnalyzeCmd_MarkdownFormat(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE test_t (
			id INTEGER PRIMARY KEY,
			val VARCHAR(50) NOT NULL
		)`,
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"analyze", "--dsn", dsn, "--suggest", "--format", "markdown"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("analyze --format markdown failed: %v", err)
	}
}

func TestMigrateCmd_SQLiteInMemory(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			title VARCHAR(255) NOT NULL,
			is_active INTEGER DEFAULT 1
		)`,
		"INSERT INTO items (id, title, is_active) VALUES (1, 'Widget', 1)",
		"INSERT INTO items (id, title, is_active) VALUES (2, 'Gadget', 0)",
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"migrate", "--dsn", dsn})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("migrate command failed: %v", err)
	}
}

func TestAnalyzeCmd_InvalidFormat(t *testing.T) {
	t.Parallel()

	dsn := setupSharedMemDB(t, []string{
		`CREATE TABLE test_t (
			id INTEGER PRIMARY KEY,
			val VARCHAR(50) NOT NULL
		)`,
	})

	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"analyze", "--dsn", dsn, "--suggest", "--format", "xml"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for unsupported format, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported format") {
		t.Errorf("expected 'unsupported format' error, got %q", err.Error())
	}
}
