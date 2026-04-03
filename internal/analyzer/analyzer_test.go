package analyzer_test

import (
	"context"
	"strings"
	"testing"

	"github.com/akaitigo/schema-lens/internal/analyzer"
	"github.com/akaitigo/schema-lens/internal/connector"
)

// ---------- Helper ----------

func extractSchema(t *testing.T, ddl string) *connector.SchemaInfo {
	t.Helper()
	conn := connector.NewSQLiteForTest(t, ddl)
	t.Cleanup(func() { conn.Close() })

	schema, err := conn.ExtractSchema(context.Background())
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}
	return schema
}

func issuesByCategory(result *analyzer.AnalysisResult, category string) []analyzer.Issue {
	for _, cat := range result.Categories {
		if cat.Name == category {
			return cat.Issues
		}
	}
	return nil
}

func categoryScore(result *analyzer.AnalysisResult, category string) float64 {
	for _, cat := range result.Categories {
		if cat.Name == category {
			return cat.Score
		}
	}
	return -1
}

func hasIssueContaining(issues []analyzer.Issue, substring string) bool {
	for _, issue := range issues {
		if strings.Contains(issue.Description, substring) {
			return true
		}
	}
	return false
}

// ---------- Overall ----------

func TestAnalyze_GoodSchema_HighScore(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			first_name VARCHAR(50) NOT NULL,
			last_name VARCHAR(50) NOT NULL,
			email VARCHAR(100) NOT NULL UNIQUE
		);
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			total_amount DECIMAL(10,2),
			order_date TEXT NOT NULL,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);
		CREATE INDEX idx_orders_user_id ON orders(user_id);
	`)

	result := analyzer.Analyze(schema)

	if result.OverallScore < 80 {
		t.Errorf("expected high overall score for good schema, got %.1f", result.OverallScore)
		for _, issue := range result.Issues {
			t.Logf("  [%s] %s.%s: %s", issue.Severity, issue.Table, issue.Column, issue.Description)
		}
	}

	if len(result.Categories) != 4 {
		t.Errorf("expected 4 categories, got %d", len(result.Categories))
	}
}

func TestAnalyze_BadSchema_LowScore(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE "order" (
			id INTEGER PRIMARY KEY,
			addr1 VARCHAR(255),
			addr2 VARCHAR(255),
			addr3 VARCHAR(255),
			is_active INTEGER,
			UserName VARCHAR(255),
			some_data TEXT,
			more_data TEXT,
			extra_data TEXT,
			ref_id INTEGER,
			FOREIGN KEY (ref_id) REFERENCES "order"(id)
		);
	`)

	result := analyzer.Analyze(schema)

	if result.OverallScore >= 90 {
		t.Errorf("expected lower score for bad schema, got %.1f", result.OverallScore)
	}

	if len(result.Issues) == 0 {
		t.Error("expected issues to be detected for bad schema")
	}
}

func TestAnalyze_EmptySchema(t *testing.T) {
	t.Parallel()

	schema := &connector.SchemaInfo{}
	result := analyzer.Analyze(schema)

	if result.OverallScore != 100 {
		t.Errorf("expected 100 for empty schema, got %.1f", result.OverallScore)
	}
	if len(result.Issues) != 0 {
		t.Errorf("expected no issues for empty schema, got %d", len(result.Issues))
	}
}

// ---------- Normalization ----------

func TestNormalization_RepeatingColumns(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE contacts (
			id INTEGER PRIMARY KEY,
			phone1 TEXT,
			phone2 TEXT,
			phone3 TEXT
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Normalization")

	if !hasIssueContaining(issues, "repeating column pattern") {
		t.Error("expected repeating column pattern issue for phone1/phone2/phone3")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}

	for _, issue := range issues {
		if strings.Contains(issue.Description, "repeating column pattern") {
			if issue.Severity != analyzer.SeverityHigh {
				t.Errorf("expected SeverityHigh for repeating columns, got %s", issue.Severity)
			}
		}
	}
}

func TestNormalization_ExcessiveJSON(t *testing.T) {
	t.Parallel()

	schema := &connector.SchemaInfo{
		Tables: []connector.Table{{
			Name: "events",
			Columns: []connector.Column{
				{Name: "id", DataType: "INTEGER", IsPrimaryKey: true},
				{Name: "payload", DataType: "JSON"},
				{Name: "metadata", DataType: "JSON"},
				{Name: "context", DataType: "JSONB"},
			},
		}},
	}

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Normalization")

	if !hasIssueContaining(issues, "JSON columns") {
		t.Error("expected excessive JSON column warning")
	}
}

func TestNormalization_NoIssueWithFewJSON(t *testing.T) {
	t.Parallel()

	schema := &connector.SchemaInfo{
		Tables: []connector.Table{{
			Name: "events",
			Columns: []connector.Column{
				{Name: "id", DataType: "INTEGER", IsPrimaryKey: true},
				{Name: "payload", DataType: "JSON"},
				{Name: "title", DataType: "TEXT"},
			},
		}},
	}

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Normalization")

	if hasIssueContaining(issues, "JSON columns") {
		t.Error("did not expect JSON warning with only 1 JSON column")
	}
}

func TestNormalization_DuplicateColumnNames(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			full_address TEXT
		);
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			full_address TEXT
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Normalization")

	if !hasIssueContaining(issues, "appears in") {
		t.Error("expected duplicate column name detection for full_address")
	}
}

// ---------- Naming ----------

func TestNaming_MixedCase(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE user_profiles (
			id INTEGER PRIMARY KEY,
			firstName TEXT,
			last_name TEXT
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Naming")

	if !hasIssueContaining(issues, "mixed naming conventions") {
		t.Error("expected mixed naming convention detection for camelCase + snake_case columns")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}
}

func TestNaming_ConsistentSnakeCase_NoIssue(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE user_profiles (
			id INTEGER PRIMARY KEY,
			first_name TEXT,
			last_name TEXT,
			email_address TEXT
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Naming")

	for _, issue := range issues {
		if strings.Contains(issue.Description, "mixed naming conventions in columns") {
			t.Error("did not expect mixed naming issue for consistent snake_case columns")
		}
	}
}

func TestNaming_SingularPluralMixed(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT
		);
		CREATE TABLE order_item (
			id INTEGER PRIMARY KEY,
			product_id INTEGER
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Naming")

	if !hasIssueContaining(issues, "singular/plural") {
		t.Error("expected singular/plural inconsistency detection")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}
}

func TestNaming_ReservedWord(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE "user" (
			id INTEGER PRIMARY KEY,
			"order" TEXT
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Naming")

	foundTable := false
	foundColumn := false
	for _, issue := range issues {
		if strings.Contains(issue.Description, "table name 'user'") {
			foundTable = true
		}
		if strings.Contains(issue.Description, "column name 'order'") {
			foundColumn = true
		}
	}
	if !foundTable {
		t.Error("expected reserved word detection for table 'user'")
	}
	if !foundColumn {
		t.Error("expected reserved word detection for column 'order'")
	}
}

// ---------- Typing ----------

func TestTyping_Varchar255(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			sku VARCHAR(20) NOT NULL,
			title VARCHAR(255),
			description VARCHAR(255)
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Typing")

	count := 0
	for _, issue := range issues {
		if strings.Contains(issue.Description, "VARCHAR(255)") {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected 2 VARCHAR(255) issues (title, description), got %d", count)
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}
}

func TestTyping_NoVarchar255Issue_ForNon255(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			sku VARCHAR(20) NOT NULL,
			code VARCHAR(10)
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Typing")

	if hasIssueContaining(issues, "VARCHAR(255)") {
		t.Error("did not expect VARCHAR(255) issue for properly sized columns")
	}
}

func TestTyping_ExcessiveTextBlob(t *testing.T) {
	t.Parallel()

	schema := &connector.SchemaInfo{
		Tables: []connector.Table{{
			Name: "documents",
			Columns: []connector.Column{
				{Name: "id", DataType: "INTEGER", IsPrimaryKey: true},
				{Name: "body", DataType: "TEXT"},
				{Name: "summary", DataType: "TEXT"},
				{Name: "notes", DataType: "TEXT"},
				{Name: "raw", DataType: "BLOB"},
			},
		}},
	}

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Typing")

	if !hasIssueContaining(issues, "TEXT/BLOB columns") {
		t.Error("expected excessive TEXT/BLOB warning")
	}
}

func TestTyping_IntForBoolean(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			is_active INTEGER DEFAULT 0,
			has_verified INTEGER DEFAULT 0,
			login_count INTEGER DEFAULT 0
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Typing")

	activeFound := false
	verifiedFound := false
	loginCountFound := false

	for _, issue := range issues {
		if strings.Contains(issue.Description, "is_active") && strings.Contains(issue.Description, "boolean") {
			activeFound = true
		}
		if strings.Contains(issue.Description, "has_verified") && strings.Contains(issue.Description, "boolean") {
			verifiedFound = true
		}
		if strings.Contains(issue.Description, "login_count") {
			loginCountFound = true
		}
	}

	if !activeFound {
		t.Error("expected boolean detection for is_active")
	}
	if !verifiedFound {
		t.Error("expected boolean detection for has_verified")
	}
	if loginCountFound {
		t.Error("did not expect boolean detection for login_count")
	}
}

// ---------- Indexing ----------

func TestIndexing_ForeignKeyWithoutIndex(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE categories (
			id INTEGER PRIMARY KEY,
			name TEXT
		);
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			category_id INTEGER,
			FOREIGN KEY (category_id) REFERENCES categories(id)
		);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Indexing")

	if !hasIssueContaining(issues, "no supporting index") {
		t.Error("expected foreign key without index detection for category_id")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}

	for _, issue := range issues {
		if strings.Contains(issue.Description, "no supporting index") {
			if issue.Severity != analyzer.SeverityHigh {
				t.Errorf("expected SeverityHigh for missing FK index, got %s", issue.Severity)
			}
		}
	}
}

func TestIndexing_ForeignKeyWithIndex_NoIssue(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE categories (
			id INTEGER PRIMARY KEY,
			name TEXT
		);
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			category_id INTEGER,
			FOREIGN KEY (category_id) REFERENCES categories(id)
		);
		CREATE INDEX idx_products_category_id ON products(category_id);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Indexing")

	if hasIssueContaining(issues, "no supporting index") {
		t.Error("did not expect FK index issue when index exists")
	}
}

func TestIndexing_DuplicateIndexes(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			sku TEXT,
			category TEXT
		);
		CREATE INDEX idx_items_sku1 ON items(sku);
		CREATE INDEX idx_items_sku2 ON items(sku);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Indexing")

	if !hasIssueContaining(issues, "duplicate") {
		t.Error("expected duplicate index detection")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}
}

func TestIndexing_LeftPrefixRedundancy(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			status TEXT
		);
		CREATE INDEX idx_orders_user_id ON orders(user_id);
		CREATE INDEX idx_orders_user_status ON orders(user_id, status);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Indexing")

	if !hasIssueContaining(issues, "redundant") {
		t.Error("expected left-prefix redundancy detection")
		for _, i := range issues {
			t.Logf("  issue: %s", i.Description)
		}
	}
}

func TestIndexing_NoLeftPrefixIssue_DifferentLeading(t *testing.T) {
	t.Parallel()

	schema := extractSchema(t, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			status TEXT
		);
		CREATE INDEX idx_orders_status ON orders(status);
		CREATE INDEX idx_orders_user_status ON orders(user_id, status);
	`)

	result := analyzer.Analyze(schema)
	issues := issuesByCategory(result, "Indexing")

	if hasIssueContaining(issues, "redundant") {
		t.Error("did not expect redundancy issue when single index has a different column")
	}
}

// ---------- Scoring ----------

func TestScoring_DeductionBySeverity(t *testing.T) {
	t.Parallel()

	// Schema with known high-severity issue: FK without index.
	schema := extractSchema(t, `
		CREATE TABLE parents (
			id INTEGER PRIMARY KEY
		);
		CREATE TABLE children (
			id INTEGER PRIMARY KEY,
			parent_id INTEGER,
			FOREIGN KEY (parent_id) REFERENCES parents(id)
		);
	`)

	result := analyzer.Analyze(schema)
	indexScore := categoryScore(result, "Indexing")

	// FK without index = -10 (High), so score should be 90.
	if indexScore != 90 {
		t.Errorf("expected indexing score 90 (100 - 10 for high severity), got %.1f", indexScore)
		for _, i := range issuesByCategory(result, "Indexing") {
			t.Logf("  [%s] %s", i.Severity, i.Description)
		}
	}
}

func TestScoring_FloorAtZero(t *testing.T) {
	t.Parallel()

	// Build schema with many high-severity issues to push score below 0.
	schema := &connector.SchemaInfo{
		Tables: []connector.Table{{
			Name: "bad",
			Columns: []connector.Column{
				{Name: "id", DataType: "INTEGER", IsPrimaryKey: true},
			},
			ForeignKeys: []connector.ForeignKey{
				{Name: "fk1", Columns: []string{"a"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk2", Columns: []string{"b"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk3", Columns: []string{"c"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk4", Columns: []string{"d"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk5", Columns: []string{"e"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk6", Columns: []string{"f"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk7", Columns: []string{"g"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk8", Columns: []string{"h"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk9", Columns: []string{"i"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk10", Columns: []string{"j"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
				{Name: "fk11", Columns: []string{"k"}, ReferencedTable: "x", ReferencedColumns: []string{"id"}},
			},
		}},
	}

	result := analyzer.Analyze(schema)
	indexScore := categoryScore(result, "Indexing")

	if indexScore < 0 {
		t.Errorf("expected score to floor at 0, got %.1f", indexScore)
	}
}

func TestScoring_WeightedAverage(t *testing.T) {
	t.Parallel()

	// Clean schema: all categories should be 100, overall should be 100.
	schema := &connector.SchemaInfo{
		Tables: []connector.Table{{
			Name: "items",
			Columns: []connector.Column{
				{Name: "id", DataType: "INTEGER", IsPrimaryKey: true},
				{Name: "sku", DataType: "VARCHAR(20)"},
			},
		}},
	}

	result := analyzer.Analyze(schema)

	for _, cat := range result.Categories {
		if cat.Score != 100 {
			t.Errorf("expected %s score 100, got %.1f", cat.Name, cat.Score)
		}
	}
	if result.OverallScore != 100 {
		t.Errorf("expected overall score 100, got %.1f", result.OverallScore)
	}
}

func TestSeverity_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		sev  analyzer.Severity
	}{
		{want: "LOW", sev: analyzer.SeverityLow},
		{want: "MEDIUM", sev: analyzer.SeverityMedium},
		{want: "HIGH", sev: analyzer.SeverityHigh},
	}

	for _, tt := range tests {
		if got := tt.sev.String(); got != tt.want {
			t.Errorf("Severity(%d).String() = %q, want %q", tt.sev, got, tt.want)
		}
	}
}
