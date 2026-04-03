package reporter_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/akaitigo/schema-lens/internal/analyzer"
	"github.com/akaitigo/schema-lens/internal/profiler"
	"github.com/akaitigo/schema-lens/internal/reporter"
)

// ---------- Helpers ----------

func makeAnalysisResult() *analyzer.AnalysisResult {
	return &analyzer.AnalysisResult{
		OverallScore: 75.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Typing",
				Score: 90.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityLow,
						Category:    "Typing",
						Table:       "users",
						Column:      "name",
						Description: "column 'name' uses VARCHAR(255) which is often a lazy default",
						Suggestion:  "specify an appropriate maximum length based on actual data requirements",
					},
				},
			},
			{
				Name:  "Indexing",
				Score: 80.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityHigh,
						Category:    "Indexing",
						Table:       "orders",
						Column:      "user_id",
						Description: "foreign key on (user_id) referencing users(id) has no supporting index",
						Suggestion:  "create an index on orders(user_id) to improve JOIN and DELETE performance",
					},
				},
			},
			{
				Name:   "Naming",
				Score:  100.0,
				Issues: nil,
			},
			{
				Name:   "Normalization",
				Score:  100.0,
				Issues: nil,
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityLow,
				Category:    "Typing",
				Table:       "users",
				Column:      "name",
				Description: "column 'name' uses VARCHAR(255) which is often a lazy default",
				Suggestion:  "specify an appropriate maximum length based on actual data requirements",
			},
			{
				Severity:    analyzer.SeverityHigh,
				Category:    "Indexing",
				Table:       "orders",
				Column:      "user_id",
				Description: "foreign key on (user_id) referencing users(id) has no supporting index",
				Suggestion:  "create an index on orders(user_id) to improve JOIN and DELETE performance",
			},
		},
	}
}

func makeProfileResult() *profiler.ProfileResult {
	return &profiler.ProfileResult{
		Tables: []profiler.TableProfile{
			{
				Name:       "users",
				RowCount:   100,
				SampleSize: 100,
				Columns: []profiler.ColumnProfile{
					{
						Name:     "id",
						DataType: "INTEGER",
					},
					{
						Name:     "name",
						DataType: "VARCHAR",
						TypeMismatch: &profiler.TypeMismatchInfo{
							DeclaredType:  "VARCHAR(255)",
							SuggestedType: "VARCHAR(20)",
							Description:   "declared as VARCHAR(255) but actual max length is 20",
							ActualMaxLen:  20,
						},
					},
					{
						Name:     "email",
						DataType: "VARCHAR",
					},
				},
			},
			{
				Name:       "orders",
				RowCount:   500,
				SampleSize: 500,
				Columns: []profiler.ColumnProfile{
					{
						Name:     "id",
						DataType: "INTEGER",
					},
					{
						Name:     "user_id",
						DataType: "INTEGER",
					},
				},
			},
		},
	}
}

// ---------- Report Generation ----------

func TestGenerateReport_Basic(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	if report.Summary.DatabaseDSN != "postgres://localhost/testdb" {
		t.Errorf("expected DSN 'postgres://localhost/testdb', got %q", report.Summary.DatabaseDSN)
	}
	if report.Summary.OverallScore != 75.0 {
		t.Errorf("expected overall score 75.0, got %.1f", report.Summary.OverallScore)
	}
	if report.Summary.IssueCount != 2 {
		t.Errorf("expected 2 issues, got %d", report.Summary.IssueCount)
	}
	if len(report.Scores) != 4 {
		t.Errorf("expected 4 category scores, got %d", len(report.Scores))
	}
	if len(report.Proposals) != 2 {
		t.Errorf("expected 2 proposals, got %d", len(report.Proposals))
	}
}

func TestGenerateReport_WithProfile(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	profile := makeProfileResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, profile)

	// Profile adds type mismatch proposals
	if report.Summary.TableCount != 2 {
		t.Errorf("expected 2 tables from profile, got %d", report.Summary.TableCount)
	}
	if report.Summary.ColumnCount != 5 {
		t.Errorf("expected 5 columns from profile, got %d", report.Summary.ColumnCount)
	}

	// 2 issues + 1 profile type mismatch = 3 proposals
	if len(report.Proposals) != 3 {
		t.Errorf("expected 3 proposals (2 issues + 1 type mismatch), got %d", len(report.Proposals))
	}
}

func TestGenerateReport_ProposalsSortedByPriority(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	if len(report.Proposals) < 2 {
		t.Fatalf("expected at least 2 proposals, got %d", len(report.Proposals))
	}

	// HIGH severity should come before LOW severity
	if report.Proposals[0].Priority != analyzer.SeverityHigh {
		t.Errorf("expected first proposal to be HIGH priority, got %s", report.Proposals[0].Priority)
	}
	if report.Proposals[1].Priority != analyzer.SeverityLow {
		t.Errorf("expected second proposal to be LOW priority, got %s", report.Proposals[1].Priority)
	}
}

func TestGenerateReport_EmptyAnalysis(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 100.0,
		Categories: []analyzer.CategoryScore{
			{Name: "Typing", Score: 100.0},
			{Name: "Indexing", Score: 100.0},
			{Name: "Naming", Score: 100.0},
			{Name: "Normalization", Score: 100.0},
		},
	}

	report := reporter.GenerateReport("sqlite://test.db", analysis, nil)

	if report.Summary.IssueCount != 0 {
		t.Errorf("expected 0 issues, got %d", report.Summary.IssueCount)
	}
	if len(report.Proposals) != 0 {
		t.Errorf("expected 0 proposals, got %d", len(report.Proposals))
	}
	if len(report.MigrationSQL) != 0 {
		t.Errorf("expected 0 migration SQL, got %d", len(report.MigrationSQL))
	}
}

func TestGenerateReport_MigrationSQLCollected(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	if len(report.MigrationSQL) == 0 {
		t.Fatal("expected migration SQL to be generated")
	}

	// Should contain CREATE INDEX for the missing FK index
	foundCreateIndex := false
	for _, sql := range report.MigrationSQL {
		if strings.HasPrefix(sql, "CREATE INDEX") {
			foundCreateIndex = true
		}
	}
	if !foundCreateIndex {
		t.Error("expected CREATE INDEX in migration SQL for missing FK index")
	}
}

// ---------- SQL Generation ----------

func TestSQLGeneration_CreateIndex(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 90.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Indexing",
				Score: 90.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityHigh,
						Category:    "Indexing",
						Table:       "orders",
						Column:      "user_id",
						Description: "foreign key on (user_id) referencing users(id) has no supporting index",
						Suggestion:  "create an index on orders(user_id)",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityHigh,
				Category:    "Indexing",
				Table:       "orders",
				Column:      "user_id",
				Description: "foreign key on (user_id) referencing users(id) has no supporting index",
				Suggestion:  "create an index on orders(user_id)",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "CREATE INDEX idx_orders_user_id ON orders(user_id);" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected 'CREATE INDEX idx_orders_user_id ON orders(user_id);' in migration SQL, got %v", report.MigrationSQL)
	}
}

func TestSQLGeneration_AlterColumnType(t *testing.T) {
	t.Parallel()

	profile := &profiler.ProfileResult{
		Tables: []profiler.TableProfile{
			{
				Name:       "users",
				RowCount:   50,
				SampleSize: 50,
				Columns: []profiler.ColumnProfile{
					{
						Name:     "status",
						DataType: "VARCHAR",
						TypeMismatch: &profiler.TypeMismatchInfo{
							DeclaredType:  "VARCHAR(255)",
							SuggestedType: "VARCHAR(10)",
							Description:   "declared as VARCHAR(255) but actual max length is 10",
							ActualMaxLen:  10,
						},
					},
				},
			},
		},
	}

	analysis := &analyzer.AnalysisResult{
		OverallScore: 100.0,
		Categories:   []analyzer.CategoryScore{{Name: "Typing", Score: 100.0}},
	}

	report := reporter.GenerateReport("test://db", analysis, profile)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "ALTER TABLE users ALTER COLUMN status TYPE VARCHAR(10);" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected ALTER COLUMN TYPE SQL for profile type mismatch, got %v", report.MigrationSQL)
	}
}

func TestSQLGeneration_DuplicateIndex(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 95.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Indexing",
				Score: 95.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityMedium,
						Category:    "Indexing",
						Table:       "items",
						Column:      "sku",
						Description: "index 'idx_items_sku2' is a duplicate of 'idx_items_sku1'; both cover (sku)",
						Suggestion:  "drop one of the duplicate indexes",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityMedium,
				Category:    "Indexing",
				Table:       "items",
				Column:      "sku",
				Description: "index 'idx_items_sku2' is a duplicate of 'idx_items_sku1'; both cover (sku)",
				Suggestion:  "drop one of the duplicate indexes",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "DROP INDEX IF EXISTS idx_items_sku2;" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected DROP INDEX for duplicate index, got %v", report.MigrationSQL)
	}
}

func TestSQLGeneration_BooleanTypeMismatch(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 98.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Typing",
				Score: 98.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityLow,
						Category:    "Typing",
						Table:       "accounts",
						Column:      "is_active",
						Description: "column 'is_active' uses INTEGER but name suggests boolean semantics",
						Suggestion:  "use BOOLEAN type for clarity",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityLow,
				Category:    "Typing",
				Table:       "accounts",
				Column:      "is_active",
				Description: "column 'is_active' uses INTEGER but name suggests boolean semantics",
				Suggestion:  "use BOOLEAN type for clarity",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "ALTER TABLE accounts ALTER COLUMN is_active TYPE BOOLEAN;" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected ALTER COLUMN TYPE BOOLEAN for boolean mismatch, got %v", report.MigrationSQL)
	}
}

func TestSQLGeneration_RedundantIndex(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 98.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Indexing",
				Score: 98.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityLow,
						Category:    "Indexing",
						Table:       "orders",
						Column:      "user_id",
						Description: "index 'idx_orders_user_id' on (user_id) is redundant; composite index 'idx_orders_user_status' on (user_id, status) already covers it as a left prefix",
						Suggestion:  "consider dropping index 'idx_orders_user_id' since 'idx_orders_user_status' provides the same coverage",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityLow,
				Category:    "Indexing",
				Table:       "orders",
				Column:      "user_id",
				Description: "index 'idx_orders_user_id' on (user_id) is redundant; composite index 'idx_orders_user_status' on (user_id, status) already covers it as a left prefix",
				Suggestion:  "consider dropping index 'idx_orders_user_id' since 'idx_orders_user_status' provides the same coverage",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "DROP INDEX IF EXISTS idx_orders_user_id;" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected DROP INDEX for redundant index, got %v", report.MigrationSQL)
	}
}

func TestSQLGeneration_NoCategoryNoSQL(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 95.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Naming",
				Score: 95.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityMedium,
						Category:    "Naming",
						Table:       "user",
						Description: "table name 'user' is a SQL reserved word",
						Suggestion:  "rename table 'user' to avoid conflicts",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityMedium,
				Category:    "Naming",
				Table:       "user",
				Description: "table name 'user' is a SQL reserved word",
				Suggestion:  "rename table 'user' to avoid conflicts",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	// Naming issues should not generate SQL
	if len(report.MigrationSQL) != 0 {
		t.Errorf("expected no migration SQL for naming issues, got %v", report.MigrationSQL)
	}
}

// ---------- Format: Table ----------

func TestFormatTable_ContainsSummary(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatTable(&buf, report); err != nil {
		t.Fatalf("FormatTable error: %v", err)
	}
	output := buf.String()

	expectations := []string{
		"Schema Analysis Report",
		"postgres://localhost/testdb",
		"Overall Score:",
		"75.0",
		"Category Scores",
		"Typing",
		"Indexing",
		"Improvement Proposals",
		"Migration SQL",
	}

	for _, expect := range expectations {
		if !strings.Contains(output, expect) {
			t.Errorf("table output missing %q", expect)
		}
	}
}

func TestFormatTable_NoProposals(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 100.0,
		Categories: []analyzer.CategoryScore{
			{Name: "Typing", Score: 100.0},
		},
	}
	report := reporter.GenerateReport("test://db", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatTable(&buf, report); err != nil {
		t.Fatalf("FormatTable error: %v", err)
	}
	output := buf.String()

	if strings.Contains(output, "Improvement Proposals") {
		t.Error("should not contain Improvement Proposals section when there are no proposals")
	}
}

// ---------- Format: JSON ----------

func TestFormatJSON_ValidJSON(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatJSON(&buf, report); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	var decoded reporter.Report
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON output is not valid: %v", err)
	}

	if decoded.Summary.DatabaseDSN != "postgres://localhost/testdb" {
		t.Errorf("expected DSN in JSON, got %q", decoded.Summary.DatabaseDSN)
	}
	if decoded.Summary.OverallScore != 75.0 {
		t.Errorf("expected overall score 75.0 in JSON, got %.1f", decoded.Summary.OverallScore)
	}
	if len(decoded.Proposals) != 2 {
		t.Errorf("expected 2 proposals in JSON, got %d", len(decoded.Proposals))
	}
}

func TestFormatJSON_EmptyReport(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 100.0,
		Categories:   []analyzer.CategoryScore{{Name: "Typing", Score: 100.0}},
	}
	report := reporter.GenerateReport("test://db", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatJSON(&buf, report); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	var decoded reporter.Report
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON output is not valid: %v", err)
	}

	if decoded.Summary.IssueCount != 0 {
		t.Errorf("expected 0 issues in JSON, got %d", decoded.Summary.IssueCount)
	}
}

// ---------- Format: Markdown ----------

func TestFormatMarkdown_ContainsMarkdownElements(t *testing.T) {
	t.Parallel()

	analysis := makeAnalysisResult()
	report := reporter.GenerateReport("postgres://localhost/testdb", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatMarkdown(&buf, report); err != nil {
		t.Fatalf("FormatMarkdown error: %v", err)
	}
	output := buf.String()

	expectations := []string{
		"# Schema Analysis Report",
		"## Category Scores",
		"## Improvement Proposals",
		"## Migration SQL",
		"```sql",
		"| Category | Score | Issues |",
		"|----------|-------|--------|",
		"| Priority | Table | Column | Category | Description |",
	}

	for _, expect := range expectations {
		if !strings.Contains(output, expect) {
			t.Errorf("markdown output missing %q", expect)
		}
	}
}

func TestFormatMarkdown_NoProposals(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 100.0,
		Categories: []analyzer.CategoryScore{
			{Name: "Typing", Score: 100.0},
		},
	}
	report := reporter.GenerateReport("test://db", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatMarkdown(&buf, report); err != nil {
		t.Fatalf("FormatMarkdown error: %v", err)
	}
	output := buf.String()

	if strings.Contains(output, "## Improvement Proposals") {
		t.Error("should not contain Improvement Proposals section when there are no proposals")
	}
}

func TestFormatMarkdown_EscapesPipes(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 95.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Typing",
				Score: 95.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityLow,
						Category:    "Typing",
						Table:       "t",
						Column:      "c",
						Description: "uses type A|B which is odd",
						Suggestion:  "fix it",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityLow,
				Category:    "Typing",
				Table:       "t",
				Column:      "c",
				Description: "uses type A|B which is odd",
				Suggestion:  "fix it",
			},
		},
	}
	report := reporter.GenerateReport("test://db", analysis, nil)

	var buf bytes.Buffer
	if err := reporter.FormatMarkdown(&buf, report); err != nil {
		t.Fatalf("FormatMarkdown error: %v", err)
	}
	output := buf.String()

	if strings.Contains(output, "A|B") {
		t.Error("pipe characters should be escaped in markdown table cells")
	}
	if !strings.Contains(output, `A\|B`) {
		t.Error("expected escaped pipe in markdown output")
	}
}

// ---------- SQL Generation: Composite FK Columns ----------

func TestSQLGeneration_CompositeFK(t *testing.T) {
	t.Parallel()

	analysis := &analyzer.AnalysisResult{
		OverallScore: 90.0,
		Categories: []analyzer.CategoryScore{
			{
				Name:  "Indexing",
				Score: 90.0,
				Issues: []analyzer.Issue{
					{
						Severity:    analyzer.SeverityHigh,
						Category:    "Indexing",
						Table:       "order_items",
						Column:      "order_id, product_id",
						Description: "foreign key on (order_id, product_id) has no supporting index",
						Suggestion:  "create an index on order_items(order_id, product_id)",
					},
				},
			},
		},
		Issues: []analyzer.Issue{
			{
				Severity:    analyzer.SeverityHigh,
				Category:    "Indexing",
				Table:       "order_items",
				Column:      "order_id, product_id",
				Description: "foreign key on (order_id, product_id) has no supporting index",
				Suggestion:  "create an index on order_items(order_id, product_id)",
			},
		},
	}

	report := reporter.GenerateReport("test://db", analysis, nil)

	found := false
	for _, sql := range report.MigrationSQL {
		if sql == "CREATE INDEX idx_order_items_order_id_product_id ON order_items(order_id, product_id);" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected composite CREATE INDEX, got %v", report.MigrationSQL)
	}
}

// ---------- SQL Generation: Foreign Key ----------

func TestGenerateAddForeignKeySQL(t *testing.T) {
	t.Parallel()

	sql := reporter.GenerateAddForeignKeySQL("orders", "user_id", "users", "id")
	expected := "ALTER TABLE orders ADD CONSTRAINT fk_orders_user_id FOREIGN KEY (user_id) REFERENCES users(id);"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}
