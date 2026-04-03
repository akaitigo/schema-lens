package internal_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/akaitigo/schema-lens/internal/analyzer"
	"github.com/akaitigo/schema-lens/internal/connector"
	"github.com/akaitigo/schema-lens/internal/profiler"
	"github.com/akaitigo/schema-lens/internal/reporter"
)

// badSchemaDDL creates a schema with multiple quality anti-patterns:
//   - Repeating columns (addr1, addr2, addr3 + phone1, phone2, phone3)
//   - Mixed naming conventions (snake_case and camelCase)
//   - VARCHAR(255) columns with short actual data
//   - Foreign keys without supporting indexes
//   - INT columns used as booleans (is_active, is_verified, has_confirmed)
//   - SQL reserved word as table name ("order")
//   - Mixed singular/plural table names
const badSchemaDDL = `
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE "order" (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total VARCHAR(255),
    region_id INTEGER NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES categories(id),
    FOREIGN KEY (region_id) REFERENCES categories(id)
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku VARCHAR(20) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description VARCHAR(255),
    addr1 VARCHAR(255),
    addr2 VARCHAR(255),
    addr3 VARCHAR(255),
    phone1 VARCHAR(255),
    phone2 VARCHAR(255),
    phone3 VARCHAR(255),
    category_id INTEGER,
    is_active INTEGER DEFAULT 1,
    is_verified INTEGER DEFAULT 0,
    has_confirmed INTEGER DEFAULT 0,
    productCode VARCHAR(30),
    itemLabel VARCHAR(40),
    FOREIGN KEY (category_id) REFERENCES categories(id)
)
`

// insertBadSchemaData inserts ~50 rows of realistic test data into the bad schema.
func insertBadSchemaData(t *testing.T, conn *connector.SQLiteConnector) {
	t.Helper()
	ctx := context.Background()
	db := conn.DB()

	categories := []string{"Electronics", "Books", "Clothing", "Food", "Toys"}
	for i, name := range categories {
		if _, err := db.ExecContext(ctx, "INSERT INTO categories (id, name) VALUES (?, ?)", i+1, name); err != nil {
			t.Fatalf("failed to insert category: %v", err)
		}
	}

	for i := 1; i <= 5; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO "order" (id, customer_id, total, region_id) VALUES (?, ?, ?, ?)`,
			i, (i%5)+1, fmt.Sprintf("$%d.00", i*10), (i%3)+1,
		); err != nil {
			t.Fatalf("failed to insert order row %d: %v", i, err)
		}
	}

	for i := 1; i <= 50; i++ {
		insertProductRow(t, db, ctx, i)
	}
}

// insertProductRow inserts a single product row with deterministic test data.
func insertProductRow(t *testing.T, db *sql.DB, ctx context.Context, i int) {
	t.Helper()
	catID := (i % 5) + 1
	active := i % 2
	verified := (i + 1) % 2
	confirmed := i % 2

	if _, err := db.ExecContext(ctx,
		`INSERT INTO products
			(id, sku, title, description, addr1, addr2, addr3,
			 phone1, phone2, phone3,
			 category_id, is_active, is_verified, has_confirmed,
			 productCode, itemLabel)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		i,
		fmt.Sprintf("SKU-%04d", i),
		fmt.Sprintf("Product %d", i),
		fmt.Sprintf("Desc %d", i),
		fmt.Sprintf("%d Main St", i*10),
		fmt.Sprintf("Suite %d", i),
		fmt.Sprintf("Floor %d", (i%5)+1),
		fmt.Sprintf("555-0%03d", i),
		fmt.Sprintf("555-1%03d", i),
		fmt.Sprintf("555-2%03d", i),
		catID, active, verified, confirmed,
		fmt.Sprintf("PC-%d", i),
		fmt.Sprintf("Label-%d", i),
	); err != nil {
		t.Fatalf("failed to insert product row %d: %v", i, err)
	}
}

// setupBadSchema creates and populates the bad schema test database.
func setupBadSchema(t *testing.T) *connector.SQLiteConnector {
	t.Helper()
	conn := connector.NewSQLiteForTest(t, badSchemaDDL)
	t.Cleanup(func() { conn.Close() })
	insertBadSchemaData(t, conn)
	return conn
}

// runFullPipeline executes the full analysis pipeline and returns all results.
func runFullPipeline(t *testing.T, conn *connector.SQLiteConnector) (
	*connector.SchemaInfo, *analyzer.AnalysisResult, *profiler.ProfileResult, *reporter.Report,
) {
	t.Helper()
	ctx := context.Background()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("ExtractSchema failed: %v", err)
	}

	analysis := analyzer.Analyze(schema)

	profileResult, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("Profile failed: %v", err)
	}

	report := reporter.GenerateReport("file::memory:", analysis, profileResult)

	return schema, analysis, profileResult, report
}

// findTable returns the table with the given name, or nil if not found.
func findTable(schema *connector.SchemaInfo, name string) *connector.Table {
	for i := range schema.Tables {
		if schema.Tables[i].Name == name {
			return &schema.Tables[i]
		}
	}
	return nil
}

// findTableProfile returns the table profile with the given name, or nil.
func findTableProfile(result *profiler.ProfileResult, name string) *profiler.TableProfile {
	for i := range result.Tables {
		if result.Tables[i].Name == name {
			return &result.Tables[i]
		}
	}
	return nil
}

// countTypeMismatches counts columns with type mismatch detections.
func countTypeMismatches(tp *profiler.TableProfile) int {
	count := 0
	for _, cp := range tp.Columns {
		if cp.TypeMismatch != nil {
			count++
		}
	}
	return count
}

// assertProposalsSorted verifies proposals are ordered from highest to lowest severity.
func assertProposalsSorted(t *testing.T, proposals []reporter.Proposal) {
	t.Helper()
	for i := 1; i < len(proposals); i++ {
		if proposals[i].Priority > proposals[i-1].Priority {
			t.Errorf("proposals not sorted by priority: index %d (%s) > index %d (%s)",
				i, proposals[i].Priority, i-1, proposals[i-1].Priority)
			return
		}
	}
}

// TestIntegration_FullPipeline runs the complete pipeline:
// Connect -> ExtractSchema -> Analyze -> Profile -> GenerateReport
// and asserts that the bad schema produces expected quality issues.
func TestIntegration_FullPipeline(t *testing.T) {
	t.Parallel()

	conn := setupBadSchema(t)
	schema, analysis, profileResult, report := runFullPipeline(t, conn)

	// --- Schema verification ---
	if len(schema.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(schema.Tables))
	}
	productsTable := findTable(schema, "products")
	if productsTable == nil {
		t.Fatal("products table not found in schema")
	}
	if len(productsTable.Columns) != 16 {
		t.Errorf("expected 16 columns in products, got %d", len(productsTable.Columns))
	}

	// --- Analysis verification ---
	if analysis.OverallScore >= 80 {
		t.Errorf("expected OverallScore < 80 for bad schema, got %.1f", analysis.OverallScore)
	}
	assertCategoryHasIssues(t, analysis, "Normalization", "repeating column")
	assertCategoryHasIssues(t, analysis, "Naming", "mixed naming")
	assertCategoryHasIssues(t, analysis, "Typing", "VARCHAR(255)")
	assertCategoryHasIssues(t, analysis, "Typing", "boolean")

	// --- Profile verification ---
	if len(profileResult.Tables) != 3 {
		t.Fatalf("expected 3 profiled tables, got %d", len(profileResult.Tables))
	}
	productsProfile := findTableProfile(profileResult, "products")
	if productsProfile == nil {
		t.Fatal("products table not found in profile results")
	}
	if productsProfile.RowCount != 50 {
		t.Errorf("expected 50 rows profiled, got %d", productsProfile.RowCount)
	}
	if len(productsProfile.Columns) == 0 {
		t.Fatal("expected column profiles for products table")
	}
	if countTypeMismatches(productsProfile) == 0 {
		t.Error("expected at least one type mismatch from profiling (VARCHAR(255) with short data or INT as boolean)")
	}

	// --- Report verification ---
	if report.Summary.OverallScore >= 80 {
		t.Errorf("expected report OverallScore < 80, got %.1f", report.Summary.OverallScore)
	}
	if report.Summary.TableCount != 3 {
		t.Errorf("expected 3 tables in report summary, got %d", report.Summary.TableCount)
	}
	if report.Summary.IssueCount == 0 {
		t.Error("expected non-zero issue count in report summary")
	}
	if len(report.Proposals) == 0 {
		t.Error("expected proposals in report")
	}
	if len(report.MigrationSQL) == 0 {
		t.Error("expected non-empty MigrationSQL in report")
	}
	assertProposalsSorted(t, report.Proposals)
}

// TestIntegration_GoodSchema verifies that a well-designed schema scores high.
func TestIntegration_GoodSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	conn := connector.NewSQLiteForTest(t, `
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
	t.Cleanup(func() { conn.Close() })

	db := conn.DB()
	for i := 1; i <= 10; i++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO users (id, first_name, last_name, email) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("User%d", i), fmt.Sprintf("Last%d", i), fmt.Sprintf("user%d@example.com", i),
		); err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
	}
	for i := 1; i <= 20; i++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO orders (id, user_id, total_amount, order_date) VALUES (?, ?, ?, ?)",
			i, (i%10)+1, float64(i)*10.5, "2026-01-15",
		); err != nil {
			t.Fatalf("failed to insert order: %v", err)
		}
	}

	schema, analysis, _, report := runFullPipeline(t, conn)
	_ = schema

	if analysis.OverallScore < 80 {
		t.Errorf("expected OverallScore >= 80 for good schema, got %.1f", analysis.OverallScore)
	}
	if report.Summary.OverallScore < 80 {
		t.Errorf("expected report score >= 80 for good schema, got %.1f", report.Summary.OverallScore)
	}
}

// TestIntegration_AllFormatOutputs verifies that all three output formats produce
// non-empty, well-formed output from a real analysis pipeline.
func TestIntegration_AllFormatOutputs(t *testing.T) {
	t.Parallel()

	conn := setupBadSchema(t)
	_, _, _, report := runFullPipeline(t, conn)

	t.Run("table_format", func(t *testing.T) {
		t.Parallel()
		assertFormatContains(t, report, "table",
			"Schema Analysis Report", "Migration SQL")
	})

	t.Run("json_format", func(t *testing.T) {
		t.Parallel()
		assertFormatContains(t, report, "json", "overall_score")
	})

	t.Run("markdown_format", func(t *testing.T) {
		t.Parallel()
		assertFormatContains(t, report, "markdown",
			"# Schema Analysis Report", "```sql")
	})
}

// assertFormatContains renders the report in the given format and checks for expected strings.
func assertFormatContains(t *testing.T, report *reporter.Report, format string, expected ...string) {
	t.Helper()

	var buf strings.Builder
	var err error

	switch format {
	case "table":
		err = reporter.FormatTable(&buf, report)
	case "json":
		err = reporter.FormatJSON(&buf, report)
	case "markdown":
		err = reporter.FormatMarkdown(&buf, report)
	default:
		t.Fatalf("unknown format: %s", format)
	}

	if err != nil {
		t.Fatalf("Format%s failed: %v", format, err)
	}

	output := buf.String()
	if len(output) == 0 {
		t.Fatalf("%s format produced empty output", format)
	}

	for _, want := range expected {
		if !strings.Contains(output, want) {
			t.Errorf("%s format missing expected content: %q", format, want)
		}
	}
}

// assertCategoryHasIssues checks that at least one issue in the given category
// contains the specified substring in its description.
func assertCategoryHasIssues(t *testing.T, result *analyzer.AnalysisResult, category, descSubstring string) {
	t.Helper()
	for _, cat := range result.Categories {
		if cat.Name != category {
			continue
		}
		for _, issue := range cat.Issues {
			if strings.Contains(strings.ToLower(issue.Description), strings.ToLower(descSubstring)) {
				return
			}
		}
	}
	t.Errorf("expected category %q to have an issue containing %q", category, descSubstring)
}
