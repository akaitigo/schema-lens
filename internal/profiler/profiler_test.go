package profiler_test

import (
	"context"
	"math"
	"testing"

	"github.com/akaitigo/schema-lens/internal/connector"
	"github.com/akaitigo/schema-lens/internal/profiler"
)

func TestProfile_NullRate(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			category TEXT
		);
		INSERT INTO products (id, name, description, category) VALUES (1, 'Apple', 'A red fruit', 'Fruit');
		INSERT INTO products (id, name, description, category) VALUES (2, 'Banana', NULL, 'Fruit');
		INSERT INTO products (id, name, description, category) VALUES (3, 'Cherry', 'A small fruit', NULL);
		INSERT INTO products (id, name, description, category) VALUES (4, 'Date', NULL, NULL);
		INSERT INTO products (id, name, description, category) VALUES (5, 'Elderberry', NULL, NULL);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(result.Tables))
	}

	tp := result.Tables[0]
	if tp.Name != "products" {
		t.Errorf("expected table name 'products', got %q", tp.Name)
	}
	if tp.RowCount != 5 {
		t.Errorf("expected row count 5, got %d", tp.RowCount)
	}

	colMap := makeColMap(tp.Columns)

	// name: 0 nulls out of 5
	assertNullRate(t, colMap, "name", 0.0)

	// description: 3 nulls out of 5
	assertNullRate(t, colMap, "description", 0.6)

	// category: 3 nulls out of 5
	assertNullRate(t, colMap, "category", 0.6)
}

func TestProfile_Cardinality(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			status VARCHAR(20) NOT NULL,
			region VARCHAR(50) NOT NULL
		);
		INSERT INTO orders (id, status, region) VALUES (1, 'pending', 'east');
		INSERT INTO orders (id, status, region) VALUES (2, 'shipped', 'west');
		INSERT INTO orders (id, status, region) VALUES (3, 'pending', 'east');
		INSERT INTO orders (id, status, region) VALUES (4, 'delivered', 'east');
		INSERT INTO orders (id, status, region) VALUES (5, 'pending', 'west');
		INSERT INTO orders (id, status, region) VALUES (6, 'shipped', 'north');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	// status has 3 distinct values: pending, shipped, delivered
	assertCardinality(t, colMap, "status", 3)

	// region has 3 distinct values: east, west, north
	assertCardinality(t, colMap, "region", 3)

	// id has 6 distinct values
	assertCardinality(t, colMap, "id", 6)

	// id unique rate should be 1.0
	assertUniqueRate(t, colMap, "id", 1.0)

	// status unique rate should be 0.5 (3 distinct / 6 non-null)
	assertUniqueRate(t, colMap, "status", 0.5)
}

func TestProfile_TypeMismatch_VarcharTooLong(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL
		);
		INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
		INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');
		INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	// name column: VARCHAR(255) but max actual length is 7 ("Charlie")
	nameCol := colMap["name"]
	if nameCol.TypeMismatch == nil {
		t.Fatal("expected type mismatch for 'name' column (VARCHAR(255) with short values)")
	}
	if nameCol.TypeMismatch.SuggestedType != "VARCHAR(7)" {
		t.Errorf("expected suggested type VARCHAR(7), got %q", nameCol.TypeMismatch.SuggestedType)
	}

	// email column: VARCHAR(255) but max actual length is 19 ("charlie@example.com")
	emailCol := colMap["email"]
	if emailCol.TypeMismatch == nil {
		t.Fatal("expected type mismatch for 'email' column")
	}
	if emailCol.TypeMismatch.SuggestedType != "VARCHAR(19)" {
		t.Errorf("expected suggested type VARCHAR(19), got %q", emailCol.TypeMismatch.SuggestedType)
	}
}

func TestProfile_TypeMismatch_IntAsBoolean(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE flags (
			id INTEGER PRIMARY KEY,
			is_active INTEGER NOT NULL,
			priority INTEGER NOT NULL
		);
		INSERT INTO flags (id, is_active, priority) VALUES (1, 1, 5);
		INSERT INTO flags (id, is_active, priority) VALUES (2, 0, 3);
		INSERT INTO flags (id, is_active, priority) VALUES (3, 1, 8);
		INSERT INTO flags (id, is_active, priority) VALUES (4, 0, 1);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	// is_active: INTEGER with all values 0 or 1 → should suggest BOOLEAN
	isActive := colMap["is_active"]
	if isActive.TypeMismatch == nil {
		t.Fatal("expected type mismatch for 'is_active' (INTEGER with boolean values)")
	}
	if isActive.TypeMismatch.SuggestedType != "BOOLEAN" {
		t.Errorf("expected suggested type BOOLEAN, got %q", isActive.TypeMismatch.SuggestedType)
	}

	// priority: INTEGER with values 1,3,5,8 → should NOT suggest BOOLEAN
	priority := colMap["priority"]
	if priority.TypeMismatch != nil {
		t.Errorf("expected no type mismatch for 'priority', got %+v", priority.TypeMismatch)
	}
}

func TestProfile_TypeMismatch_TextWithShortValues(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE settings (
			id INTEGER PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
		INSERT INTO settings (id, key, value) VALUES (1, 'theme', 'dark');
		INSERT INTO settings (id, key, value) VALUES (2, 'lang', 'en');
		INSERT INTO settings (id, key, value) VALUES (3, 'timezone', 'UTC+9');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	// key: TEXT with max length 8 ("timezone") → should suggest VARCHAR(8)
	keyCol := colMap["key"]
	if keyCol.TypeMismatch == nil {
		t.Fatal("expected type mismatch for 'key' column (TEXT with short values)")
	}
	if keyCol.TypeMismatch.SuggestedType != "VARCHAR(8)" {
		t.Errorf("expected suggested type VARCHAR(8), got %q", keyCol.TypeMismatch.SuggestedType)
	}

	// value: TEXT with max length 5 ("UTC+9") → should suggest VARCHAR(5)
	valCol := colMap["value"]
	if valCol.TypeMismatch == nil {
		t.Fatal("expected type mismatch for 'value' column (TEXT with short values)")
	}
	if valCol.TypeMismatch.SuggestedType != "VARCHAR(5)" {
		t.Errorf("expected suggested type VARCHAR(5), got %q", valCol.TypeMismatch.SuggestedType)
	}
}

func TestProfile_StringLengthStats(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			code VARCHAR(20) NOT NULL
		);
		INSERT INTO items (id, code) VALUES (1, 'AB');
		INSERT INTO items (id, code) VALUES (2, 'ABCD');
		INSERT INTO items (id, code) VALUES (3, 'ABCDEF');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	codeCol := colMap["code"]
	if codeCol.MaxLength != 6 {
		t.Errorf("expected max length 6, got %d", codeCol.MaxLength)
	}
	// avg length = (2+4+6) / 3 = 4.0
	if math.Abs(codeCol.AvgLength-4.0) > 0.01 {
		t.Errorf("expected avg length 4.0, got %f", codeCol.AvgLength)
	}
}

func TestProfile_MinMaxValues(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE scores (
			id INTEGER PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			score INTEGER NOT NULL
		);
		INSERT INTO scores (id, name, score) VALUES (1, 'Charlie', 90);
		INSERT INTO scores (id, name, score) VALUES (2, 'Alice', 75);
		INSERT INTO scores (id, name, score) VALUES (3, 'Bob', 85);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	nameCol := colMap["name"]
	if nameCol.MinValue == nil || *nameCol.MinValue != "Alice" {
		t.Errorf("expected min value 'Alice', got %v", nameCol.MinValue)
	}
	if nameCol.MaxValue == nil || *nameCol.MaxValue != "Charlie" {
		t.Errorf("expected max value 'Charlie', got %v", nameCol.MaxValue)
	}
}

func TestProfile_EmptyTable(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE empty_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100)
		);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	if tp.RowCount != 0 {
		t.Errorf("expected row count 0, got %d", tp.RowCount)
	}

	colMap := makeColMap(tp.Columns)
	nameCol := colMap["name"]
	if nameCol.NullRate != 0 {
		t.Errorf("expected null rate 0 for empty table, got %f", nameCol.NullRate)
	}
	if nameCol.Cardinality != 0 {
		t.Errorf("expected cardinality 0 for empty table, got %d", nameCol.Cardinality)
	}
	if nameCol.MinValue != nil {
		t.Errorf("expected nil min value for empty table, got %v", nameCol.MinValue)
	}
	if nameCol.MaxValue != nil {
		t.Errorf("expected nil max value for empty table, got %v", nameCol.MaxValue)
	}
	if nameCol.TypeMismatch != nil {
		t.Errorf("expected no type mismatch for empty table, got %+v", nameCol.TypeMismatch)
	}
}

func TestProfile_MultipleTables(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE departments (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		);
		CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			dept_id INTEGER NOT NULL,
			name VARCHAR(100) NOT NULL,
			FOREIGN KEY (dept_id) REFERENCES departments(id)
		);
		INSERT INTO departments (id, name) VALUES (1, 'Engineering');
		INSERT INTO departments (id, name) VALUES (2, 'Sales');
		INSERT INTO employees (id, dept_id, name) VALUES (1, 1, 'Alice');
		INSERT INTO employees (id, dept_id, name) VALUES (2, 1, 'Bob');
		INSERT INTO employees (id, dept_id, name) VALUES (3, 2, 'Charlie');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	if len(result.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(result.Tables))
	}

	tableMap := make(map[string]*profiler.TableProfile)
	for i := range result.Tables {
		tableMap[result.Tables[i].Name] = &result.Tables[i]
	}

	deptTable, ok := tableMap["departments"]
	if !ok {
		t.Fatal("departments table not found in results")
	}
	if deptTable.RowCount != 2 {
		t.Errorf("expected 2 rows in departments, got %d", deptTable.RowCount)
	}

	empTable, ok := tableMap["employees"]
	if !ok {
		t.Fatal("employees table not found in results")
	}
	if empTable.RowCount != 3 {
		t.Errorf("expected 3 rows in employees, got %d", empTable.RowCount)
	}
}

func TestProfile_NoTypeMismatchForLargeVarchar(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE articles (
			id INTEGER PRIMARY KEY,
			title VARCHAR(255) NOT NULL
		);
		INSERT INTO articles (id, title) VALUES (1, 'This is a moderately long title for an article about something interesting and noteworthy');
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	result, err := profiler.Profile(ctx, conn, schema, 100)
	if err != nil {
		t.Fatalf("failed to profile: %v", err)
	}

	tp := result.Tables[0]
	colMap := makeColMap(tp.Columns)

	// Title with actual max >= 50 should NOT trigger type mismatch
	titleCol := colMap["title"]
	if titleCol.TypeMismatch != nil {
		t.Errorf("expected no type mismatch for title with actual max >= 50, got %+v", titleCol.TypeMismatch)
	}
}

// makeColMap creates a map of column name to ColumnProfile for convenient lookups.
func makeColMap(columns []profiler.ColumnProfile) map[string]profiler.ColumnProfile {
	m := make(map[string]profiler.ColumnProfile, len(columns))
	for _, col := range columns {
		m[col.Name] = col
	}
	return m
}

// assertNullRate checks that the named column has the expected null rate.
func assertNullRate(t *testing.T, colMap map[string]profiler.ColumnProfile, name string, expected float64) {
	t.Helper()
	col, ok := colMap[name]
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	if math.Abs(col.NullRate-expected) > 0.01 {
		t.Errorf("column %q: expected null rate %.2f, got %.2f", name, expected, col.NullRate)
	}
}

// assertCardinality checks that the named column has the expected cardinality.
func assertCardinality(t *testing.T, colMap map[string]profiler.ColumnProfile, name string, expected int64) {
	t.Helper()
	col, ok := colMap[name]
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	if col.Cardinality != expected {
		t.Errorf("column %q: expected cardinality %d, got %d", name, expected, col.Cardinality)
	}
}

// assertUniqueRate checks that the named column has the expected unique rate.
func assertUniqueRate(t *testing.T, colMap map[string]profiler.ColumnProfile, name string, expected float64) {
	t.Helper()
	col, ok := colMap[name]
	if !ok {
		t.Fatalf("column %q not found", name)
	}
	if math.Abs(col.UniqueRate-expected) > 0.01 {
		t.Errorf("column %q: expected unique rate %.2f, got %.2f", name, expected, col.UniqueRate)
	}
}
