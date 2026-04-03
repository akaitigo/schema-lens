package reporter

import (
	"fmt"
	"strings"

	"github.com/akaitigo/schema-lens/internal/analyzer"
)

// generateSQLForIssue produces a migration SQL statement for a given analyzer issue.
// Returns an empty string if no automated SQL fix is applicable.
func generateSQLForIssue(issue *analyzer.Issue) string {
	switch issue.Category {
	case "Typing":
		return generateTypingSQL(issue)
	case "Indexing":
		return generateIndexingSQL(issue)
	default:
		return ""
	}
}

// generateTypingSQL generates ALTER TABLE statements for typing issues.
func generateTypingSQL(issue *analyzer.Issue) string {
	if issue.Table == "" || issue.Column == "" {
		return ""
	}

	desc := strings.ToLower(issue.Description)

	// VARCHAR(255) lazy default: suggest a resize (we use a conservative placeholder
	// since static analysis doesn't know the actual max length).
	if strings.Contains(desc, "varchar(255)") {
		return generateAlterColumnTypeSQL(issue.Table, issue.Column, "VARCHAR(255) -- resize based on actual data")
	}

	// INT used for boolean
	if strings.Contains(desc, "boolean") {
		return generateAlterColumnTypeSQL(issue.Table, issue.Column, "BOOLEAN")
	}

	return ""
}

// generateIndexingSQL generates CREATE INDEX or ALTER TABLE statements for indexing issues.
func generateIndexingSQL(issue *analyzer.Issue) string {
	if issue.Table == "" {
		return ""
	}

	desc := strings.ToLower(issue.Description)

	// Foreign key without supporting index
	if strings.Contains(desc, "no supporting index") {
		return generateCreateIndexSQL(issue.Table, splitColumns(issue.Column))
	}

	// Duplicate index: generate DROP INDEX
	if strings.Contains(desc, "duplicate") {
		indexName := extractFirstQuotedName(issue.Description)
		if indexName != "" {
			return generateDropIndexSQL(indexName)
		}
	}

	// Redundant left-prefix index: generate DROP INDEX
	if strings.Contains(desc, "redundant") {
		indexName := extractFirstQuotedName(issue.Description)
		if indexName != "" {
			return generateDropIndexSQL(indexName)
		}
	}

	return ""
}

// generateAlterColumnTypeSQL generates an ALTER TABLE ... ALTER COLUMN ... TYPE statement.
func generateAlterColumnTypeSQL(table, column, newType string) string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", table, column, newType)
}

// generateCreateIndexSQL generates a CREATE INDEX statement for the given table and columns.
func generateCreateIndexSQL(table string, columns []string) string {
	idxName := fmt.Sprintf("idx_%s_%s", table, strings.Join(columns, "_"))
	colList := strings.Join(columns, ", ")
	return fmt.Sprintf("CREATE INDEX %s ON %s(%s);", idxName, table, colList)
}

// GenerateAddForeignKeySQL generates an ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statement.
// It is exported for direct use in migration tooling.
func GenerateAddForeignKeySQL(table, column, refTable, refColumn string) string {
	constraintName := fmt.Sprintf("fk_%s_%s", table, column)
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);",
		table, constraintName, column, refTable, refColumn)
}

// generateDropIndexSQL generates a DROP INDEX statement.
func generateDropIndexSQL(indexName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s;", indexName)
}

// splitColumns splits a comma-separated column list into individual column names.
func splitColumns(cols string) []string {
	parts := strings.Split(cols, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// extractFirstQuotedName extracts the first single-quoted name from a string.
// For example, "index 'idx_foo' is a duplicate" returns "idx_foo".
func extractFirstQuotedName(s string) string {
	start := strings.Index(s, "'")
	if start < 0 {
		return ""
	}
	end := strings.Index(s[start+1:], "'")
	if end < 0 {
		return ""
	}
	return s[start+1 : start+1+end]
}
