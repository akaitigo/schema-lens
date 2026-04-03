package analyzer

import (
	"fmt"
	"strings"

	"github.com/akaitigo/schema-lens/internal/connector"
)

const categoryTyping = "Typing"

// blobLikeTypes are types that store large binary data.
var blobLikeTypes = map[string]bool{
	"BLOB":       true,
	"LONGBLOB":   true,
	"MEDIUMBLOB": true,
	"TINYBLOB":   true,
	"BYTEA":      true,
}

// textLikeTypes are types that store unbounded text.
var textLikeTypes = map[string]bool{
	"TEXT":       true,
	"LONGTEXT":   true,
	"MEDIUMTEXT": true,
	"TINYTEXT":   true,
	"CLOB":       true,
}

// excessiveTextThreshold is the number of TEXT/BLOB columns per table that triggers a warning.
const excessiveTextThreshold = 3

// booleanLikePatterns are column name patterns that suggest boolean semantics.
var booleanLikePatterns = []string{
	"is_",
	"has_",
	"can_",
	"should_",
	"flag",
	"enabled",
	"disabled",
	"active",
	"visible",
	"deleted",
	"archived",
	"verified",
	"confirmed",
	"approved",
	"published",
	"locked",
}

// integerTypes are types where boolean intent should be detected.
var integerTypes = map[string]bool{
	"INT":       true,
	"INTEGER":   true,
	"BIGINT":    true,
	"SMALLINT":  true,
	"TINYINT":   true,
	"MEDIUMINT": true,
	"INT4":      true,
	"INT8":      true,
}

// checkTyping detects type usage issues in the schema.
func checkTyping(schema *connector.SchemaInfo) []Issue {
	varchar := checkVarchar255(schema)
	textBlob := checkExcessiveTextBlob(schema)
	intBool := checkIntForBoolean(schema)

	issues := make([]Issue, 0, len(varchar)+len(textBlob)+len(intBool))
	issues = append(issues, varchar...)
	issues = append(issues, textBlob...)
	issues = append(issues, intBool...)
	return issues
}

// checkVarchar255 detects lazy VARCHAR(255) default usage.
func checkVarchar255(schema *connector.SchemaInfo) []Issue {
	var issues []Issue
	for _, table := range schema.Tables {
		var varchar255Cols []string
		for _, col := range table.Columns {
			if isVarchar255(col) {
				varchar255Cols = append(varchar255Cols, col.Name)
			}
		}
		if len(varchar255Cols) == 0 {
			continue
		}
		for _, colName := range varchar255Cols {
			issues = append(issues, Issue{
				Severity: SeverityLow,
				Category: categoryTyping,
				Table:    table.Name,
				Column:   colName,
				Description: fmt.Sprintf(
					"column '%s' uses VARCHAR(255) which is often a lazy default",
					colName,
				),
				Suggestion: "specify an appropriate maximum length based on actual data requirements",
			})
		}
	}
	return issues
}

// isVarchar255 checks if a column is VARCHAR(255).
func isVarchar255(col connector.Column) bool {
	dt := strings.ToUpper(col.DataType)
	isVarchar := strings.HasPrefix(dt, "VARCHAR") || strings.HasPrefix(dt, "CHARACTER VARYING")
	return isVarchar && col.MaxLength != nil && *col.MaxLength == 255
}

// checkExcessiveTextBlob detects tables with too many TEXT or BLOB columns.
func checkExcessiveTextBlob(schema *connector.SchemaInfo) []Issue {
	var issues []Issue
	for _, table := range schema.Tables {
		var textBlobCols []string
		for _, col := range table.Columns {
			upper := strings.ToUpper(col.DataType)
			if textLikeTypes[upper] || blobLikeTypes[upper] {
				textBlobCols = append(textBlobCols, col.Name)
			}
		}
		if len(textBlobCols) >= excessiveTextThreshold {
			issues = append(issues, Issue{
				Severity: SeverityMedium,
				Category: categoryTyping,
				Table:    table.Name,
				Column:   strings.Join(textBlobCols, ", "),
				Description: fmt.Sprintf(
					"table has %d TEXT/BLOB columns (%s); excessive usage impacts query performance",
					len(textBlobCols), strings.Join(textBlobCols, ", "),
				),
				Suggestion: "consider using VARCHAR with appropriate limits, or move large data to object storage",
			})
		}
	}
	return issues
}

// checkIntForBoolean detects integer columns that likely represent boolean values.
func checkIntForBoolean(schema *connector.SchemaInfo) []Issue {
	var issues []Issue
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			if !integerTypes[strings.ToUpper(col.DataType)] {
				continue
			}
			if looksBoolean(col.Name) {
				issues = append(issues, Issue{
					Severity: SeverityLow,
					Category: categoryTyping,
					Table:    table.Name,
					Column:   col.Name,
					Description: fmt.Sprintf(
						"column '%s' uses %s but name suggests boolean semantics",
						col.Name, col.DataType,
					),
					Suggestion: "use BOOLEAN type for clarity and self-documentation",
				})
			}
		}
	}
	return issues
}

// looksBoolean checks if a column name suggests boolean semantics.
func looksBoolean(name string) bool {
	lower := strings.ToLower(name)
	for _, pattern := range booleanLikePatterns {
		if strings.HasPrefix(lower, pattern) || strings.HasSuffix(lower, pattern) || lower == pattern {
			return true
		}
	}
	return false
}
