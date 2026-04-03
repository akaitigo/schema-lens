package analyzer

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/akaitigo/schema-lens/internal/connector"
)

const categoryNormalization = "Normalization"

// repeatingPattern matches column names ending with a digit (e.g., addr1, addr2, phone_3).
var repeatingPattern = regexp.MustCompile(`^(.+?)_?(\d+)$`)

// jsonTypes contains data types that indicate JSON storage.
var jsonTypes = map[string]bool{
	"JSON":  true,
	"JSONB": true,
}

// excessiveJSONThreshold is the number of JSON columns in a single table
// that triggers a warning.
const excessiveJSONThreshold = 3

// checkNormalization detects normalization issues in the schema.
func checkNormalization(schema *connector.SchemaInfo) []Issue {
	repeating := checkRepeatingColumns(schema)
	json := checkExcessiveJSON(schema)
	dupes := checkDuplicateColumnNames(schema)

	issues := make([]Issue, 0, len(repeating)+len(json)+len(dupes))
	issues = append(issues, repeating...)
	issues = append(issues, json...)
	issues = append(issues, dupes...)
	return issues
}

// checkRepeatingColumns detects repeating column name patterns like addr1, addr2, addr3.
func checkRepeatingColumns(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		// Group columns by their base name (without trailing digits).
		groups := make(map[string][]string)
		for _, col := range table.Columns {
			matches := repeatingPattern.FindStringSubmatch(col.Name)
			if matches != nil {
				base := strings.ToLower(matches[1])
				groups[base] = append(groups[base], col.Name)
			}
		}

		for base, cols := range groups {
			if len(cols) < 2 {
				continue
			}
			issues = append(issues, Issue{
				Severity: SeverityHigh,
				Category: categoryNormalization,
				Table:    table.Name,
				Column:   strings.Join(cols, ", "),
				Description: fmt.Sprintf(
					"repeating column pattern detected: %d columns with base name '%s' (%s)",
					len(cols), base, strings.Join(cols, ", "),
				),
				Suggestion: fmt.Sprintf(
					"extract '%s' into a separate table with a foreign key relationship",
					base,
				),
			})
		}
	}

	return issues
}

// checkExcessiveJSON detects tables with too many JSON/JSONB columns.
func checkExcessiveJSON(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		var jsonCols []string
		for _, col := range table.Columns {
			if jsonTypes[strings.ToUpper(col.DataType)] {
				jsonCols = append(jsonCols, col.Name)
			}
		}
		if len(jsonCols) >= excessiveJSONThreshold {
			issues = append(issues, Issue{
				Severity: SeverityMedium,
				Category: categoryNormalization,
				Table:    table.Name,
				Column:   strings.Join(jsonCols, ", "),
				Description: fmt.Sprintf(
					"table has %d JSON columns (%s); excessive JSON usage may indicate missing normalization",
					len(jsonCols), strings.Join(jsonCols, ", "),
				),
				Suggestion: "consider extracting JSON data into properly typed relational columns or separate tables",
			})
		}
	}

	return issues
}

// checkDuplicateColumnNames detects the same column name appearing in multiple tables,
// which may indicate denormalization (excluding common columns like id, created_at, etc.).
func checkDuplicateColumnNames(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	// Common columns that are expected to appear in multiple tables.
	commonColumns := map[string]bool{
		"id":         true,
		"created_at": true,
		"updated_at": true,
		"deleted_at": true,
		"created_by": true,
		"updated_by": true,
		"name":       true,
		"status":     true,
		"type":       true,
	}

	// Map column name -> list of table names.
	colTables := make(map[string][]string)
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			lower := strings.ToLower(col.Name)
			if !commonColumns[lower] && !col.IsPrimaryKey {
				colTables[lower] = append(colTables[lower], table.Name)
			}
		}
	}

	for colName, tables := range colTables {
		if len(tables) < 2 {
			continue
		}
		issues = append(issues, Issue{
			Severity: SeverityLow,
			Category: categoryNormalization,
			Column:   colName,
			Description: fmt.Sprintf(
				"column '%s' appears in %d tables (%s); this may indicate denormalization",
				colName, len(tables), strings.Join(tables, ", "),
			),
			Suggestion: "verify whether this data is duplicated or if a join via foreign key would be more appropriate",
		})
	}

	return issues
}
