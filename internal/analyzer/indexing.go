package analyzer

import (
	"fmt"
	"strings"

	"github.com/akaitigo/schema-lens/internal/connector"
)

const categoryIndexing = "Indexing"

// checkIndexing detects indexing issues in the schema.
func checkIndexing(schema *connector.SchemaInfo) []Issue {
	fkIdx := checkForeignKeyIndexes(schema)
	leftPrefix := checkLeftPrefixViolations(schema)
	dupes := checkDuplicateIndexes(schema)

	issues := make([]Issue, 0, len(fkIdx)+len(leftPrefix)+len(dupes))
	issues = append(issues, fkIdx...)
	issues = append(issues, leftPrefix...)
	issues = append(issues, dupes...)
	return issues
}

// checkForeignKeyIndexes detects foreign keys that lack a supporting index.
func checkForeignKeyIndexes(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		indexedPrefixes := buildIndexPrefixes(table.Indexes)

		for _, fk := range table.ForeignKeys {
			fkKey := normalizeColumns(fk.Columns)
			if !indexedPrefixes[fkKey] {
				issues = append(issues, Issue{
					Severity: SeverityHigh,
					Category: categoryIndexing,
					Table:    table.Name,
					Column:   strings.Join(fk.Columns, ", "),
					Description: fmt.Sprintf(
						"foreign key on (%s) referencing %s(%s) has no supporting index",
						strings.Join(fk.Columns, ", "),
						fk.ReferencedTable,
						strings.Join(fk.ReferencedColumns, ", "),
					),
					Suggestion: fmt.Sprintf(
						"create an index on %s(%s) to improve JOIN and DELETE performance",
						table.Name, strings.Join(fk.Columns, ", "),
					),
				})
			}
		}
	}

	return issues
}

// buildIndexPrefixes returns a set of all left-prefix column combinations covered by indexes.
// For example, an index on (a, b, c) covers "a", "a,b", and "a,b,c".
func buildIndexPrefixes(indexes []connector.Index) map[string]bool {
	prefixes := make(map[string]bool)
	for _, idx := range indexes {
		for i := 1; i <= len(idx.Columns); i++ {
			prefix := normalizeColumns(idx.Columns[:i])
			prefixes[prefix] = true
		}
	}
	return prefixes
}

// checkLeftPrefixViolations detects composite indexes where a query might use only
// non-leading columns, meaning there is no single-column index for those columns.
//
// Specifically: if a composite index covers (a, b) but there is a separate index on
// just (b), the (b) index may be redundant or the composite may need reordering.
// This check flags single-column indexes that are already covered as a non-leading
// prefix of a composite index.
func checkLeftPrefixViolations(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		// Collect all composite indexes.
		var composites []connector.Index
		singleCols := make(map[string]string) // col -> index name

		for _, idx := range table.Indexes {
			if len(idx.Columns) > 1 {
				composites = append(composites, idx)
			} else if len(idx.Columns) == 1 {
				singleCols[strings.ToLower(idx.Columns[0])] = idx.Name
			}
		}

		// Check if any single-column index is already the leading column of a composite.
		for _, comp := range composites {
			if len(comp.Columns) == 0 {
				continue
			}
			leadCol := strings.ToLower(comp.Columns[0])
			if singleIdxName, ok := singleCols[leadCol]; ok {
				issues = append(issues, Issue{
					Severity: SeverityLow,
					Category: categoryIndexing,
					Table:    table.Name,
					Column:   comp.Columns[0],
					Description: fmt.Sprintf(
						"index '%s' on (%s) is redundant; composite index '%s' on (%s) already covers it as a left prefix",
						singleIdxName, comp.Columns[0],
						comp.Name, strings.Join(comp.Columns, ", "),
					),
					Suggestion: fmt.Sprintf(
						"consider dropping index '%s' since '%s' provides the same coverage",
						singleIdxName, comp.Name,
					),
				})
			}
		}
	}

	return issues
}

// checkDuplicateIndexes detects indexes that cover the exact same columns.
func checkDuplicateIndexes(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		seen := make(map[string]string) // normalized cols -> first index name

		for _, idx := range table.Indexes {
			key := normalizeColumns(idx.Columns)
			if existing, ok := seen[key]; ok {
				issues = append(issues, Issue{
					Severity: SeverityMedium,
					Category: categoryIndexing,
					Table:    table.Name,
					Column:   strings.Join(idx.Columns, ", "),
					Description: fmt.Sprintf(
						"index '%s' is a duplicate of '%s'; both cover (%s)",
						idx.Name, existing, strings.Join(idx.Columns, ", "),
					),
					Suggestion: fmt.Sprintf(
						"drop one of the duplicate indexes to reduce write overhead: '%s' or '%s'",
						idx.Name, existing,
					),
				})
			} else {
				seen[key] = idx.Name
			}
		}
	}

	return issues
}

// normalizeColumns creates a canonical string key from a list of column names.
func normalizeColumns(cols []string) string {
	normalized := make([]string, len(cols))
	for i, c := range cols {
		normalized[i] = strings.ToLower(c)
	}
	return strings.Join(normalized, ",")
}
