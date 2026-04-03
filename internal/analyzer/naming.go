package analyzer

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/akaitigo/schema-lens/internal/connector"
)

const categoryNaming = "Naming"

// caseStyle represents a detected naming style.
type caseStyle int

const (
	caseUnknown   caseStyle = iota
	caseSnake               // snake_case
	caseCamel               // camelCase
	casePascal              // PascalCase
	caseLowerFlat           // lowercase (single word, ambiguous)
)

// reservedWords is a set of SQL reserved words commonly misused as identifiers.
var reservedWords = map[string]bool{
	"select":   true,
	"insert":   true,
	"update":   true,
	"delete":   true,
	"table":    true,
	"column":   true,
	"index":    true,
	"group":    true,
	"order":    true,
	"where":    true,
	"from":     true,
	"join":     true,
	"left":     true,
	"right":    true,
	"inner":    true,
	"outer":    true,
	"on":       true,
	"as":       true,
	"and":      true,
	"or":       true,
	"not":      true,
	"null":     true,
	"key":      true,
	"primary":  true,
	"foreign":  true,
	"values":   true,
	"set":      true,
	"create":   true,
	"drop":     true,
	"alter":    true,
	"add":      true,
	"user":     true,
	"role":     true,
	"grant":    true,
	"revoke":   true,
	"check":    true,
	"default":  true,
	"having":   true,
	"limit":    true,
	"offset":   true,
	"between":  true,
	"like":     true,
	"in":       true,
	"exists":   true,
	"case":     true,
	"when":     true,
	"then":     true,
	"else":     true,
	"end":      true,
	"type":     true,
	"comment":  true,
	"database": true,
	"schema":   true,
	"trigger":  true,
	"view":     true,
}

// commonSingulars maps common plurals to their singular form for inconsistency detection.
// We only need a small set -- the check detects mixed singular/plural, not correctness.
var commonPluralSuffixes = []struct {
	plural   string
	singular string
}{
	{"ies", "y"},
	{"ses", "s"},
	{"es", "e"},
	{"s", ""},
}

// camelCasePattern matches identifiers with camelCase transitions.
var camelCasePattern = regexp.MustCompile(`[a-z][A-Z]`)

// checkNaming detects naming convention issues in the schema.
func checkNaming(schema *connector.SchemaInfo) []Issue {
	mixed := checkMixedCase(schema)
	consistency := checkTableNameConsistency(schema)
	reserved := checkReservedWords(schema)

	issues := make([]Issue, 0, len(mixed)+len(consistency)+len(reserved))
	issues = append(issues, mixed...)
	issues = append(issues, consistency...)
	issues = append(issues, reserved...)
	return issues
}

// detectCase determines the naming style of an identifier.
func detectCase(name string) caseStyle {
	if strings.Contains(name, "_") {
		return caseSnake
	}
	if name != "" && unicode.IsUpper(rune(name[0])) && camelCasePattern.MatchString(name) {
		return casePascal
	}
	if camelCasePattern.MatchString(name) {
		return caseCamel
	}
	return caseLowerFlat
}

// styleName returns a human-readable name for a caseStyle.
func styleName(cs caseStyle) string {
	switch cs {
	case caseSnake:
		return "snake_case"
	case caseCamel:
		return "camelCase"
	case casePascal:
		return "PascalCase"
	default:
		return "flat"
	}
}

// checkMixedCase detects mixed naming conventions across tables and columns.
func checkMixedCase(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	// Check table name consistency.
	tableCases := make(map[caseStyle][]string)
	for _, table := range schema.Tables {
		cs := detectCase(table.Name)
		if cs != caseLowerFlat {
			tableCases[cs] = append(tableCases[cs], table.Name)
		}
	}
	if len(tableCases) > 1 {
		parts := make([]string, 0, len(tableCases))
		for cs, names := range tableCases {
			parts = append(parts, fmt.Sprintf("%s: %s", styleName(cs), strings.Join(names, ", ")))
		}
		issues = append(issues, Issue{
			Severity:    SeverityMedium,
			Category:    categoryNaming,
			Description: fmt.Sprintf("mixed naming conventions in table names (%s)", strings.Join(parts, "; ")),
			Suggestion:  "standardize all table names to use snake_case for consistency",
		})
	}

	// Check column name consistency within each table.
	for _, table := range schema.Tables {
		colCases := make(map[caseStyle][]string)
		for _, col := range table.Columns {
			cs := detectCase(col.Name)
			if cs != caseLowerFlat {
				colCases[cs] = append(colCases[cs], col.Name)
			}
		}
		if len(colCases) > 1 {
			parts := make([]string, 0, len(colCases))
			for cs, names := range colCases {
				parts = append(parts, fmt.Sprintf("%s: %s", styleName(cs), strings.Join(names, ", ")))
			}
			issues = append(issues, Issue{
				Severity:    SeverityMedium,
				Category:    categoryNaming,
				Table:       table.Name,
				Description: fmt.Sprintf("mixed naming conventions in columns (%s)", strings.Join(parts, "; ")),
				Suggestion:  "standardize all column names within the table to use snake_case",
			})
		}
	}

	return issues
}

// looksPlural returns true if the table name appears to be plural.
func looksPlural(name string) bool {
	lower := strings.ToLower(name)
	// Avoid false positives for common words ending in 's' that aren't plural.
	nonPlurals := map[string]bool{
		"status":  true,
		"address": true,
		"process": true,
		"access":  true,
		"class":   true,
		"bus":     true,
	}
	if nonPlurals[lower] {
		return false
	}
	for _, suffix := range commonPluralSuffixes {
		if strings.HasSuffix(lower, suffix.plural) && len(lower) > len(suffix.plural) {
			return true
		}
	}
	return false
}

// checkTableNameConsistency detects mixed singular/plural table names.
func checkTableNameConsistency(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	if len(schema.Tables) < 2 {
		return issues
	}

	var singularTables, pluralTables []string
	for _, table := range schema.Tables {
		if looksPlural(table.Name) {
			pluralTables = append(pluralTables, table.Name)
		} else {
			singularTables = append(singularTables, table.Name)
		}
	}

	if len(singularTables) > 0 && len(pluralTables) > 0 {
		issues = append(issues, Issue{
			Severity: SeverityLow,
			Category: categoryNaming,
			Description: fmt.Sprintf(
				"mixed singular/plural table names: singular=[%s], plural=[%s]",
				strings.Join(singularTables, ", "), strings.Join(pluralTables, ", "),
			),
			Suggestion: "choose either singular or plural table names and apply consistently",
		})
	}

	return issues
}

// checkReservedWords detects table or column names that are SQL reserved words.
func checkReservedWords(schema *connector.SchemaInfo) []Issue {
	var issues []Issue

	for _, table := range schema.Tables {
		if reservedWords[strings.ToLower(table.Name)] {
			issues = append(issues, Issue{
				Severity:    SeverityMedium,
				Category:    categoryNaming,
				Table:       table.Name,
				Description: fmt.Sprintf("table name '%s' is a SQL reserved word", table.Name),
				Suggestion:  fmt.Sprintf("rename table '%s' to avoid conflicts with SQL syntax", table.Name),
			})
		}
		for _, col := range table.Columns {
			if reservedWords[strings.ToLower(col.Name)] {
				issues = append(issues, Issue{
					Severity:    SeverityLow,
					Category:    categoryNaming,
					Table:       table.Name,
					Column:      col.Name,
					Description: fmt.Sprintf("column name '%s' is a SQL reserved word", col.Name),
					Suggestion:  fmt.Sprintf("rename column '%s' to avoid potential quoting issues", col.Name),
				})
			}
		}
	}

	return issues
}
