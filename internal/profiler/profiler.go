// Package profiler provides column-level data profiling for database tables.
//
// It samples data from each table and computes per-column statistics such as
// NULL rate, cardinality, string lengths, and type mismatch detection to help
// identify schema improvements based on actual data patterns.
package profiler

import (
	"context"
	"fmt"
	"strings"

	"github.com/akaitigo/schema-lens/internal/connector"
)

// ProfileResult holds the complete profiling output for all tables.
type ProfileResult struct {
	Tables []TableProfile
}

// TableProfile holds profiling results for a single table.
type TableProfile struct {
	Name       string
	Columns    []ColumnProfile
	RowCount   int64
	SampleSize int
}

// ColumnProfile holds per-column statistics derived from sampled data.
type ColumnProfile struct {
	TypeMismatch *TypeMismatchInfo
	MinValue     *string
	MaxValue     *string
	Name         string
	DataType     string
	NullRate     float64
	UniqueRate   float64
	AvgLength    float64
	Cardinality  int64
	MaxLength    int
}

// TypeMismatchInfo describes a mismatch between a column's declared type and
// the actual data it contains, along with a suggested alternative type.
type TypeMismatchInfo struct {
	DeclaredType  string
	SuggestedType string
	Description   string
	ActualMaxLen  int
}

// Profile samples data from each table in the schema and computes column-level
// statistics. The sampleSize parameter controls how many rows are sampled per table.
func Profile(ctx context.Context, conn connector.Connector, schema *connector.SchemaInfo, sampleSize int) (*ProfileResult, error) {
	result := &ProfileResult{}

	for _, table := range schema.Tables {
		tp, err := profileTable(ctx, conn, &table, sampleSize)
		if err != nil {
			return nil, fmt.Errorf("profiling table %s: %w", table.Name, err)
		}
		result.Tables = append(result.Tables, *tp)
	}

	return result, nil
}

// profileTable profiles a single table by sampling rows and computing column stats.
func profileTable(ctx context.Context, conn connector.Connector, table *connector.Table, sampleSize int) (*TableProfile, error) {
	rows, err := conn.SampleData(ctx, table.Name, sampleSize)
	if err != nil {
		return nil, fmt.Errorf("sampling data: %w", err)
	}

	tp := &TableProfile{
		Name:       table.Name,
		RowCount:   int64(len(rows)),
		SampleSize: sampleSize,
	}

	for _, col := range table.Columns {
		cp := profileColumn(&col, rows)
		tp.Columns = append(tp.Columns, cp)
	}

	return tp, nil
}

// columnStats holds intermediate statistics collected from sampled rows.
type columnStats struct {
	distinctValues map[string]struct{}
	minVal         string
	maxVal         string
	totalLength    int64
	nullCount      int
	total          int
	maxLen         int
	hasMinMax      bool
}

// profileColumn computes statistics for a single column from the sampled rows.
func profileColumn(col *connector.Column, rows []map[string]any) ColumnProfile {
	cp := ColumnProfile{
		Name:     col.Name,
		DataType: col.DataType,
	}

	if len(rows) == 0 {
		return cp
	}

	stats := collectColumnStats(col, rows)
	applyStats(&cp, col, &stats)
	cp.TypeMismatch = detectTypeMismatch(col, rows)

	return cp
}

// collectColumnStats iterates over sampled rows and gathers raw statistics for a column.
func collectColumnStats(col *connector.Column, rows []map[string]any) columnStats {
	stats := columnStats{
		distinctValues: make(map[string]struct{}),
		total:          len(rows),
	}

	isStr := isStringType(col.DataType)

	for _, row := range rows {
		val, exists := row[col.Name]
		if !exists || val == nil {
			stats.nullCount++
			continue
		}

		strVal := toString(val)
		stats.distinctValues[strVal] = struct{}{}

		if isStr {
			length := len(strVal)
			stats.totalLength += int64(length)
			if length > stats.maxLen {
				stats.maxLen = length
			}
		}

		updateMinMax(&stats, strVal)
	}

	return stats
}

// updateMinMax tracks the lexicographic min and max values.
func updateMinMax(stats *columnStats, strVal string) {
	if !stats.hasMinMax {
		stats.minVal = strVal
		stats.maxVal = strVal
		stats.hasMinMax = true
		return
	}
	if strVal < stats.minVal {
		stats.minVal = strVal
	}
	if strVal > stats.maxVal {
		stats.maxVal = strVal
	}
}

// applyStats populates a ColumnProfile from the collected columnStats.
func applyStats(cp *ColumnProfile, col *connector.Column, stats *columnStats) {
	cp.NullRate = float64(stats.nullCount) / float64(stats.total)
	cp.Cardinality = int64(len(stats.distinctValues))

	nonNullCount := stats.total - stats.nullCount
	if nonNullCount > 0 {
		cp.UniqueRate = float64(len(stats.distinctValues)) / float64(nonNullCount)
	}

	if stats.hasMinMax {
		cp.MinValue = &stats.minVal
		cp.MaxValue = &stats.maxVal
	}

	if isStringType(col.DataType) && nonNullCount > 0 {
		cp.AvgLength = float64(stats.totalLength) / float64(nonNullCount)
		cp.MaxLength = stats.maxLen
	}
}

// detectTypeMismatch checks whether a column's declared type is appropriate
// for its actual data, and returns a suggestion if a better type exists.
func detectTypeMismatch(col *connector.Column, rows []map[string]any) *TypeMismatchInfo {
	if len(rows) == 0 {
		return nil
	}

	upperType := strings.ToUpper(col.DataType)

	// VARCHAR(255) with actual max < 50 → suggest VARCHAR(actual_max)
	if isVarcharWithLength(upperType) && col.MaxLength != nil && *col.MaxLength >= 255 {
		actualMax := computeMaxStringLength(col.Name, rows)
		if actualMax > 0 && actualMax < 50 {
			return &TypeMismatchInfo{
				DeclaredType:  col.DataType,
				ActualMaxLen:  actualMax,
				SuggestedType: fmt.Sprintf("VARCHAR(%d)", actualMax),
				Description:   fmt.Sprintf("declared as %s but actual max length is %d", col.DataType, actualMax),
			}
		}
	}

	// INT where all values are 0 or 1 → suggest BOOLEAN
	if isIntegerType(upperType) {
		if allBooleanValues(col.Name, rows) {
			return &TypeMismatchInfo{
				DeclaredType:  col.DataType,
				ActualMaxLen:  0,
				SuggestedType: "BOOLEAN",
				Description:   fmt.Sprintf("declared as %s but all values are 0 or 1, suggesting boolean semantics", col.DataType),
			}
		}
	}

	// TEXT with max length < 100 → suggest VARCHAR(max_len)
	if isUnboundedTextType(upperType) {
		actualMax := computeMaxStringLength(col.Name, rows)
		if actualMax > 0 && actualMax < 100 {
			return &TypeMismatchInfo{
				DeclaredType:  col.DataType,
				ActualMaxLen:  actualMax,
				SuggestedType: fmt.Sprintf("VARCHAR(%d)", actualMax),
				Description:   fmt.Sprintf("declared as %s but actual max length is %d; bounded VARCHAR is more efficient", col.DataType, actualMax),
			}
		}
	}

	return nil
}

// toString converts a value from SampleData into its string representation.
func toString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// isStringType returns true if the data type stores variable-length text.
func isStringType(dataType string) bool {
	upper := strings.ToUpper(dataType)
	return strings.HasPrefix(upper, "VARCHAR") ||
		strings.HasPrefix(upper, "CHARACTER VARYING") ||
		upper == "TEXT" ||
		upper == "LONGTEXT" ||
		upper == "MEDIUMTEXT" ||
		upper == "TINYTEXT" ||
		upper == "CLOB"
}

// isVarcharWithLength returns true if the type is a VARCHAR-like type with a length specifier.
func isVarcharWithLength(upperType string) bool {
	return strings.HasPrefix(upperType, "VARCHAR") ||
		strings.HasPrefix(upperType, "CHARACTER VARYING")
}

// isIntegerType returns true if the type is an integer type.
func isIntegerType(upperType string) bool {
	intTypes := map[string]bool{
		"INT":       true,
		"INTEGER":   true,
		"BIGINT":    true,
		"SMALLINT":  true,
		"TINYINT":   true,
		"MEDIUMINT": true,
		"INT4":      true,
		"INT8":      true,
	}
	return intTypes[upperType]
}

// isUnboundedTextType returns true if the type is an unbounded text type.
func isUnboundedTextType(upperType string) bool {
	textTypes := map[string]bool{
		"TEXT":       true,
		"LONGTEXT":   true,
		"MEDIUMTEXT": true,
		"TINYTEXT":   true,
		"CLOB":       true,
	}
	return textTypes[upperType]
}

// computeMaxStringLength computes the maximum string length for a column across sampled rows.
func computeMaxStringLength(colName string, rows []map[string]any) int {
	maxLen := 0
	for _, row := range rows {
		val, exists := row[colName]
		if !exists || val == nil {
			continue
		}
		strVal := toString(val)
		if len(strVal) > maxLen {
			maxLen = len(strVal)
		}
	}
	return maxLen
}

// allBooleanValues returns true if every non-null value in the column is 0 or 1
// (as integer or string representation).
func allBooleanValues(colName string, rows []map[string]any) bool {
	nonNullCount := 0
	for _, row := range rows {
		val, exists := row[colName]
		if !exists || val == nil {
			continue
		}
		nonNullCount++
		if !isBooleanValue(val) {
			return false
		}
	}
	return nonNullCount > 0
}

// isBooleanValue checks if a value represents a boolean (0 or 1).
func isBooleanValue(val any) bool {
	switch v := val.(type) {
	case int64:
		return v == 0 || v == 1
	case int:
		return v == 0 || v == 1
	case float64:
		return v == 0.0 || v == 1.0
	case string:
		return v == "0" || v == "1"
	case []byte:
		s := string(v)
		return s == "0" || s == "1"
	default:
		return false
	}
}
