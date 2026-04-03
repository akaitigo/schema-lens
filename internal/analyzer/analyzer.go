// Package analyzer provides schema quality analysis and scoring.
//
// It inspects a database schema for common anti-patterns in normalization,
// naming conventions, type usage, and indexing, then produces a scored report
// with actionable suggestions.
package analyzer

import (
	"github.com/akaitigo/schema-lens/internal/connector"
)

// Severity represents the severity level of a detected issue.
type Severity int

const (
	// SeverityLow indicates a minor style or preference issue.
	SeverityLow Severity = iota
	// SeverityMedium indicates a moderate issue that may cause maintenance problems.
	SeverityMedium
	// SeverityHigh indicates a serious issue that likely causes bugs or performance problems.
	SeverityHigh
)

// String returns the human-readable name of the severity level.
func (s Severity) String() string {
	switch s {
	case SeverityLow:
		return "LOW"
	case SeverityMedium:
		return "MEDIUM"
	case SeverityHigh:
		return "HIGH"
	default:
		return "UNKNOWN"
	}
}

// deduction returns the score penalty for this severity level.
func (s Severity) deduction() float64 {
	switch s {
	case SeverityLow:
		return 2
	case SeverityMedium:
		return 5
	case SeverityHigh:
		return 10
	default:
		return 0
	}
}

// Issue represents a single quality issue found in the schema.
type Issue struct {
	Category    string
	Table       string
	Column      string
	Description string
	Suggestion  string
	Severity    Severity
}

// CategoryScore represents the quality score for a specific analysis category.
type CategoryScore struct {
	Name   string
	Issues []Issue
	Score  float64
}

// AnalysisResult holds the complete analysis output.
type AnalysisResult struct {
	Categories   []CategoryScore
	Issues       []Issue
	OverallScore float64
}

// Category weights for overall score calculation.
const (
	weightNormalization = 0.30
	weightNaming        = 0.20
	weightTyping        = 0.25
	weightIndexing      = 0.25
)

// checker is the function signature for each category's check function.
type checker func(schema *connector.SchemaInfo) []Issue

// Analyze runs all quality checks against the given schema and returns a scored result.
func Analyze(schema *connector.SchemaInfo) *AnalysisResult {
	type categoryDef struct {
		checker checker
		name    string
		weight  float64
	}

	categories := []categoryDef{
		{name: "Normalization", checker: checkNormalization, weight: weightNormalization},
		{name: "Naming", checker: checkNaming, weight: weightNaming},
		{name: "Typing", checker: checkTyping, weight: weightTyping},
		{name: "Indexing", checker: checkIndexing, weight: weightIndexing},
	}

	result := &AnalysisResult{}
	weightedSum := 0.0
	totalWeight := 0.0

	for _, cat := range categories {
		issues := cat.checker(schema)
		score := calculateScore(issues)

		cs := CategoryScore{
			Name:   cat.name,
			Score:  score,
			Issues: issues,
		}
		result.Categories = append(result.Categories, cs)
		result.Issues = append(result.Issues, issues...)

		weightedSum += score * cat.weight
		totalWeight += cat.weight
	}

	if totalWeight > 0 {
		result.OverallScore = weightedSum / totalWeight
	}

	return result
}

// calculateScore computes a category score starting at 100 and deducting by severity.
func calculateScore(issues []Issue) float64 {
	score := 100.0
	for _, issue := range issues {
		score -= issue.Severity.deduction()
	}
	if score < 0 {
		score = 0
	}
	return score
}
