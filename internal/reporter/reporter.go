// Package reporter generates improvement proposals, migration SQL, and
// formatted output from schema analysis and profiling results.
package reporter

import (
	"fmt"
	"time"

	"github.com/akaitigo/schema-lens/internal/analyzer"
	"github.com/akaitigo/schema-lens/internal/profiler"
)

// Report holds the complete report output including summary, scores, proposals, and SQL.
type Report struct {
	Scores       []analyzer.CategoryScore `json:"scores"`
	Proposals    []Proposal               `json:"proposals"`
	MigrationSQL []string                 `json:"migration_sql"`
	Summary      Summary                  `json:"summary"`
}

// Summary holds high-level metadata about the analysis run.
type Summary struct {
	AnalyzedAt   time.Time `json:"analyzed_at"`
	DatabaseDSN  string    `json:"database_dsn"`
	OverallScore float64   `json:"overall_score"`
	TableCount   int       `json:"table_count"`
	ColumnCount  int       `json:"column_count"`
	IssueCount   int       `json:"issue_count"`
}

// Proposal represents a single improvement proposal with optional migration SQL.
type Proposal struct {
	Table         string            `json:"table"`
	Column        string            `json:"column"`
	Category      string            `json:"category"`
	Description   string            `json:"description"`
	CurrentState  string            `json:"current_state"`
	ProposedState string            `json:"proposed_state"`
	SQL           string            `json:"sql,omitempty"`
	Priority      analyzer.Severity `json:"priority"`
}

// GenerateReport creates a Report from analysis results and optional profiling data.
// The dsn parameter is recorded in the summary. profile may be nil if profiling was not run.
func GenerateReport(dsn string, analysis *analyzer.AnalysisResult, profile *profiler.ProfileResult) *Report {
	tableCount, columnCount := countSchemaObjects(analysis, profile)

	report := &Report{
		Summary: Summary{
			DatabaseDSN:  dsn,
			AnalyzedAt:   time.Now(),
			TableCount:   tableCount,
			ColumnCount:  columnCount,
			OverallScore: analysis.OverallScore,
			IssueCount:   len(analysis.Issues),
		},
		Scores: analysis.Categories,
	}

	proposals := buildProposalsFromIssues(analysis.Issues)

	if profile != nil {
		profileProposals := buildProposalsFromProfile(profile)
		proposals = append(proposals, profileProposals...)
	}

	sortProposalsByPriority(proposals)

	report.Proposals = proposals
	report.MigrationSQL = collectMigrationSQL(proposals)

	return report
}

// countSchemaObjects counts tables and columns from analysis issues and profile results.
func countSchemaObjects(analysis *analyzer.AnalysisResult, profile *profiler.ProfileResult) (tables, columns int) {
	if profile != nil {
		tables = len(profile.Tables)
		for _, tp := range profile.Tables {
			columns += len(tp.Columns)
		}
		return tables, columns
	}

	// Estimate from issues when profile is not available.
	tableSet := make(map[string]struct{})
	columnSet := make(map[string]struct{})
	for _, issue := range analysis.Issues {
		if issue.Table != "" {
			tableSet[issue.Table] = struct{}{}
			if issue.Column != "" {
				key := fmt.Sprintf("%s.%s", issue.Table, issue.Column)
				columnSet[key] = struct{}{}
			}
		}
	}
	return len(tableSet), len(columnSet)
}

// buildProposalsFromIssues converts analyzer issues into proposals with SQL when applicable.
func buildProposalsFromIssues(issues []analyzer.Issue) []Proposal {
	proposals := make([]Proposal, 0, len(issues))
	for _, issue := range issues {
		p := Proposal{
			Priority:      issue.Severity,
			Table:         issue.Table,
			Column:        issue.Column,
			Category:      issue.Category,
			Description:   issue.Description,
			CurrentState:  issue.Description,
			ProposedState: issue.Suggestion,
		}
		p.SQL = generateSQLForIssue(&issue)
		proposals = append(proposals, p)
	}
	return proposals
}

// buildProposalsFromProfile creates proposals from profiling type-mismatch detections.
func buildProposalsFromProfile(profile *profiler.ProfileResult) []Proposal {
	var proposals []Proposal
	for _, tp := range profile.Tables {
		for _, cp := range tp.Columns {
			if cp.TypeMismatch == nil {
				continue
			}
			tm := cp.TypeMismatch
			p := Proposal{
				Priority:      analyzer.SeverityLow,
				Table:         tp.Name,
				Column:        cp.Name,
				Category:      "Typing",
				Description:   tm.Description,
				CurrentState:  fmt.Sprintf("%s (declared as %s)", cp.Name, tm.DeclaredType),
				ProposedState: fmt.Sprintf("change type to %s", tm.SuggestedType),
				SQL:           generateAlterColumnTypeSQL(tp.Name, cp.Name, tm.SuggestedType),
			}
			proposals = append(proposals, p)
		}
	}
	return proposals
}

// sortProposalsByPriority sorts proposals from highest to lowest severity.
func sortProposalsByPriority(proposals []Proposal) {
	for i := 1; i < len(proposals); i++ {
		key := proposals[i]
		j := i - 1
		for j >= 0 && proposals[j].Priority < key.Priority {
			proposals[j+1] = proposals[j]
			j--
		}
		proposals[j+1] = key
	}
}

// collectMigrationSQL extracts all non-empty SQL statements from proposals.
func collectMigrationSQL(proposals []Proposal) []string {
	var sqls []string
	for _, p := range proposals {
		if p.SQL != "" {
			sqls = append(sqls, p.SQL)
		}
	}
	return sqls
}
