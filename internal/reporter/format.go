package reporter

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// FormatTable writes the report as a human-readable table to the given writer.
func FormatTable(w io.Writer, report *Report) error {
	// Summary
	fmt.Fprintln(w, "=== Schema Analysis Report ===")
	fmt.Fprintf(w, "Database:     %s\n", report.Summary.DatabaseDSN)
	fmt.Fprintf(w, "Analyzed at:  %s\n", report.Summary.AnalyzedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Tables:       %d\n", report.Summary.TableCount)
	fmt.Fprintf(w, "Columns:      %d\n", report.Summary.ColumnCount)
	fmt.Fprintf(w, "Overall Score: %.1f / 100\n", report.Summary.OverallScore)
	fmt.Fprintf(w, "Issues:       %d\n", report.Summary.IssueCount)
	fmt.Fprintln(w)

	// Category Scores
	fmt.Fprintln(w, "--- Category Scores ---")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Category\tScore\tIssues\n")
	fmt.Fprintf(tw, "--------\t-----\t------\n")
	for _, cs := range report.Scores {
		fmt.Fprintf(tw, "%s\t%.1f\t%d\n", cs.Name, cs.Score, len(cs.Issues))
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flushing category scores: %w", err)
	}
	fmt.Fprintln(w)

	// Proposals
	if len(report.Proposals) > 0 {
		fmt.Fprintln(w, "--- Improvement Proposals ---")
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintf(tw, "Priority\tTable\tColumn\tCategory\tDescription\n")
		fmt.Fprintf(tw, "--------\t-----\t------\t--------\t-----------\n")
		for _, p := range report.Proposals {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				p.Priority, p.Table, p.Column, p.Category, p.Description)
		}
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("flushing proposals: %w", err)
		}
		fmt.Fprintln(w)
	}

	// Migration SQL
	if len(report.MigrationSQL) > 0 {
		fmt.Fprintln(w, "--- Migration SQL ---")
		for i, sql := range report.MigrationSQL {
			fmt.Fprintf(w, "%d. %s\n", i+1, sql)
		}
		fmt.Fprintln(w)
	}

	return nil
}

// FormatJSON writes the report as indented JSON to the given writer.
func FormatJSON(w io.Writer, report *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return fmt.Errorf("encoding JSON report: %w", err)
	}
	return nil
}

// FormatMarkdown writes the report as a Markdown document to the given writer.
func FormatMarkdown(w io.Writer, report *Report) error {
	fmt.Fprintln(w, "# Schema Analysis Report")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "- **Database:** %s\n", report.Summary.DatabaseDSN)
	fmt.Fprintf(w, "- **Analyzed at:** %s\n", report.Summary.AnalyzedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "- **Tables:** %d\n", report.Summary.TableCount)
	fmt.Fprintf(w, "- **Columns:** %d\n", report.Summary.ColumnCount)
	fmt.Fprintf(w, "- **Overall Score:** %.1f / 100\n", report.Summary.OverallScore)
	fmt.Fprintf(w, "- **Issues:** %d\n", report.Summary.IssueCount)
	fmt.Fprintln(w)

	// Category Scores
	fmt.Fprintln(w, "## Category Scores")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Category | Score | Issues |")
	fmt.Fprintln(w, "|----------|-------|--------|")
	for _, cs := range report.Scores {
		fmt.Fprintf(w, "| %s | %.1f | %d |\n", cs.Name, cs.Score, len(cs.Issues))
	}
	fmt.Fprintln(w)

	// Proposals
	if len(report.Proposals) > 0 {
		fmt.Fprintln(w, "## Improvement Proposals")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Priority | Table | Column | Category | Description |")
		fmt.Fprintln(w, "|----------|-------|--------|----------|-------------|")
		for _, p := range report.Proposals {
			fmt.Fprintf(w, "| %s | %s | %s | %s | %s |\n",
				p.Priority, escapeMarkdown(p.Table), escapeMarkdown(p.Column),
				p.Category, escapeMarkdown(p.Description))
		}
		fmt.Fprintln(w)
	}

	// Migration SQL
	if len(report.MigrationSQL) > 0 {
		fmt.Fprintln(w, "## Migration SQL")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "```sql")
		for _, sql := range report.MigrationSQL {
			fmt.Fprintln(w, sql)
		}
		fmt.Fprintln(w, "```")
		fmt.Fprintln(w)
	}

	return nil
}

// escapeMarkdown escapes pipe characters in a string for use inside markdown table cells.
func escapeMarkdown(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}
