package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/akaitigo/schema-lens/internal/analyzer"
	"github.com/akaitigo/schema-lens/internal/connector"
	"github.com/akaitigo/schema-lens/internal/profiler"
	"github.com/akaitigo/schema-lens/internal/reporter"
	"github.com/spf13/cobra"
)

var version = "dev"

const formatMarkdown = "markdown"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:     "schema-lens",
		Short:   "Database schema quality analysis tool",
		Version: version,
	}
	root.AddCommand(newAnalyzeCmd())
	root.AddCommand(newMigrateCmd())
	return root
}

type analyzeOpts struct {
	dsn        string
	format     string
	profile    bool
	suggest    bool
	sampleSize int
}

func newAnalyzeCmd() *cobra.Command {
	opts := &analyzeOpts{}

	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze database schema quality",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.dsn == "" {
				return fmt.Errorf("--dsn is required")
			}
			return runAnalyze(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.dsn, "dsn", "", "database connection string (e.g., postgres://..., mysql://..., sqlite://..., file:...)")
	cmd.Flags().StringVar(&opts.format, "format", "table", "output format: table, json, markdown")
	cmd.Flags().BoolVar(&opts.profile, "profile", false, "enable data profiling")
	cmd.Flags().BoolVar(&opts.suggest, "suggest", false, "show improvement suggestions")
	cmd.Flags().IntVar(&opts.sampleSize, "sample-size", 1000, "sample size for data profiling")

	return cmd
}

func runAnalyze(ctx context.Context, opts *analyzeOpts) error {
	conn, err := connector.NewFromDSN(opts.dsn)
	if err != nil {
		return fmt.Errorf("unsupported database: %w", err)
	}

	connErr := conn.Connect(ctx, opts.dsn)
	if connErr != nil {
		return fmt.Errorf("connection failed: %w", connErr)
	}
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		return fmt.Errorf("schema extraction failed: %w", err)
	}

	// When --suggest is not set and no profiling, use simple schema output
	if !opts.suggest && !opts.profile {
		return renderSchemaOnly(schema, opts.format)
	}

	// Run analysis
	analysis := analyzer.Analyze(schema)

	// Run profiling if requested
	var profileResult *profiler.ProfileResult
	if opts.profile {
		profileResult, err = profiler.Profile(ctx, conn, schema, opts.sampleSize)
		if err != nil {
			return fmt.Errorf("profiling failed: %w", err)
		}
	}

	// Generate report
	report := reporter.GenerateReport(opts.dsn, analysis, profileResult)

	// Format and output
	return formatReport(report, opts.format)
}

// renderSchemaOnly outputs the raw schema information without analysis.
func renderSchemaOnly(schema *connector.SchemaInfo, format string) error {
	switch format {
	case "json":
		return reporter.FormatJSON(os.Stdout, schemaToReport(schema))
	case "table", formatMarkdown:
		printSchemaTable(schema, format)
		return nil
	default:
		return fmt.Errorf("unsupported format: %s (use table, json, or markdown)", format)
	}
}

// schemaToReport creates a minimal Report from a SchemaInfo for JSON output.
func schemaToReport(schema *connector.SchemaInfo) *reporter.Report {
	columnCount := 0
	for _, t := range schema.Tables {
		columnCount += len(t.Columns)
	}
	return &reporter.Report{
		Summary: reporter.Summary{
			TableCount:   len(schema.Tables),
			ColumnCount:  columnCount,
			OverallScore: 100.0,
		},
	}
}

// formatReport writes the report in the specified format to stdout.
func formatReport(report *reporter.Report, format string) error {
	switch format {
	case "table":
		return reporter.FormatTable(os.Stdout, report)
	case "json":
		return reporter.FormatJSON(os.Stdout, report)
	case formatMarkdown:
		return reporter.FormatMarkdown(os.Stdout, report)
	default:
		return fmt.Errorf("unsupported format: %s (use table, json, or markdown)", format)
	}
}

type migrateOpts struct {
	dsn        string
	dryRun     bool
	sampleSize int
}

func newMigrateCmd() *cobra.Command {
	opts := &migrateOpts{}

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Generate migration SQL from schema analysis",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.dsn == "" {
				return fmt.Errorf("--dsn is required")
			}
			return runMigrate(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.dsn, "dsn", "", "database connection string")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", true, "only output SQL, do not execute (default: true)")
	cmd.Flags().IntVar(&opts.sampleSize, "sample-size", 1000, "sample size for data profiling")

	return cmd
}

func runMigrate(ctx context.Context, opts *migrateOpts) error {
	conn, err := connector.NewFromDSN(opts.dsn)
	if err != nil {
		return fmt.Errorf("unsupported database: %w", err)
	}

	connErr := conn.Connect(ctx, opts.dsn)
	if connErr != nil {
		return fmt.Errorf("connection failed: %w", connErr)
	}
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		return fmt.Errorf("schema extraction failed: %w", err)
	}

	analysis := analyzer.Analyze(schema)

	profileResult, err := profiler.Profile(ctx, conn, schema, opts.sampleSize)
	if err != nil {
		return fmt.Errorf("profiling failed: %w", err)
	}

	report := reporter.GenerateReport(opts.dsn, analysis, profileResult)

	if len(report.MigrationSQL) == 0 {
		fmt.Println("-- No migration SQL generated. Schema looks good!")
		return nil
	}

	fmt.Println("-- Schema-Lens Migration SQL")
	fmt.Println("-- Generated from schema analysis and data profiling")
	fmt.Printf("-- Database: %s\n", opts.dsn)
	fmt.Printf("-- Mode: %s\n", migrationMode(opts.dryRun))
	fmt.Println()

	for _, sql := range report.MigrationSQL {
		fmt.Println(sql)
	}

	return nil
}

func migrationMode(dryRun bool) string {
	if dryRun {
		return "dry-run (no changes applied)"
	}
	return "execute"
}

func printSchemaTable(schema *connector.SchemaInfo, format string) {
	if len(schema.Tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	for _, table := range schema.Tables {
		printTableHeader(table.Name, format)
		printColumns(table.Columns, format)
		printIndexes(table.Indexes, format)
		printForeignKeys(table.ForeignKeys, format)
		fmt.Println()
	}
}

func printTableHeader(name, format string) {
	if format == formatMarkdown {
		fmt.Printf("## %s\n\n", name)
		fmt.Println("| Column | Type | Nullable | PK |")
		fmt.Println("|--------|------|----------|----|")
	} else {
		fmt.Printf("Table: %s\n", name)
		fmt.Printf("  %-30s %-20s %-10s %-5s\n", "Column", "Type", "Nullable", "PK")
		fmt.Printf("  %-30s %-20s %-10s %-5s\n", "------", "----", "--------", "--")
	}
}

func printColumns(columns []connector.Column, format string) {
	for _, col := range columns {
		nullable := "NO"
		if col.IsNullable {
			nullable = "YES"
		}
		pk := ""
		if col.IsPrimaryKey {
			pk = "PK"
		}
		typeStr := col.DataType
		if col.MaxLength != nil {
			typeStr = fmt.Sprintf("%s(%d)", col.DataType, *col.MaxLength)
		}

		if format == formatMarkdown {
			fmt.Printf("| %s | %s | %s | %s |\n", col.Name, typeStr, nullable, pk)
		} else {
			fmt.Printf("  %-30s %-20s %-10s %-5s\n", col.Name, typeStr, nullable, pk)
		}
	}
}

func printIndexes(indexes []connector.Index, format string) {
	if len(indexes) == 0 {
		return
	}
	fmt.Println()
	if format == formatMarkdown {
		fmt.Println("### Indexes")
	} else {
		fmt.Println("  Indexes:")
	}
	for _, idx := range indexes {
		unique := ""
		if idx.IsUnique {
			if format == formatMarkdown {
				unique = " (UNIQUE)"
			} else {
				unique = " UNIQUE"
			}
		}
		cols := strings.Join(idx.Columns, ", ")
		if format == formatMarkdown {
			fmt.Printf("- `%s` on (%s)%s\n", idx.Name, cols, unique)
		} else {
			fmt.Printf("    %s (%s)%s\n", idx.Name, cols, unique)
		}
	}
}

func printForeignKeys(fks []connector.ForeignKey, format string) {
	if len(fks) == 0 {
		return
	}
	fmt.Println()
	if format == formatMarkdown {
		fmt.Println("### Foreign Keys")
	} else {
		fmt.Println("  Foreign Keys:")
	}
	for _, fk := range fks {
		cols := strings.Join(fk.Columns, ", ")
		refCols := strings.Join(fk.ReferencedColumns, ", ")
		if format == formatMarkdown {
			fmt.Printf("- `%s`: (%s) -> %s(%s)\n", fk.Name, cols, fk.ReferencedTable, refCols)
		} else {
			fmt.Printf("    %s: (%s) -> %s(%s)\n", fk.Name, cols, fk.ReferencedTable, refCols)
		}
	}
}
