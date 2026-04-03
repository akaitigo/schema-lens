package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/akaitigo/schema-lens/internal/connector"
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

	switch opts.format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(schema)
	case "table", formatMarkdown:
		printSchemaTable(schema, opts.format)
		return nil
	default:
		return fmt.Errorf("unsupported format: %s (use table, json, or markdown)", opts.format)
	}
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
