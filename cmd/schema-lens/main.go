package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/akaitigo/schema-lens/internal/connector"
	"github.com/spf13/cobra"
)

var version = "dev"

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

func newAnalyzeCmd() *cobra.Command {
	var (
		dsn        string
		format     string
		profile    bool
		suggest    bool
		sampleSize int
	)

	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze database schema quality",
		RunE: func(cmd *cobra.Command, args []string) error {
			if dsn == "" {
				return fmt.Errorf("--dsn is required")
			}
			return runAnalyze(cmd.Context(), dsn, format, profile, suggest, sampleSize)
		},
	}

	cmd.Flags().StringVar(&dsn, "dsn", "", "database connection string (e.g., postgres://..., mysql://..., sqlite://..., file:...)")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json, markdown")
	cmd.Flags().BoolVar(&profile, "profile", false, "enable data profiling")
	cmd.Flags().BoolVar(&suggest, "suggest", false, "show improvement suggestions")
	cmd.Flags().IntVar(&sampleSize, "sample-size", 1000, "sample size for data profiling")

	return cmd
}

func runAnalyze(ctx context.Context, dsn, format string, profile, suggest bool, sampleSize int) error {
	conn, err := connector.NewFromDSN(dsn)
	if err != nil {
		return fmt.Errorf("unsupported database: %w", err)
	}

	if err := conn.Connect(ctx, dsn); err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		return fmt.Errorf("schema extraction failed: %w", err)
	}

	switch format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(schema)
	case "table", "markdown":
		printSchemaTable(schema, format)
		return nil
	default:
		return fmt.Errorf("unsupported format: %s (use table, json, or markdown)", format)
	}
}

func printSchemaTable(schema *connector.SchemaInfo, format string) {
	if len(schema.Tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	for _, table := range schema.Tables {
		if format == "markdown" {
			fmt.Printf("## %s\n\n", table.Name)
			fmt.Println("| Column | Type | Nullable | PK |")
			fmt.Println("|--------|------|----------|----|")
		} else {
			fmt.Printf("Table: %s\n", table.Name)
			fmt.Printf("  %-30s %-20s %-10s %-5s\n", "Column", "Type", "Nullable", "PK")
			fmt.Printf("  %-30s %-20s %-10s %-5s\n", "------", "----", "--------", "--")
		}

		for _, col := range table.Columns {
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

			if format == "markdown" {
				fmt.Printf("| %s | %s | %s | %s |\n", col.Name, typeStr, nullable, pk)
			} else {
				fmt.Printf("  %-30s %-20s %-10s %-5s\n", col.Name, typeStr, nullable, pk)
			}
		}

		if len(table.Indexes) > 0 {
			fmt.Println()
			if format == "markdown" {
				fmt.Println("### Indexes")
				for _, idx := range table.Indexes {
					unique := ""
					if idx.IsUnique {
						unique = " (UNIQUE)"
					}
					fmt.Printf("- `%s` on (%s)%s\n", idx.Name, joinCols(idx.Columns), unique)
				}
			} else {
				fmt.Println("  Indexes:")
				for _, idx := range table.Indexes {
					unique := ""
					if idx.IsUnique {
						unique = " UNIQUE"
					}
					fmt.Printf("    %s (%s)%s\n", idx.Name, joinCols(idx.Columns), unique)
				}
			}
		}

		if len(table.ForeignKeys) > 0 {
			fmt.Println()
			if format == "markdown" {
				fmt.Println("### Foreign Keys")
				for _, fk := range table.ForeignKeys {
					fmt.Printf("- `%s`: (%s) -> %s(%s)\n", fk.Name, joinCols(fk.Columns), fk.ReferencedTable, joinCols(fk.ReferencedColumns))
				}
			} else {
				fmt.Println("  Foreign Keys:")
				for _, fk := range table.ForeignKeys {
					fmt.Printf("    %s: (%s) -> %s(%s)\n", fk.Name, joinCols(fk.Columns), fk.ReferencedTable, joinCols(fk.ReferencedColumns))
				}
			}
		}
		fmt.Println()
	}
}

func joinCols(cols []string) string {
	result := ""
	for i, c := range cols {
		if i > 0 {
			result += ", "
		}
		result += c
	}
	return result
}
