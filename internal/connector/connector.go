// Package connector provides database connection and schema extraction for multiple DB engines.
package connector

import (
	"context"
	"fmt"
	"strings"
)

// SchemaInfo holds the complete schema information extracted from a database.
type SchemaInfo struct {
	Tables []Table
}

// Table represents a database table and its metadata.
type Table struct {
	Name        string
	Columns     []Column
	Indexes     []Index
	ForeignKeys []ForeignKey
	Constraints []Constraint
}

// Column represents a column in a table.
type Column struct {
	DefaultValue *string
	MaxLength    *int
	Name         string
	DataType     string
	IsNullable   bool
	IsPrimaryKey bool
}

// Index represents an index on a table.
type Index struct {
	Name     string
	Type     string
	Columns  []string
	IsUnique bool
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

// Constraint represents a table constraint.
type Constraint struct {
	Name       string
	Type       string
	Definition string
}

// Connector is the interface for database connections and schema extraction.
type Connector interface {
	Connect(ctx context.Context, dsn string) error
	ExtractSchema(ctx context.Context) (*SchemaInfo, error)
	SampleData(ctx context.Context, table string, limit int) ([]map[string]any, error)
	Close() error
}

// NewFromDSN creates a Connector based on the DSN scheme.
func NewFromDSN(dsn string) (Connector, error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://"):
		return &PostgresConnector{}, nil
	case strings.HasPrefix(dsn, "mysql://"):
		return &MySQLConnector{}, nil
	case strings.HasPrefix(dsn, "sqlite://") || strings.HasPrefix(dsn, "file:"):
		return &SQLiteConnector{}, nil
	default:
		return nil, fmt.Errorf("unsupported DSN scheme: %s", dsn)
	}
}
