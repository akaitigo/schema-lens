package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresConnector implements the Connector interface for PostgreSQL databases.
type PostgresConnector struct {
	db *sql.DB
}

func (c *PostgresConnector) Connect(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}
	c.db = db
	return nil
}

func (c *PostgresConnector) ExtractSchema(ctx context.Context) (*SchemaInfo, error) {
	tables, err := c.listTables(ctx)
	if err != nil {
		return nil, err
	}

	schema := &SchemaInfo{}
	for _, tableName := range tables {
		table, err := c.extractTable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("extracting table %s: %w", tableName, err)
		}
		schema.Tables = append(schema.Tables, *table)
	}
	return schema, nil
}

func (c *PostgresConnector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		 ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (c *PostgresConnector) extractTable(ctx context.Context, name string) (*Table, error) {
	table := &Table{Name: name}

	columns, err := c.extractColumns(ctx, name)
	if err != nil {
		return nil, err
	}
	table.Columns = columns

	indexes, err := c.extractIndexes(ctx, name)
	if err != nil {
		return nil, err
	}
	table.Indexes = indexes

	fks, err := c.extractForeignKeys(ctx, name)
	if err != nil {
		return nil, err
	}
	table.ForeignKeys = fks

	return table, nil
}

func (c *PostgresConnector) extractColumns(ctx context.Context, tableName string) ([]Column, error) {
	query := `
		SELECT c.column_name, c.data_type, c.character_maximum_length,
		       c.is_nullable, c.column_default,
		       CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk
		FROM information_schema.columns c
		LEFT JOIN (
		    SELECT ku.column_name
		    FROM information_schema.table_constraints tc
		    JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
		    WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
		) pk ON c.column_name = pk.column_name
		WHERE c.table_name = $1 AND c.table_schema = 'public'
		ORDER BY c.ordinal_position`

	rows, err := c.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.DataType, &col.MaxLength, &nullable, &col.DefaultValue, &col.IsPrimaryKey); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		col.DataType = strings.ToUpper(col.DataType)
		col.IsNullable = nullable == "YES"
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

func (c *PostgresConnector) extractIndexes(ctx context.Context, tableName string) ([]Index, error) {
	query := `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE tablename = $1 AND schemaname = 'public'`

	rows, err := c.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting indexes for %s: %w", tableName, err)
	}
	defer rows.Close()

	var indexes []Index
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return nil, fmt.Errorf("scanning index: %w", err)
		}
		idx := Index{
			Name:     name,
			IsUnique: strings.Contains(strings.ToUpper(def), "UNIQUE"),
		}
		idx.Columns = parseIndexColumns(def)
		indexes = append(indexes, idx)
	}
	return indexes, rows.Err()
}

func parseIndexColumns(indexDef string) []string {
	start := strings.LastIndex(indexDef, "(")
	end := strings.LastIndex(indexDef, ")")
	if start < 0 || end <= start {
		return nil
	}
	colStr := indexDef[start+1 : end]
	parts := strings.Split(colStr, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

func (c *PostgresConnector) extractForeignKeys(ctx context.Context, tableName string) ([]ForeignKey, error) {
	query := `
		SELECT tc.constraint_name, kcu.column_name,
		       ccu.table_name AS referenced_table,
		       ccu.column_name AS referenced_column
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
		WHERE tc.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY'
		ORDER BY tc.constraint_name, kcu.ordinal_position`

	rows, err := c.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting foreign keys for %s: %w", tableName, err)
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKey)
	for rows.Next() {
		var constraintName, colName, refTable, refCol string
		if err := rows.Scan(&constraintName, &colName, &refTable, &refCol); err != nil {
			return nil, fmt.Errorf("scanning foreign key: %w", err)
		}
		if fk, ok := fkMap[constraintName]; ok {
			fk.Columns = append(fk.Columns, colName)
			fk.ReferencedColumns = append(fk.ReferencedColumns, refCol)
		} else {
			fkMap[constraintName] = &ForeignKey{
				Name:              constraintName,
				Columns:           []string{colName},
				ReferencedTable:   refTable,
				ReferencedColumns: []string{refCol},
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var fks []ForeignKey
	for _, fk := range fkMap {
		fks = append(fks, *fk)
	}
	return fks, nil
}

func (c *PostgresConnector) SampleData(ctx context.Context, table string, limit int) ([]map[string]any, error) {
	query := `SELECT * FROM "` + table + `" ORDER BY RANDOM() LIMIT ` + fmt.Sprint(limit)
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sampling data from %s: %w", table, err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting column names: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		row := make(map[string]any, len(colNames))
		for i, col := range colNames {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func (c *PostgresConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
