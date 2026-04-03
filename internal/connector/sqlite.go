package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite" // SQLite driver
)

// SQLiteConnector implements the Connector interface for SQLite databases.
type SQLiteConnector struct {
	db *sql.DB
}

func (c *SQLiteConnector) Connect(ctx context.Context, dsn string) error {
	dsn = strings.TrimPrefix(dsn, "sqlite://")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping sqlite: %w", err)
	}
	c.db = db
	return nil
}

func (c *SQLiteConnector) ExtractSchema(ctx context.Context) (*SchemaInfo, error) {
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

func (c *SQLiteConnector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
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

func (c *SQLiteConnector) extractTable(ctx context.Context, name string) (*Table, error) {
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

func (c *SQLiteConnector) extractColumns(ctx context.Context, tableName string) ([]Column, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info('%s')", tableName))
	if err != nil {
		return nil, fmt.Errorf("getting columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dfltValue *string
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		col := Column{
			Name:         name,
			DataType:     strings.ToUpper(dataType),
			IsNullable:   notNull == 0,
			DefaultValue: dfltValue,
			IsPrimaryKey: pk > 0,
		}
		col.MaxLength = parseMaxLength(dataType)
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

func parseMaxLength(dataType string) *int {
	dt := strings.ToUpper(dataType)
	if idx := strings.Index(dt, "("); idx > 0 {
		end := strings.Index(dt, ")")
		if end > idx {
			var length int
			if _, err := fmt.Sscanf(dt[idx+1:end], "%d", &length); err == nil {
				return &length
			}
		}
	}
	return nil
}

func (c *SQLiteConnector) extractIndexes(ctx context.Context, tableName string) ([]Index, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list('%s')", tableName))
	if err != nil {
		return nil, fmt.Errorf("getting indexes for %s: %w", tableName, err)
	}
	defer rows.Close()

	var indexes []Index
	for rows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, fmt.Errorf("scanning index: %w", err)
		}

		cols, err := c.extractIndexColumns(ctx, name)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, Index{
			Name:     name,
			Columns:  cols,
			IsUnique: unique == 1,
			Type:     origin,
		})
	}
	return indexes, rows.Err()
}

func (c *SQLiteConnector) extractIndexColumns(ctx context.Context, indexName string) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info('%s')", indexName))
	if err != nil {
		return nil, fmt.Errorf("getting index info for %s: %w", indexName, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var seqNo, cid int
		var name *string
		if err := rows.Scan(&seqNo, &cid, &name); err != nil {
			return nil, fmt.Errorf("scanning index column: %w", err)
		}
		if name != nil {
			cols = append(cols, *name)
		}
	}
	return cols, rows.Err()
}

func (c *SQLiteConnector) extractForeignKeys(ctx context.Context, tableName string) ([]ForeignKey, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list('%s')", tableName))
	if err != nil {
		return nil, fmt.Errorf("getting foreign keys for %s: %w", tableName, err)
	}
	defer rows.Close()

	fkMap := make(map[int]*ForeignKey)
	for rows.Next() {
		var id, seq int
		var refTable, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("scanning foreign key: %w", err)
		}
		if fk, ok := fkMap[id]; ok {
			fk.Columns = append(fk.Columns, from)
			fk.ReferencedColumns = append(fk.ReferencedColumns, to)
		} else {
			fkMap[id] = &ForeignKey{
				Name:              fmt.Sprintf("fk_%s_%d", tableName, id),
				Columns:           []string{from},
				ReferencedTable:   refTable,
				ReferencedColumns: []string{to},
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

func (c *SQLiteConnector) SampleData(ctx context.Context, table string, limit int) ([]map[string]any, error) {
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

func (c *SQLiteConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
