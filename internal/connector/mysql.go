package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// MySQLConnector implements the Connector interface for MySQL databases.
type MySQLConnector struct {
	db     *sql.DB
	dbName string
}

func (c *MySQLConnector) Connect(ctx context.Context, dsn string) error {
	mysqlDSN := strings.TrimPrefix(dsn, "mysql://")
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return fmt.Errorf("failed to open mysql: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping mysql: %w", err)
	}
	c.db = db

	var dbName string
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		db.Close()
		return fmt.Errorf("failed to get database name: %w", err)
	}
	c.dbName = dbName

	return nil
}

func (c *MySQLConnector) ExtractSchema(ctx context.Context) (*SchemaInfo, error) {
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

func (c *MySQLConnector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = ? AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, c.dbName)
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

func (c *MySQLConnector) extractTable(ctx context.Context, name string) (*Table, error) {
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

func (c *MySQLConnector) extractColumns(ctx context.Context, tableName string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, character_maximum_length,
		       is_nullable, column_default, column_key
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = ?
		ORDER BY ordinal_position`

	rows, err := c.db.QueryContext(ctx, query, tableName, c.dbName)
	if err != nil {
		return nil, fmt.Errorf("getting columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable, columnKey string
		var maxLen *int
		if err := rows.Scan(&col.Name, &col.DataType, &maxLen, &nullable, &col.DefaultValue, &columnKey); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		col.DataType = strings.ToUpper(col.DataType)
		col.MaxLength = maxLen
		col.IsNullable = nullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

func (c *MySQLConnector) extractIndexes(ctx context.Context, tableName string) ([]Index, error) {
	query := `
		SELECT index_name, column_name, non_unique
		FROM information_schema.statistics
		WHERE table_name = ? AND table_schema = ?
		ORDER BY index_name, seq_in_index`

	rows, err := c.db.QueryContext(ctx, query, tableName, c.dbName)
	if err != nil {
		return nil, fmt.Errorf("getting indexes for %s: %w", tableName, err)
	}
	defer rows.Close()

	idxMap := make(map[string]*Index)
	var order []string
	for rows.Next() {
		var idxName, colName string
		var nonUnique int
		if err := rows.Scan(&idxName, &colName, &nonUnique); err != nil {
			return nil, fmt.Errorf("scanning index: %w", err)
		}
		if idx, ok := idxMap[idxName]; ok {
			idx.Columns = append(idx.Columns, colName)
		} else {
			idxMap[idxName] = &Index{
				Name:     idxName,
				Columns:  []string{colName},
				IsUnique: nonUnique == 0,
			}
			order = append(order, idxName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes := make([]Index, 0, len(idxMap))
	for _, name := range order {
		indexes = append(indexes, *idxMap[name])
	}
	return indexes, nil
}

func (c *MySQLConnector) extractForeignKeys(ctx context.Context, tableName string) ([]ForeignKey, error) {
	query := `
		SELECT constraint_name, column_name, referenced_table_name, referenced_column_name
		FROM information_schema.key_column_usage
		WHERE table_name = ? AND table_schema = ? AND referenced_table_name IS NOT NULL
		ORDER BY constraint_name, ordinal_position`

	rows, err := c.db.QueryContext(ctx, query, tableName, c.dbName)
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

	fks := make([]ForeignKey, 0, len(fkMap))
	for _, fk := range fkMap {
		fks = append(fks, *fk)
	}
	return fks, nil
}

func (c *MySQLConnector) SampleData(ctx context.Context, table string, limit int) ([]map[string]any, error) {
	query := "SELECT * FROM `" + table + "` ORDER BY RAND() LIMIT " + fmt.Sprint(limit) //nolint:gosec // table name is from schema metadata, not user input
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

func (c *MySQLConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
