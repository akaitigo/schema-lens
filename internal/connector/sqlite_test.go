package connector_test

import (
	"context"
	"testing"

	"github.com/akaitigo/schema-lens/internal/connector"
)

func TestSQLiteConnector_ExtractSchema_Empty(t *testing.T) {
	ctx := context.Background()
	conn := &connector.SQLiteConnector{}
	if err := conn.Connect(ctx, "file::memory:"); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}
	if len(schema.Tables) != 0 {
		t.Errorf("expected 0 tables in empty db, got %d", len(schema.Tables))
	}
}

func TestSQLiteConnector_ExtractSchema_WithTables(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255),
			status INTEGER DEFAULT 0
		);
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			total DECIMAL(10,2),
			FOREIGN KEY (user_id) REFERENCES users(id)
		);
		CREATE INDEX idx_orders_user_id ON orders(user_id);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(schema.Tables))
	}

	var usersTable, ordersTable *connector.Table
	for i := range schema.Tables {
		switch schema.Tables[i].Name {
		case "users":
			usersTable = &schema.Tables[i]
		case "orders":
			ordersTable = &schema.Tables[i]
		}
	}

	if usersTable == nil {
		t.Fatal("users table not found")
	}
	if ordersTable == nil {
		t.Fatal("orders table not found")
	}

	if len(usersTable.Columns) != 4 {
		t.Errorf("expected 4 columns in users, got %d", len(usersTable.Columns))
	}

	if len(ordersTable.ForeignKeys) != 1 {
		t.Errorf("expected 1 foreign key in orders, got %d", len(ordersTable.ForeignKeys))
	}
	if len(ordersTable.ForeignKeys) > 0 {
		fk := ordersTable.ForeignKeys[0]
		if fk.ReferencedTable != "users" {
			t.Errorf("expected FK to reference users, got %s", fk.ReferencedTable)
		}
	}

	if len(ordersTable.Indexes) != 1 {
		t.Errorf("expected 1 index in orders, got %d", len(ordersTable.Indexes))
	}
}

func TestSQLiteConnector_SampleData(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);
		INSERT INTO items VALUES (1, 'apple', 1.50);
		INSERT INTO items VALUES (2, 'banana', 0.75);
		INSERT INTO items VALUES (3, 'cherry', 2.00);
	`)
	defer conn.Close()

	data, err := conn.SampleData(ctx, "items", 2)
	if err != nil {
		t.Fatalf("failed to sample data: %v", err)
	}
	if len(data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(data))
	}
}

func TestSQLiteConnector_ColumnTypes(t *testing.T) {
	ctx := context.Background()
	conn := connector.NewSQLiteForTest(t, `
		CREATE TABLE typed (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			active BOOLEAN DEFAULT 1,
			score DECIMAL(5,2)
		);
	`)
	defer conn.Close()

	schema, err := conn.ExtractSchema(ctx)
	if err != nil {
		t.Fatalf("failed to extract schema: %v", err)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	for _, col := range table.Columns {
		switch col.Name {
		case "name":
			if col.MaxLength == nil || *col.MaxLength != 100 {
				t.Errorf("expected name MaxLength=100, got %v", col.MaxLength)
			}
			if col.IsNullable {
				t.Error("expected name to be NOT NULL")
			}
		case "id":
			if !col.IsPrimaryKey {
				t.Error("expected id to be primary key")
			}
		case "description":
			if !col.IsNullable {
				t.Error("expected description to be nullable")
			}
		}
	}
}

func TestNewFromDSN(t *testing.T) {
	tests := []struct {
		dsn     string
		wantErr bool
	}{
		{"sqlite://test.db", false},
		{"file::memory:", false},
		{"postgres://localhost/db", false},
		{"mysql://user:pass@tcp(localhost)/db", false},
		{"unknown://foo", true},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			_, err := connector.NewFromDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFromDSN(%q) error = %v, wantErr %v", tt.dsn, err, tt.wantErr)
			}
		})
	}
}
