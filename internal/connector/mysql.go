package connector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

type MySQLConn struct {
	db *sql.DB
}

func (m *MySQLConn) Connect(_ context.Context, dsn DSN) error {
	cfg := mysql.NewConfig()
	cfg.User = dsn.User
	cfg.Passwd = dsn.Pass
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", dsn.Host, dsn.Port)
	cfg.DBName = dsn.DBName
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	conn, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	if err := conn.Ping(); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}
	m.db = conn
	return nil
}

func (m *MySQLConn) IncrementalRead(ctx context.Context, table string, cursor Cursor, limit int) ([]map[string]any, Cursor, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?", table, cursor.Field, cursor.Field)
	rows, err := m.db.QueryContext(ctx, query, cursor.Value, limit)
	if err != nil {
		return nil, cursor, fmt.Errorf("query mysql: %w", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	results := make([]map[string]any, 0, limit)
	var newCursor Cursor

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, cursor, fmt.Errorf("scan row: %w", err)
		}

		row := make(map[string]any)
		for i, col := range cols {
			row[col] = vals[i]
			if col == cursor.Field {
				newCursor = Cursor{Field: cursor.Field, Value: vals[i]}
			}
		}
		results = append(results, row)
	}

	if len(results) == 0 {
		return nil, cursor, nil
	}
	return results, newCursor, nil
}

func (m *MySQLConn) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}
