package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type PostgresConn struct {
	conn *pgx.Conn
	cfg  pgx.ConnConfig
}

func (p *PostgresConn) Connect(ctx context.Context, dsn DSN) error {
	connCfg, err := pgx.ParseConfig(fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dsn.Host, dsn.Port, dsn.User, dsn.Pass, dsn.DBName,
	))
	if err != nil {
		return fmt.Errorf("parse pg config: %w", err)
	}
	connCfg.ConnectTimeout = 10 * time.Second
	connCfg.RuntimeParams = map[string]string{
		"application_name": "sync-tool",
		"timezone":         "UTC",
	}

	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	// 验证连接 & 设置会话参数
	if _, err := conn.Exec(ctx, "SET idle_in_transaction_session_timeout = 30000"); err != nil {
		conn.Close(ctx)
		return fmt.Errorf("set pg session: %w", err)
	}

	p.conn = conn
	p.cfg = *connCfg
	return nil
}

func (p *PostgresConn) IncrementalRead(ctx context.Context, table string, cursor Cursor, limit int) ([]map[string]any, Cursor, error) {
	// 构建安全查询（表名/字段名需白名单校验，此处假设已校验）
	// 支持三种游标策略：
	// 1. 时间戳: updated_at > $1 ORDER BY updated_at, id LIMIT $2
	// 2. 自增ID: id > $1 ORDER BY id LIMIT $2
	// 3. ctid: ctid > $1 ORDER BY ctid LIMIT $2 (物理行定位，需配合 VACUUM)
	var query string
	var args []any

	switch cursor.Field {
	case "updated_at", "created_at":
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC, ctid ASC LIMIT $2`,
			table, cursor.Field, cursor.Field,
		)
		args = []any{cursor.Value, limit}
	case "id", "seq":
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC LIMIT $2`,
			table, cursor.Field, cursor.Field,
		)
		args = []any{cursor.Value, limit}
	default:
		// 回退到 ctid 物理游标（需确保表无频繁 UPDATE/DELETE）
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE ctid > $1 ORDER BY ctid ASC LIMIT $2`,
			table,
		)
		args = []any{cursor.Value, limit}
	}

	rows, err := p.conn.Query(ctx, query, args...)
	if err != nil {
		var pgErr *pgconn.PgError
		if pgErr != nil {
			return nil, cursor, fmt.Errorf("pg query [%s]: code=%s msg=%s", query, pgErr.Code, pgErr.Message)
		}
		return nil, cursor, fmt.Errorf("query postgres: %w", err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	results := make([]map[string]any, 0, limit)
	var newCursor Cursor

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, cursor, fmt.Errorf("scan values: %w", err)
		}

		row := make(map[string]any, len(fields))
		for i, field := range fields {
			val := p.normalizePGValue(values[i])
			row[field.Name] = val

			// 更新游标值
			if field.Name == cursor.Field || (cursor.Field == "" && i == 0) {
				newCursor = Cursor{Field: field.Name, Value: val}
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, cursor, fmt.Errorf("rows iteration: %w", err)
	}

	if len(results) == 0 {
		return nil, cursor, nil
	}
	return results, newCursor, nil
}

// normalizePGValue 将 pgtype 特有类型转换为 Go 标准类型，确保 transform 引擎兼容
func (p *PostgresConn) normalizePGValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case pgtype.Timestamp:
		if val.Valid {
			return val.Time.UTC()
		}
		return nil
	case pgtype.Timestamptz:
		if val.Valid {
			return val.Time.UTC()
		}
		return nil
	case pgtype.Date:
		if val.Valid {
			return val.Time.UTC()
		}
		return nil
	case pgtype.Numeric:
		if val.Valid {
			f, _ := val.Float64Value()
			return f
		}
		return nil
	case []byte:
		// 尝试解码为 string（避免 JSON 字段被当作 []byte）
		return string(val)
	default:
		return val
	}
}

func (p *PostgresConn) Close() error {
	if p.conn != nil {
		return p.conn.Close(context.Background())
	}
	return nil
}
