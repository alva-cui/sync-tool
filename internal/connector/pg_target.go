package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/alva-cui/sync-tool/internal/cdc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PGTarget struct {
	cfg     WriteConfig
	conn    *pgx.Conn
	typeMap *pgconn.StatementDescription
}

func NewPGTarget(cfg WriteConfig) (*PGTarget, error) {
	if len(cfg.PrimaryKey) == 0 {
		return nil, fmt.Errorf("PostgreSQL target requires primary_key for UPSERT")
	}

	cfg.Driver = "postgres"
	t := &PGTarget{cfg: cfg}

	if err := t.connect(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *PGTarget) connect() error {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		t.cfg.Host, t.cfg.Port, t.cfg.User, t.cfg.Pass, t.cfg.DBName,
	)
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	t.conn = conn
	return nil
}

func (t *PGTarget) Write(ctx context.Context, eventType cdc.EventType, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	switch eventType {
	case cdc.EventInsert, cdc.EventUpdate:
		return t.batchUpsert(ctx, rows)
	case cdc.EventDelete:
		return t.batchDelete(ctx, rows)
	default:
		return fmt.Errorf("unknown event type: %s", eventType)
	}
}

func (t *PGTarget) batchUpsert(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	// ✅ 使用 pgx.CopyFrom 实现高性能批量 UPSERT
	// 步骤: 1. 创建临时表 2. COPY INTO 临时表 3. INSERT ... ON CONFLICT 合并

	// 简化实现：逐行执行 ON CONFLICT DO UPDATE（生产环境应优化为批量）
	// 动态构建列与参数
	cols := t.inferColumns(rows[0])
	pkSet := make(map[string]bool)
	for _, pk := range t.cfg.PrimaryKey {
		pkSet[pk] = true
	}

	// 构建 ON CONFLICT 子句
	conflictCols := strings.Join(t.cfg.PrimaryKey, ", ")
	setParts := make([]string, 0, len(cols))
	for _, col := range cols {
		if pkSet[col] {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s=EXCLUDED.%s", col, col))
	}
	onConflict := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", conflictCols, strings.Join(setParts, ", "))

	// 预编译语句
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) %s",
		t.cfg.Table,
		strings.Join(cols, ","),
		t.buildPlaceholders(len(cols), 1),
		onConflict,
	)

	batch := &pgx.Batch{}
	for _, row := range rows {
		values := t.buildValues(cols, row)
		batch.Queue(sql, values...)
	}

	br := t.conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(rows); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("exec upsert row %d: %w", i, err)
		}
	}
	return nil
}

func (t *PGTarget) batchDelete(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	where := make([]string, len(t.cfg.PrimaryKey))
	for i, pk := range t.cfg.PrimaryKey {
		where[i] = fmt.Sprintf("%s=$%d", pk, i+1)
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", t.cfg.Table, strings.Join(where, " AND "))

	batch := &pgx.Batch{}
	for _, row := range rows {
		values := t.buildPKValues(row)
		batch.Queue(sql, values...)
	}

	br := t.conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(rows); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("exec delete row %d: %w", i, err)
		}
	}
	return nil
}

func (t *PGTarget) inferColumns(row map[string]any) []string {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	return cols
}

func (t *PGTarget) buildPlaceholders(n, start int) string {
	placeholders := make([]string, n)
	for i := 0; i < n; i++ {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(placeholders, ", ")
}

func (t *PGTarget) buildValues(cols []string, row map[string]any) []any {
	values := make([]any, 0, len(cols))
	for _, col := range cols {
		values = append(values, row[col])
	}
	return values
}

func (t *PGTarget) buildPKValues(row map[string]any) []any {
	values := make([]any, 0, len(t.cfg.PrimaryKey))
	for _, pk := range t.cfg.PrimaryKey {
		values = append(values, row[pk])
	}
	return values
}

func (t *PGTarget) Close() error {
	if t.conn != nil {
		return t.conn.Close(context.Background())
	}
	return nil
}

func (t *PGTarget) String() string { return "postgres-target" }
