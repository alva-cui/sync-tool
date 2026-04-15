package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/alva-cui/sync-tool/internal/cdc"
	"github.com/alva-cui/sync-tool/internal/metrics"
	"github.com/go-sql-driver/mysql"
)

type MySQLTarget struct {
	cfg     WriteConfig
	db      *sql.DB
	stmtUp  *sql.Stmt // 预编译 UPSERT 语句
	stmtDel *sql.Stmt // 预编译 DELETE 语句
}

func NewMySQLTarget(cfg WriteConfig) (*MySQLTarget, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		metrics.ObserveTargetWrite("mysql-task", "mysql", duration) // 实际 taskID 需透传
	}()

	if len(cfg.PrimaryKey) == 0 {
		return nil, fmt.Errorf("MySQL target requires primary_key for UPSERT")
	}

	cfg.Driver = "mysql"
	t := &MySQLTarget{cfg: cfg}

	if err := t.connect(); err != nil {
		return nil, err
	}
	if err := t.prepareStatements(); err != nil {
		t.Close()
		return nil, err
	}
	return t, nil
}

func (t *MySQLTarget) connect() error {
	cfg := mysql.NewConfig()
	cfg.User = t.cfg.User
	cfg.Passwd = t.cfg.Pass
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", t.cfg.Host, t.cfg.Port)
	cfg.DBName = t.cfg.DBName
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.MultiStatements = true // 允许批量语句

	conn, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(30 * time.Minute)

	if err := conn.Ping(); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}
	t.db = conn
	return nil
}

func (t *MySQLTarget) prepareStatements() error {
	// ✅ UPSERT: INSERT ... ON DUPLICATE KEY UPDATE
	upsertSQL := t.buildMySQLUpsertSQL()
	stmtUp, err := t.db.Prepare(upsertSQL)
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	t.stmtUp = stmtUp

	// ✅ DELETE: DELETE FROM table WHERE pk1=? AND pk2=?
	delSQL := t.buildMySQLDeleteSQL()
	stmtDel, err := t.db.Prepare(delSQL)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	t.stmtDel = stmtDel

	return nil
}

func (t *MySQLTarget) buildMySQLUpsertSQL() string {
	// 动态构建: INSERT INTO tbl (c1,c2,c3) VALUES (?,?,?) ON DUPLICATE KEY UPDATE c2=VALUES(c2), c3=VALUES(c3)
	cols := t.getColumns() // 从首次写入推断，或配置指定
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	setParts := make([]string, 0, len(cols)-len(t.cfg.PrimaryKey))
	for _, col := range cols {
		if t.isPrimaryKey(col) {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		t.cfg.Table,
		strings.Join(cols, ","),
		strings.Join(placeholders, ","),
		strings.Join(setParts, ","),
	)
}

func (t *MySQLTarget) buildMySQLDeleteSQL() string {
	where := make([]string, len(t.cfg.PrimaryKey))
	for i, pk := range t.cfg.PrimaryKey {
		where[i] = fmt.Sprintf("%s=?", pk)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", t.cfg.Table, strings.Join(where, " AND "))
}

func (t *MySQLTarget) getColumns() []string {
	// MVP: 从首次写入的 row 推断列名
	// 生产环境应查询 information_schema 或配置指定
	// 此处返回空，由 Write 动态构建（简化实现）
	return []string{}
}

func (t *MySQLTarget) isPrimaryKey(col string) bool {
	for _, pk := range t.cfg.PrimaryKey {
		if pk == col {
			return true
		}
	}
	return false
}

func (t *MySQLTarget) Write(ctx context.Context, eventType cdc.EventType, rows []map[string]any) error {
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

func (t *MySQLTarget) batchUpsert(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	// ✅ 动态构建 SQL（首次调用缓存）
	// 生产环境应预解析列顺序，此处为简化实现
	cols := t.inferColumns(rows[0])
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	setParts := make([]string, 0, len(cols))
	for _, col := range cols {
		if t.isPrimaryKey(col) {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		t.cfg.Table,
		strings.Join(cols, ","),
		strings.Join(placeholders, ","),
		strings.Join(setParts, ","),
	)

	// ✅ 批量执行（每行一个语句，避免参数爆炸）
	// 生产环境应使用事务 + 批量参数绑定
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare batch upsert: %w", err)
	}

	for _, row := range rows {
		values := t.buildValues(cols, row)
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("exec upsert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (t *MySQLTarget) batchDelete(ctx context.Context, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, t.buildMySQLDeleteSQL())
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}

	for _, row := range rows {
		values := t.buildPKValues(row)
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("exec delete row: %w", err)
		}
	}

	return tx.Commit()
}

func (t *MySQLTarget) inferColumns(row map[string]any) []string {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	return cols
}

func (t *MySQLTarget) buildValues(cols []string, row map[string]any) []any {
	values := make([]any, 0, len(cols))
	for _, col := range cols {
		values = append(values, row[col])
	}
	return values
}

func (t *MySQLTarget) buildPKValues(row map[string]any) []any {
	values := make([]any, 0, len(t.cfg.PrimaryKey))
	for _, pk := range t.cfg.PrimaryKey {
		values = append(values, row[pk])
	}
	return values
}

func (t *MySQLTarget) Close() error {
	if t.stmtUp != nil {
		_ = t.stmtUp.Close()
	}
	if t.stmtDel != nil {
		_ = t.stmtDel.Close()
	}
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

func (t *MySQLTarget) String() string { return "mysql-target" }
