package connector

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresConn_Integration 需本地运行: docker compose -f scripts/test-pg.yml up -d
func TestPostgresConn_Integration(t *testing.T) {
	if os.Getenv("RUN_PG_INTEGRATION") == "" {
		t.Skip("skip PG integration test; set RUN_PG_INTEGRATION=1 to run")
	}

	ctx := context.Background()
	dsn := DSN{
		Driver: "postgres",
		Host:   "localhost",
		Port:   5432,
		User:   "sync_user",
		Pass:   "sync_pass",
		DBName: "sync_test",
	}

	conn := &PostgresConn{}
	require.NoError(t, conn.Connect(ctx, dsn))
	defer conn.Close()

	// 准备测试表
	_, err := conn.conn.Exec(ctx, `
		CREATE TEMP TABLE test_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		);
		INSERT INTO test_users (name, updated_at) VALUES
			('Alice', NOW() - INTERVAL '2 hours'),
			('Bob', NOW() - INTERVAL '1 hour'),
			('Charlie', NOW());
	`)
	require.NoError(t, err)

	tests := []struct {
		name     string
		cursor   Cursor
		wantRows int
		wantNext any
	}{
		{
			name:     "initial_fetch",
			cursor:   Cursor{Field: "updated_at", Value: time.Time{}},
			wantRows: 3,
			wantNext: "Charlie", // 最后一条 name
		},
		{
			name:     "incremental_fetch",
			cursor:   Cursor{Field: "updated_at", Value: time.Now().Add(-90 * time.Minute)},
			wantRows: 2,
			wantNext: "Bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, next, err := conn.IncrementalRead(ctx, "test_users", tt.cursor, 10)
			require.NoError(t, err)
			assert.Len(t, rows, tt.wantRows)

			if tt.wantRows > 0 {
				lastRow := rows[len(rows)-1]
				assert.Equal(t, tt.wantNext, lastRow["name"])
				assert.NotNil(t, next.Value)
			}
		})
	}
}

func TestPostgresConn_normalizePGValue(t *testing.T) {
	p := &PostgresConn{}
	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"nil", nil, nil},
		{"string", "hello", "hello"},
		{"[]byte", []byte("world"), "world"},
		{"pgtype.Timestamp valid", pgtype.Timestamp{Time: time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC), Valid: true}, time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC)},
		{"pgtype.Timestamp invalid", pgtype.Timestamp{Valid: false}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.normalizePGValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
