package connector

import "context"

type DSN struct {
	Driver string `json:"driver"` // mysql, postgres
	Host   string `json:"host"`
	Port   int    `json:"port"`
	User   string `json:"user"`
	Pass   string `json:"pass"` // 解密后传入
	DBName string `json:"db_name"`
}

type Cursor struct {
	Field string
	Value any // int64, time.Time, string
}

type RowWriter interface {
	Write(ctx context.Context, rows []map[string]any) error
}

type Connector interface {
	Connect(ctx context.Context, dsn DSN) error
	// IncrementalRead 拉取下一批数据，返回游标
	IncrementalRead(ctx context.Context, table string, cursor Cursor, limit int) ([]map[string]any, Cursor, error)
	Close() error
}
