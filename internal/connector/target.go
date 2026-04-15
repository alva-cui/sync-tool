package connector

import (
	"context"
	"fmt"

	"github.com/alva-cui/sync-tool/internal/cdc"
)

// WriteConfig 目标库写入配置
type WriteConfig struct {
	Driver           string // "mysql" | "postgres"
	Host             string
	Port             int
	User             string
	Pass             string // 解密后传入
	DBName           string
	Table            string   // 目标表名
	PrimaryKey       []string // 主键/唯一键列名，用于 UPSERT
	BatchSize        int
	FlushInterval    int    // 毫秒
	ConflictStrategy string // "upsert" | "ignore" | "error"
}

// TargetWriter 通用目标写入接口
type TargetWriter interface {
	// Write 批量写入数据，支持 INSERT/UPDATE/DELETE 语义
	Write(ctx context.Context, eventType cdc.EventType, rows []map[string]any) error
	// Close 释放连接资源
	Close() error
	// String 返回标识（用于日志）
	String() string
}

// NewTargetWriter 工厂函数：根据驱动创建具体实现
func NewTargetWriter(cfg WriteConfig) (TargetWriter, error) {
	switch cfg.Driver {
	case "mysql", "mariadb":
		return NewMySQLTarget(cfg)
	case "postgres", "postgresql":
		return NewPGTarget(cfg)
	default:
		return nil, fmt.Errorf("unsupported target driver: %s", cfg.Driver)
	}
}
