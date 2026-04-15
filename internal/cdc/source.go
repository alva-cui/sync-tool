package cdc

import "context"

// Source 定义 CDC 数据源的统一契约（MySQL/PG 通用）
type Source interface {
	// Start 启动 CDC 流，initialCursor 格式因源而异：
	// - MySQL: "mysql-bin.000001:154"
	// - PG:    "0/1A2B3C4D" (LSN hex string)
	Start(ctx context.Context, initialCursor string) error

	// Events 返回只读事件通道，供消费层拉取
	Events() <-chan *Event

	// Close 优雅关闭连接与资源
	Close() error

	// String 返回源标识（用于日志）
	String() string
}
