package cdc

// SourceConfig 通用 CDC 源配置（MySQL/PG 共用）
type SourceConfig struct {
	Driver   string // 🆕 "mysql" | "postgres"
	Host     string
	Port     int
	User     string
	Pass     string
	DBName   string
	Table    string
	ChanSize int
}

// DefaultSourceConfig 提供安全默认值
func DefaultSourceConfig() SourceConfig {
	return SourceConfig{
		Port:     3306, // MySQL 默认端口
		ChanSize: 2000,
	}
}

// DefaultPGConfig PostgreSQL 专用默认值
func DefaultPGConfig() SourceConfig {
	cfg := DefaultSourceConfig()
	cfg.Port = 5432
	cfg.Driver = "postgres"
	return cfg
}
