package config

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	JWT      JWTConfig
	Security SecurityConfig
	CDC      CDCConfig
}

type ServerConfig struct {
	Port int    `env:"SERVER_PORT" envDefault:"8080"`
	Env  string `env:"APP_ENV" envDefault:"development"`
}

type DatabaseConfig struct {
	Path string `env:"DB_PATH" envDefault:"./data/sync_tool.db"`
}

type JWTConfig struct {
	Secret string `env:"JWT_SECRET" envDefault:"dev-secret-change-me"`
}

type SecurityConfig struct {
	MasterKey string `env:"VAULT_MASTER_KEY" envDefault:"dev-32-bytes-master-key!!"` // 仅开发用
}

type PostgresConfig struct {
	MaxConns        int32         `env:"PG_MAX_CONNS" envDefault:"20"`
	MinConns        int32         `env:"PG_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime time.Duration `env:"PG_MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnIdleTime time.Duration `env:"PG_MAX_CONN_IDLE_TIME" envDefault:"10m"`
}

// 在 Config 结构体末尾追加
type CDCConfig struct {
	DefaultChanSize int           `env:"CDC_CHAN_SIZE" envDefault:"2000"`
	FlushInterval   time.Duration `env:"CDC_FLUSH_INTERVAL" envDefault:"500ms"`
	BatchSize       int           `env:"CDC_BATCH_SIZE" envDefault:"500"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	os.MkdirAll("./data", 0755)
	return &cfg, nil
}
