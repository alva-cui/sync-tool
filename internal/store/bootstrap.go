package store

import (
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func NewDB(dbPath string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// 获取所有迁移文件并排序
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	// 按文件名排序确保执行顺序 (001_ → 002_ → ...)
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sql" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	// 顺序执行每个迁移文件
	for _, fname := range files {
		content, err := migrationsFS.ReadFile(filepath.Join("migrations", fname))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("read %s: %w", fname, err)
		}
		if _, err := db.Exec(string(content)); err != nil {
			db.Close()
			return nil, fmt.Errorf("exec %s: %w", fname, err)
		}
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	return db, nil
}
