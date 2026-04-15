package main

import (
	"log/slog"
	"os"

	"github.com/alva-cui/sync-tool/internal/api"
	"github.com/alva-cui/sync-tool/internal/auth"
	"github.com/alva-cui/sync-tool/internal/config"
	"github.com/alva-cui/sync-tool/internal/dlq"
	"github.com/alva-cui/sync-tool/internal/engine"
	"github.com/alva-cui/sync-tool/internal/security"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	db, err := store.NewDB(cfg.Database.Path)
	if err != nil {
		slog.Error("init db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	q := store.New(db)
	tMgr := transform.NewManager()
	dlqWriter := dlq.New(q)
	jobMgr := engine.NewJobManager(cfg, q, tMgr, dlqWriter)

	jwtAuth := auth.NewJWTAuth(cfg.JWT.Secret)
	enforcer, _ := auth.NewRBACEnforcer("configs/rbac_model.conf", "configs/rbac_policy.csv")
	vault, _ := security.NewVault(cfg.Security.MasterKey)
	audit := security.NewAudit(q)

	srv := api.New(cfg, q, tMgr, jwtAuth, enforcer, audit, vault, jobMgr)
	if err := srv.Start(); err != nil {
		slog.Error("server exit", "err", err)
		os.Exit(1)
	}
}
