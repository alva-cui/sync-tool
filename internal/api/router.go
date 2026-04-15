package api

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/alva-cui/sync-tool/internal/auth"
	"github.com/alva-cui/sync-tool/internal/engine"
	"github.com/alva-cui/sync-tool/internal/security"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"

	"github.com/alva-cui/sync-tool/internal/config"
	"github.com/gin-gonic/gin"
)

type Server struct {
	Router       *gin.Engine
	Store        *store.Queries
	Cfg          *config.Config
	TransformMgr *transform.Manager
	JWTAuth      *auth.JWTAuth
	Enforcer     *auth.RBACEnforcer
	Audit        *security.AuditLog
	Vault        *security.Vault
	JobMgr       *engine.JobManager
}

func New(cfg *config.Config, q *store.Queries, tMgr *transform.Manager, jwtAuth *auth.JWTAuth, enforcer *auth.RBACEnforcer, audit *security.AuditLog, vault *security.Vault, jobMgr *engine.JobManager) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(func(c *gin.Context) {
		slog.Info("request", "method", c.Request.Method, "path", c.Request.URL.Path)
		c.Next()
	})

	s := &Server{Router: r, Store: q, Cfg: cfg, TransformMgr: tMgr, JWTAuth: jwtAuth, Enforcer: enforcer, Audit: audit, Vault: vault, JobMgr: jobMgr}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// 公开路由
	s.Router.POST("/api/v1/login", s.login)

	// 受保护路由
	v1 := s.Router.Group("/api/v1")
	v1.Use(s.JWTAuth.Middleware())
	v1.Use(s.Enforcer.Middleware())
	v1.Use(s.Audit.Middleware())

	v1.GET("/health", s.health)
	v1.POST("/preview", s.previewMapping)

	tasks := v1.Group("/tasks")
	tasks.POST("", s.createTask)
	tasks.GET("", s.listTasks)
	tasks.GET("/:id", s.getTask)
	tasks.PUT("/:id/status", s.updateTaskStatus)
	tasks.POST("/:id/start", s.startTask)
	tasks.POST("/:id/stop", s.stopTask)
	tasks.POST("/:id/start/cdc", s.startCDCTask)

	s.Router.GET("/metrics", func(c *gin.Context) { c.Status(http.StatusOK) }) // 预留
}

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Cfg.Server.Port)
	slog.Info("listening", "addr", addr)
	return http.ListenAndServe(addr, s.Router)
}
