package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/alva-cui/sync-tool/internal/cdc"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type response struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data,omitempty"`
}

type startCDCRequest struct {
	SourceDSN string `json:"source_dsn" binding:"required"`
	Table     string `json:"table" binding:"required"`
}

func ok(c *gin.Context, data any) {
	c.JSON(http.StatusOK, response{Code: 0, Msg: "success", Data: data})
}

func sendError(c *gin.Context, status int, msg string) {
	c.JSON(status, response{Code: status, Msg: msg})
}

func (s *Server) health(c *gin.Context) {
	ok(c, map[string]string{"status": "ok", "version": "v0.2.0"})
}
func (s *Server) createTask(c *gin.Context) {
	var req struct {
		Name          string          `json:"name" binding:"required"`
		SourceDSN     string          `json:"source_dsn" binding:"required"`
		TargetDSN     string          `json:"target_dsn" binding:"required"`
		CronExpr      string          `json:"cron_expr"`
		MappingConfig json.RawMessage `json:"mapping_config" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, err.Error())
		return
	}

	// 🔒 加密凭证后落盘
	srcEnc, err := s.Vault.Encrypt(req.SourceDSN)
	if err != nil {
		return
	} // 实际项目应返回具体错误
	tgtEnc, err := s.Vault.Encrypt(req.TargetDSN)
	if err != nil {
		return
	}

	// 校验映射配置
	var tCfg transform.Config
	if err := json.Unmarshal(req.MappingConfig, &tCfg); err != nil {
		sendError(c, http.StatusBadRequest, "invalid mapping_config: "+err.Error())
		return
	}
	if err := transform.NewEngine().Compile(tCfg); err != nil {
		sendError(c, http.StatusBadRequest, "mapping compile failed: "+err.Error())
		return
	}

	id := uuid.New().String()
	res, err := s.Store.CreateTask(c, store.CreateTaskParams{
		ID:                 id,
		Name:               req.Name,
		Status:             "draft",
		SourceDsnEncrypted: srcEnc,
		TargetDsnEncrypted: tgtEnc,
		CronExpr:           req.CronExpr,
		MappingConfig:      req.MappingConfig,
	})
	if err != nil {
		sendError(c, http.StatusInternalServerError, "db error")
		return
	}
	rows, _ := res.RowsAffected()
	slog.Info("task stop requested", "rows", rows)
	s.TransformMgr.CompileAndCache(id, tCfg)
	ok(c, gin.H{"id": id})
}

func (s *Server) getTask(c *gin.Context) {
	task, err := s.Store.GetTaskByID(c, c.Param("id"))
	if err != nil {
		sendError(c, http.StatusNotFound, "not found")
		return
	}
	// 🛡️ 返回前端时强制脱敏，永不返回明文
	task.SourceDsnEncrypted = "********"
	task.TargetDsnEncrypted = "********"
	ok(c, task)
}

func (s *Server) listTasks(c *gin.Context) {
	tasks, err := s.Store.ListTasks(c)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "query failed")
		return
	}
	ok(c, tasks)
}

func (s *Server) updateTaskStatus(c *gin.Context) {
	var req struct {
		Status string `json:"status" binding:"required,oneof=running paused draft failed"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.Store.UpdateTaskStatus(c, store.UpdateTaskStatusParams{
		Status: req.Status,
		ID:     c.Param("id"),
	}); err != nil {
		sendError(c, http.StatusInternalServerError, "update failed")
		return
	}
	ok(c, nil)
}

func (s *Server) startTask(c *gin.Context) {
	taskID := c.Param("id")
	// 1. 获取缓存引擎
	_, err := s.TransformMgr.Get(taskID)
	if !err {
		sendError(c, http.StatusBadRequest, "task not compiled or not found")
		return
	}
	// 2. 实际项目中此处应注入 connector/Target/DLQ 并启动 errgroup
	// 示例：s.EnginePool.StartTask(ctx, taskID, eng)
	slog.Info("task start requested", "task_id", taskID)
	ok(c, gin.H{"status": "starting", "note": "engine pool integration pending"})
}

func (s *Server) login(c *gin.Context) {
	var req struct {
		User string `json:"user" binding:"required"`
		Role string `json:"role" binding:"required,oneof=admin dev viewer"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, err.Error())
		return
	}
	// 生产环境应对接用户表/密码校验，此处为演示
	token, err := s.JWTAuth.GenerateToken(req.User, req.Role)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "token gen failed")
		return
	}
	ok(c, gin.H{"token": token})
}

func (s *Server) startCDCTask(c *gin.Context) {
	taskID := c.Param("id")
	var req startCDCRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, err.Error())
		return
	}

	// 解析 DSN 简化示例（生产应使用标准 DSN 解析库）
	parts := strings.SplitN(req.SourceDSN, "@", 2)
	if len(parts) != 2 {
		sendError(c, 400, "invalid dsn")
		return
	}
	creds := strings.SplitN(parts[0], "//", 2)[1]
	userPass := strings.SplitN(creds, ":", 2)
	hostPort := strings.SplitN(parts[1], "/", 2)

	srcCfg := cdc.DefaultSourceConfig()
	srcCfg.Host = strings.Split(hostPort[0], ":")[0]
	if p, _ := strconv.Atoi(strings.Split(hostPort[0], ":")[1]); p > 0 {
		srcCfg.Port = p
	}
	srcCfg.User = userPass[0]
	srcCfg.Pass = userPass[1]
	srcCfg.DBName = hostPort[1]
	srcCfg.Table = req.Table

	if err := s.JobMgr.StartCDC(taskID, srcCfg); err != nil {
		sendError(c, http.StatusConflict, err.Error())
		return
	}
	ok(c, gin.H{"status": "cdc started"})
}

func (s *Server) stopTask(c *gin.Context) {
	taskID := c.Param("id")
	slog.Info("task stop requested", "task_id", taskID)
	if err := s.JobMgr.Stop(taskID); err != nil {
		sendError(c, http.StatusNotFound, err.Error())
		return
	}
	ok(c, gin.H{"status": "stopping"})
}
