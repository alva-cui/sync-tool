package security

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/gin-gonic/gin"
)

type AuditLog struct {
	Queries *store.Queries
}

func NewAudit(q *store.Queries) *AuditLog {
	return &AuditLog{Queries: q}
}

func (a *AuditLog) Log(ctx context.Context, action, actor string, details any) error {
	data, _ := json.Marshal(details)
	_, err := a.Queries.CreateAuditLog(ctx, store.CreateAuditLogParams{
		Action:  action,
		Actor:   actor,
		Details: data,
	})
	return err
}

func (a *AuditLog) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 仅记录写操作
		if c.Request.Method == "GET" {
			c.Next()
			return
		}

		actor := c.GetString("user_id")
		if actor == "" {
			actor = "anonymous"
		}

		c.Next()

		// 异步记录（避免阻塞响应）
		go func() {
			ctx := context.Background()
			details := map[string]any{
				"method": c.Request.Method,
				"path":   c.Request.URL.Path,
				"status": c.Writer.Status(),
			}
			if err := a.Log(ctx, c.Request.Method+" "+c.Request.URL.Path, actor, details); err != nil {
				slog.Warn("audit log failed", "err", err)
			}
		}()
	}
}
