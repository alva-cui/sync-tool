package auth

import (
	"net/http"

	"github.com/casbin/casbin/v2"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/gin-gonic/gin"
)

type RBACEnforcer struct {
	*casbin.Enforcer
}

func NewRBACEnforcer(modelPath, policyPath string) (*RBACEnforcer, error) {
	e, err := casbin.NewEnforcer(modelPath, fileadapter.NewAdapter(policyPath))
	if err != nil {
		return nil, err
	}
	return &RBACEnforcer{Enforcer: e}, nil
}

func (e *RBACEnforcer) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		role := c.GetString("role")
		obj := c.Request.URL.Path
		act := c.Request.Method

		allowed, err := e.Enforce(role, obj, act)
		if err != nil || !allowed {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"code": 403, "msg": "permission denied"})
			return
		}
		c.Next()
	}
}
