package api

import (
	"net/http"

	"github.com/alva-cui/sync-tool/internal/transform"
	"github.com/gin-gonic/gin"
)

type PreviewRequest struct {
	Config    transform.Config `json:"config"`
	SampleRow map[string]any   `json:"sample_row"`
}

func (s *Server) previewMapping(c *gin.Context) {
	var req PreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		sendError(c, http.StatusBadRequest, err.Error())
		return
	}

	eng := transform.NewEngine()
	if err := eng.Compile(req.Config); err != nil {
		sendError(c, http.StatusBadRequest, "compile failed: "+err.Error())
		return
	}

	result, passed, err := eng.Execute(c, req.SampleRow)
	if err != nil {
		sendError(c, http.StatusInternalServerError, "execution failed: "+err.Error())
		return
	}

	ok(c, gin.H{
		"passed": passed,
		"result": result,
		"note":   "dry-run mode, no data written",
	})
}
