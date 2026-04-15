package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/alva-cui/sync-tool/internal/cdc"
	"github.com/alva-cui/sync-tool/internal/config"
	"github.com/alva-cui/sync-tool/internal/dlq"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"
)

type JobManager struct {
	mu    sync.RWMutex
	jobs  map[string]context.CancelFunc
	cfg   *config.Config
	store *store.Queries
	tMgr  *transform.Manager
	dlq   *dlq.Writer
}

func NewJobManager(cfg *config.Config, q *store.Queries, tMgr *transform.Manager, dlq *dlq.Writer) *JobManager {
	return &JobManager{
		jobs:  make(map[string]context.CancelFunc),
		cfg:   cfg,
		store: q,
		tMgr:  tMgr,
		dlq:   dlq,
	}
}

func (jm *JobManager) StartCDC(taskID string, srcCfg cdc.SourceConfig) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if _, ok := jm.jobs[taskID]; ok {
		return fmt.Errorf("task %s already running", taskID)
	}

	eng, ok := jm.tMgr.Get(taskID)
	if !ok {
		return fmt.Errorf("transform engine for task %s not found", taskID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	jm.jobs[taskID] = cancel

	job := &CDCSyncJob{
		TaskID:        taskID,
		Cfg:           srcCfg,
		Transformer:   eng,
		DLQWriter:     jm.dlq,
		CursorStore:   jm.store,
		BatchSize:     jm.cfg.CDC.BatchSize,
		FlushInterval: jm.cfg.CDC.FlushInterval,
		// Target 应在实际项目中注入具体目标库连接器，此处预留
		Target: &placeholderTarget{},
	}

	go func() {
		defer func() {
			jm.mu.Lock()
			delete(jm.jobs, taskID)
			jm.mu.Unlock()
			slog.Info("cdc job cleanup", "task", taskID)
		}()
		if err := job.Run(ctx); err != nil && err != context.Canceled {
			slog.Error("cdc job failed", "task", taskID, "err", err)
			// 更新任务状态为 failed (需调用 store.UpdateTaskStatus)
		}
	}()

	return nil
}

func (jm *JobManager) Stop(taskID string) error {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if cancel, ok := jm.jobs[taskID]; ok {
		cancel()
		delete(jm.jobs, taskID)
		return nil
	}
	return fmt.Errorf("task %s not running", taskID)
}

// placeholderTarget 仅用于编译通过，实际应替换为 connector.TargetWriter
type placeholderTarget struct{}

func (p *placeholderTarget) Write(ctx context.Context, eventType cdc.EventType, rows []map[string]any) error {
	_ = rows
	return nil
}
