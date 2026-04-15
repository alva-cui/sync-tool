package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alva-cui/sync-tool/internal/connector"
	"github.com/alva-cui/sync-tool/internal/dlq"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"
)

type SyncJob struct {
	TaskID    string
	TableName string
	Cursor    connector.Cursor
	BatchSize int
	Interval  time.Duration

	Source      connector.Connector
	Target      connector.RowWriter // 实际项目中应为目标 Connector
	Transformer *transform.Engine
	DLQWriter   *dlq.Writer
	CursorStore *store.Queries
}

// Run 启动增量同步循环（阻塞式，可配合 errgroup/ctx 控制）
func (j *SyncJob) Run(ctx context.Context) error {
	slog.Info("sync job started", "task", j.TaskID, "table", j.TableName)
	defer slog.Info("sync job stopped", "task", j.TaskID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, nextCursor, err := j.Source.IncrementalRead(ctx, j.TableName, j.Cursor, j.BatchSize)
		if err != nil {
			return fmt.Errorf("source read: %w", err)
		}
		if len(rows) == 0 {
			time.Sleep(j.Interval)
			continue
		}

		// 转换 & 过滤
		var validRows []map[string]any
		for _, row := range rows {
			mapped, passed, err := j.Transformer.Execute(ctx, row)
			if err != nil {
				_ = j.DLQWriter.Push(ctx, j.TaskID, j.TableName, row, err.Error())
				continue
			}
			if passed {
				validRows = append(validRows, mapped)
			}
		}

		if len(validRows) == 0 {
			j.Cursor = nextCursor
			continue
		}

		// 批量写入目标
		if err := j.Target.Write(ctx, validRows); err != nil {
			// 整批失败入 DLQ
			for _, r := range validRows {
				_ = j.DLQWriter.Push(ctx, j.TaskID, j.TableName, r, err.Error())
			}
			continue
		}

		// 持久化游标
		_ = j.CursorStore.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
			Status: "running",
			ID:     j.TaskID,
		})
		j.Cursor = nextCursor
		time.Sleep(500 * time.Millisecond) // 背压缓冲
	}
}
