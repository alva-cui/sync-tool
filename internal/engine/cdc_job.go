package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alva-cui/sync-tool/internal/cdc"
	"github.com/alva-cui/sync-tool/internal/connector"
	"github.com/alva-cui/sync-tool/internal/dlq"
	"github.com/alva-cui/sync-tool/internal/metrics"
	"github.com/alva-cui/sync-tool/internal/store"
	"github.com/alva-cui/sync-tool/internal/transform"
)

type CDCSyncJob struct {
	TaskID        string
	Cfg           cdc.SourceConfig
	TargetCfg     connector.WriteConfig // 🆕 目标库配置
	Transformer   *transform.Engine
	DLQWriter     *dlq.Writer
	CursorStore   *store.Queries
	BatchSize     int
	FlushInterval time.Duration
}

func newCDCSource(cfg cdc.SourceConfig) (cdc.Source, error) {
	switch cfg.Driver {
	case "mysql", "mariadb":
		return cdc.NewMySQLBinlog(cfg), nil
	case "postgres", "postgresql":
		pgCfg := cdc.PGConfig{
			Host:     cfg.Host,
			Port:     uint16(cfg.Port),
			User:     cfg.User,
			Pass:     cfg.Pass,
			DBName:   cfg.DBName,
			Table:    cfg.Table,
			ChanSize: cfg.ChanSize,
		}
		return cdc.NewPGReplication(pgCfg), nil
	default:
		return nil, fmt.Errorf("unsupported CDC driver: %s", cfg.Driver)
	}
}

func (j *CDCSyncJob) Run(ctx context.Context) error {
	slog.Info("cdc job started", "task", j.TaskID, "table", j.Cfg.Table)

	// 加载游标
	pos, err := j.CursorStore.GetCursor(ctx, store.GetCursorParams{
		TaskID:    j.TaskID,
		TableName: j.Cfg.Table,
	})
	if err != nil || pos == "" {
		slog.Info("no cursor found, starting from beginning", "task", j.TaskID)
		pos = ""
	}

	// ✅ 使用工厂创建源
	target, err := connector.NewTargetWriter(j.TargetCfg)
	if err != nil {
		return fmt.Errorf("create target writer: %w", err)
	}
	defer target.Close()

	// 启动复制流
	go func() {
		if err := src.Start(ctx, pos); err != nil {
			slog.Error("cdc source crashed", "task", j.TaskID, "err", err)
		}
	}()
	var buf []map[string]any
	var bufType cdc.EventType
	ticker := time.NewTicker(j.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := j.Target.Write(ctx, bufType, buf); err != nil {
			for _, r := range buf {
				_ = j.DLQWriter.Push(ctx, j.TaskID, j.Cfg.Table, r, err.Error())
			}
		}
		buf = buf[:0]
	}

	eventCh := src.Events()

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case evt, ok := <-eventCh:
			if !ok {
				return fmt.Errorf("cdc channel closed")
			}

			metrics.IncCDCEvent(j.TaskID, j.Cfg.Driver, string(evt.Type))
			lag := time.Since(evt.Timestamp).Seconds()
			metrics.SetReplicationLag(j.TaskID, j.Cfg.Driver, lag)

			mapped, passed, err := j.Transformer.Execute(ctx, evt.Data)
			if err != nil {
				_ = j.DLQWriter.Push(ctx, j.TaskID, evt.Table, evt.Data, err.Error())
				continue
			}
			if !passed {
				continue
			}

			if len(buf) == 0 {
				bufType = evt.Type
			}
			buf = append(buf, mapped)

			// ✅ 修复: 直接调用，无返回值
			j.saveCursor(evt.Cursor)

			if len(buf) >= j.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// saveCursor 持久化游标到数据库（无返回值）
func (j *CDCSyncJob) saveCursor(pos string) {
	ctx := context.Background()
	_ = j.CursorStore.UpsertCursor(ctx, store.UpsertCursorParams{
		TaskID:      j.TaskID,
		TableName:   j.Cfg.Table,
		CursorField: "binlog_pos",
		CursorValue: pos,
	})
}
