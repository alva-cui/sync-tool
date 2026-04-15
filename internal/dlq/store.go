package dlq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alva-cui/sync-tool/internal/store"
)

type Writer struct {
	Queries *store.Queries
}

func New(q *store.Queries) *Writer {
	return &Writer{Queries: q}
}

func (w *Writer) Push(ctx context.Context, taskID, table string, row map[string]any, errMsg string) error {
	data, _ := json.Marshal(row)
	_, err := w.Queries.CreateDLQEntry(ctx, store.CreateDLQEntryParams{
		TaskID:    taskID,
		TableName: table,
		FailedRow: data,
		ErrorMsg:  errMsg,
	})
	if err != nil {
		return fmt.Errorf("write dlq: %w", err)
	}
	return nil
}
