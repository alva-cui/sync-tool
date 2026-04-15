-- name: UpsertCursor :exec
INSERT INTO sync_cursors (task_id, table_name, cursor_field, cursor_value, updated_at)
VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(task_id, table_name) DO UPDATE SET
    cursor_value = excluded.cursor_value,
    updated_at = CURRENT_TIMESTAMP;

-- name: GetCursor :one
SELECT cursor_value FROM sync_cursors 
WHERE task_id = ? AND table_name = ?;