-- name: CreateDLQEntry :execresult
INSERT INTO sync_dlq (task_id, table_name, failed_row, error_msg, retry_count)
VALUES (?, ?, ?, ?, 0);