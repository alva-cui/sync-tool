-- name: CreateTask :execresult
INSERT INTO tasks (id, name, status, source_dsn_encrypted, target_dsn_encrypted, mapping_config, cron_expr)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetTaskByID :one
SELECT * FROM tasks WHERE id = ?;

-- name: ListTasks :many
SELECT id, name, status, cron_expr, created_at, updated_at FROM tasks ORDER BY created_at DESC;

-- name: UpdateTaskStatus :exec
UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?;