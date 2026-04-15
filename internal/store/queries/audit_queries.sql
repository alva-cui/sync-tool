-- name: CreateAuditLog :execresult
INSERT INTO audit_logs (task_id, action, actor, details)
VALUES (COALESCE(?, 'system'), ?, ?, ?);