CREATE TABLE IF NOT EXISTS sync_cursors (
    task_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    cursor_field TEXT NOT NULL,
    cursor_value TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task_id, table_name)
);

CREATE TABLE IF NOT EXISTS sync_dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    failed_row JSON NOT NULL,
    error_msg TEXT NOT NULL,
    retry_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);