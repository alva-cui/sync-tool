-- 创建目标数据库
CREATE DATABASE sync_target;

-- 创建同步账号（仅写入权限）
CREATE ROLE sync_writer WITH LOGIN PASSWORD 'StrongPass123!';
GRANT CONNECT ON DATABASE sync_target TO sync_writer;
GRANT USAGE ON SCHEMA public TO sync_writer;
GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA public TO sync_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE, SELECT ON TABLES TO sync_writer;

-- 创建测试表
\c sync_target
CREATE TABLE IF NOT EXISTS test_users (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);