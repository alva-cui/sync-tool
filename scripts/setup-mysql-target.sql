-- 创建目标数据库
CREATE DATABASE IF NOT EXISTS sync_target;

-- 创建同步账号（仅写入权限）
CREATE USER 'sync_writer'@'%' IDENTIFIED BY 'StrongPass123!';
GRANT INSERT, UPDATE, DELETE, SELECT ON sync_target.* TO 'sync_writer'@'%';
FLUSH PRIVILEGES;

-- 创建测试表
USE sync_target;
CREATE TABLE IF NOT EXISTS test_users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);