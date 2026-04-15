#!/bin/bash
set -e

echo "🐬 Starting MySQL with binlog for CDC testing..."

docker run -d --name sync-mysql-cdc \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_USER=sync_user \
  -e MYSQL_PASSWORD=sync_pass \
  -e MYSQL_DATABASE=sync_test \
  -p 3306:3306 \
  mysql:8.0 \
  --server-id=1 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON

# 等待 MySQL 就绪
for i in {1..30}; do
  if mysql -h 127.0.0.1 -P 3306 -uroot -proot123 -e "SELECT 1" &>/dev/null; then
    echo "✅ MySQL ready"
    break
  fi
  echo "⏳ Waiting for MySQL... ($i/30)"
  sleep 1
done

# 授权复制权限
mysql -h 127.0.0.1 -P 3306 -uroot -proot123 <<EOF
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
CREATE TABLE IF NOT EXISTS sync_test.users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO sync_test.users (name, email) VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com');
EOF

echo "🔑 CDC Connection string:"
echo "  host=127.0.0.1 port=3306 user=sync_user password=sync_pass dbname=sync_test"
echo ""
echo "🧪 Test: Start sync-tool task with cdc_mode=true, then run:"
echo "  mysql -h 127.0.0.1 -P 3306 -usync_user -psync_pass sync_test -e \"INSERT INTO users (name,email) VALUES ('Charlie','c@test.com');\""
echo ""
echo "🧹 Cleanup: docker rm -f sync-mysql-cdc"