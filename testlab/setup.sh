#!/bin/bash
# Brings up the lab: GTID replication from source to replica, some data on the source,
# and an errant transaction on the replica that a reseed should wipe out.
set -euo pipefail
cd "$(dirname "$0")"

[ -f .ssh/id_ed25519 ] || { mkdir -p .ssh; ssh-keygen -q -t ed25519 -N '' -C go-reseed-lab -f .ssh/id_ed25519; }
docker compose up -d --build --wait-timeout 180 >/dev/null

sql() { docker compose exec -T "$1" mysql -N -e "$2"; }

echo "waiting for mysqld and sshd..."
for svc in source replica; do
	for _ in $(seq 120); do
		docker compose exec -T "$svc" mysqladmin --no-defaults --socket=/var/run/mysqld/mysqld.sock ping --silent >/dev/null 2>&1 && break
		sleep 1
	done
done
for port in 2221 2222; do
	for _ in $(seq 30); do ssh-keyscan -p "$port" 127.0.0.1 >/dev/null 2>&1 && break; sleep 1; done
done
ssh-keyscan -q -p 2221 127.0.0.1 > known_hosts
ssh-keyscan -q -p 2222 127.0.0.1 >> known_hosts

echo "configuring replication..."
sql replica "STOP REPLICA; RESET REPLICA ALL;
  CHANGE REPLICATION SOURCE TO SOURCE_HOST='source', SOURCE_USER='repl',
    SOURCE_PASSWORD='${LAB_REPL_PASSWORD:-lab-repl-pw}', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1;
  START REPLICA;"

echo "loading data on the source..."
sql source "CREATE DATABASE IF NOT EXISTS lab;
  CREATE TABLE IF NOT EXISTS lab.t (id INT AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(200), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
  INSERT INTO lab.t (pad) SELECT REPEAT('x', 200) FROM
    (SELECT 1 FROM information_schema.columns LIMIT 1000) a, (SELECT 1 FROM information_schema.columns LIMIT 200) b;"

echo "adding an errant transaction on the replica..."
sql replica "CREATE DATABASE IF NOT EXISTS errant;"

src_gtids=$(sql source 'SELECT @@gtid_executed' | tr -d '\n')
sql replica "SELECT WAIT_FOR_EXECUTED_GTID_SET('$src_gtids', 120)" >/dev/null
echo
echo "source rows:  $(sql source 'SELECT COUNT(*) FROM lab.t')"
echo "replica rows: $(sql replica 'SELECT COUNT(*) FROM lab.t')"
echo "source  gtid_executed: $(sql source 'SELECT @@gtid_executed' | tr -d '\n')"
echo "replica gtid_executed: $(sql replica 'SELECT @@gtid_executed' | tr -d '\n')"
docker compose exec -T replica mysql -e "SHOW REPLICA STATUS\G" | grep -E 'Replica_(IO|SQL)_Running:'
