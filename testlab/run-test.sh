#!/bin/bash
# Reseeds the lab replica with go-reseed while the source takes writes, then verifies that
# replication is healthy, the errant GTID is gone and the data matches.
# Extra arguments are passed to go-reseed (e.g. --compress none, --verbose).
set -euo pipefail
cd "$(dirname "$0")"

bin=../bin/go-reseed
sql() { docker compose exec -T "$1" mysql -N -e "$2"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

errant_before=$(sql replica "SELECT @@server_uuid")
echo "replica server_uuid before: $errant_before (owns the errant GTID)"

# Keep writing to the source for the whole reseed, so the backup is taken under load.
(
	while :; do
		sql source "INSERT INTO lab.t (pad) VALUES ('during-reseed')" >/dev/null 2>&1 || true
		sleep 0.2
	done
) &
writer=$!
trap 'kill $writer 2>/dev/null || true' EXIT

env -u SSH_AUTH_SOCK \
	MYSQL_ROOT_PASSWORD="${LAB_ROOT_PASSWORD:-lab-root-pw}" \
	MYSQL_REPL_PASSWORD="${LAB_REPL_PASSWORD:-lab-repl-pw}" \
	"$bin" \
	--source 127.0.0.1:2221 --replica 127.0.0.1:2222 \
	--ssh-user dba --ssh-key .ssh/id_ed25519 --known-hosts known_hosts \
	--stream-addr replica --source-mysql-host source --datadir /var/lib/mysql \
	--stop-cmd 'mysqlctl stop' --start-cmd 'mysqlctl start' --status-cmd 'mysqlctl status' \
	--parallel 2 --log-file run.log --yes "$@"

kill $writer; wait $writer 2>/dev/null || true
trap - EXIT

echo
echo "==> verifying"
src_gtids=$(sql source "SELECT @@gtid_executed" | tr -d '\n')
[ "$(sql replica "SELECT WAIT_FOR_EXECUTED_GTID_SET('$src_gtids', 120)")" = 0 ] ||
	fail "replica did not catch up to $src_gtids"

status=$(docker compose exec -T replica mysql -e "SHOW REPLICA STATUS\G")
grep -q 'Replica_IO_Running: Yes' <<<"$status" || fail "IO thread not running"
grep -q 'Replica_SQL_Running: Yes' <<<"$status" || fail "SQL thread not running"
echo "ok  replication threads running"

rep_gtids=$(sql replica "SELECT @@gtid_executed" | tr -d '\n')
[ "$rep_gtids" = "$src_gtids" ] || fail "gtid_executed differs: source=$src_gtids replica=$rep_gtids"
grep -q "$errant_before" <<<"$rep_gtids" && fail "errant GTID from $errant_before still present"
echo "ok  gtid_executed identical to source, errant transaction gone ($rep_gtids)"

[ "$(sql replica "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='errant'")" = 0 ] ||
	fail "errant schema still exists"
echo "ok  errant schema gone"

src_uuid=$(sql source "SELECT @@server_uuid")
rep_uuid=$(sql replica "SELECT @@server_uuid")
[ "$src_uuid" != "$rep_uuid" ] || fail "replica has the source's server_uuid (auto.cnf not removed)"
echo "ok  replica has its own server_uuid ($rep_uuid)"

src_sum=$(sql source "CHECKSUM TABLE lab.t" | awk '{print $2}')
rep_sum=$(sql replica "CHECKSUM TABLE lab.t" | awk '{print $2}')
src_rows=$(sql source "SELECT COUNT(*) FROM lab.t")
during=$(sql replica "SELECT COUNT(*) FROM lab.t WHERE pad='during-reseed'")
[ "$src_sum" = "$rep_sum" ] || fail "CHECKSUM TABLE differs: source=$src_sum replica=$rep_sum"
echo "ok  lab.t identical on both: $src_rows rows, checksum $src_sum ($during written during the reseed)"

echo
echo "PASS"
