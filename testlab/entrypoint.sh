#!/bin/bash
# Initializes MySQL on first boot, starts it, then runs sshd in the foreground.
set -euo pipefail

ssh-keygen -A >/dev/null
install -d -m 700 -o dba -g dba /home/dba/.ssh
install -m 600 -o dba -g dba /lab/authorized_keys /home/dba/.ssh/authorized_keys

# Credentials for root's mysql client and for xtrabackup.
cat > /root/.my.cnf <<EOF
[client]
user=root
password=${MYSQL_ROOT_PASSWORD}
[xtrabackup]
user=root
password=${MYSQL_ROOT_PASSWORD}
EOF
chmod 600 /root/.my.cnf

if [ ! -d /var/lib/mysql/mysql ]; then
	mysqld --initialize-insecure --user=mysql
	mysqlctl start
	# sql_log_bin=0: every host creates its own users, so they must not replicate.
	mysql --no-defaults --socket=/var/run/mysqld/mysqld.sock -uroot <<SQL
SET sql_log_bin = 0;
ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
CREATE USER 'root'@'%' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION;
CREATE USER 'repl'@'%' IDENTIFIED BY '${MYSQL_REPL_PASSWORD}';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
SQL
else
	mysqlctl start
fi

exec /usr/sbin/sshd -D -e
