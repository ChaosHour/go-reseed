# A MySQL 8.0 host that go-reseed can drive: sshd as PID 1, mysqld managed by mysqlctl,
# and Percona XtraBackup 8.0. No netcat or ss: the go-reseed agent handles the stream itself.
FROM mysql:8.0.43-oracle

RUN microdnf install -y openssh-server sudo zstd \
 && rpm -i https://repo.percona.com/yum/percona-release-latest.noarch.rpm \
 && percona-release enable-only pxb-80 release \
 && microdnf install -y percona-xtrabackup-80 \
 && microdnf clean all \
 && useradd -m dba \
 && echo 'dba ALL=(root) NOPASSWD: ALL' > /etc/sudoers.d/dba \
 && chmod 440 /etc/sudoers.d/dba

COPY mysqlctl /usr/bin/mysqlctl
COPY entrypoint.sh /usr/local/bin/lab-entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/usr/local/bin/lab-entrypoint.sh"]
