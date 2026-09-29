package reseed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// sqlDialect holds the replication keywords, which were renamed in MySQL 8.0.23 and 8.2.
type sqlDialect struct {
	stop, reset, start, status string
	change, prefix             string // "CHANGE REPLICATION SOURCE TO" / "SOURCE_"
	publicKey                  string
	resetBinlogs               string
	ioRunning, sqlRunning      string
}

func dialectFor(major, minor, patch int) sqlDialect {
	d := sqlDialect{
		stop: "STOP REPLICA", reset: "RESET REPLICA ALL", start: "START REPLICA", status: "SHOW REPLICA STATUS",
		change: "CHANGE REPLICATION SOURCE TO", prefix: "SOURCE_", publicKey: "GET_SOURCE_PUBLIC_KEY",
		resetBinlogs: "RESET MASTER", ioRunning: "Replica_IO_Running", sqlRunning: "Replica_SQL_Running",
	}
	v := major*10000 + minor*100 + patch
	if v < 80023 {
		d = sqlDialect{
			stop: "STOP SLAVE", reset: "RESET SLAVE ALL", start: "START SLAVE", status: "SHOW SLAVE STATUS",
			change: "CHANGE MASTER TO", prefix: "MASTER_", publicKey: "GET_MASTER_PUBLIC_KEY",
			resetBinlogs: "RESET MASTER", ioRunning: "Slave_IO_Running", sqlRunning: "Slave_SQL_Running",
		}
	}
	if v >= 80200 {
		d.resetBinlogs = "RESET BINARY LOGS AND GTIDS"
	}
	return d
}

func (r *Reseeder) replication(ctx context.Context) error {
	c := r.cfg
	if c.DryRun {
		fmt.Fprintf(r.out, "  [%s] read %s/xtrabackup_binlog_info, then run as %s via an SSH tunnel to %s:\n",
			r.replica.Name(), c.Datadir, c.ReplicaMySQLUser, c.ReplicaMySQLAddr)
		d := dialectFor(8, 0, 23)
		for _, q := range []string{
			d.stop, d.reset,
			"(gtid_mode=ON) " + d.resetBinlogs + "; SET GLOBAL gtid_purged = '<gtid set>'",
			d.change + " SOURCE_HOST='" + c.SourceMySQLHost + "', SOURCE_USER='" + c.ReplUser + "', ... SOURCE_AUTO_POSITION=1 | SOURCE_LOG_FILE/POS",
			d.start,
		} {
			fmt.Fprintf(r.out, "      %s\n", q)
		}
		return nil
	}

	raw, err := r.repAgent.call(ctx, "binlog-info", "--dir", c.Datadir)
	if err != nil {
		return err
	}
	info, err := ParseBinlogInfo(raw.Content)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.out, "    backup position: %s:%d gtid=%q\n", info.File, info.Pos, info.GTIDSet)

	db, err := r.openReplicaDB()
	if err != nil {
		return err
	}
	defer db.Close()
	if err := waitForMySQL(ctx, db, 10*time.Minute); err != nil {
		return err
	}

	var version, gtidMode string
	if err := db.QueryRowContext(ctx, "SELECT VERSION(), @@GLOBAL.gtid_mode").Scan(&version, &gtidMode); err != nil {
		return err
	}
	d, err := dialectForVersion(version)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.out, "    replica MySQL %s, gtid_mode=%s\n", version, gtidMode)
	plan, err := replicationPlan(d, info, gtidMode, c)
	if err != nil {
		return err
	}

	// Pin one connection so session state is consistent across statements.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, st := range plan {
		fmt.Fprintf(r.out, "    sql> %s\n", st.Query) // args are not printed: they include the password
		if _, err := conn.ExecContext(ctx, st.Query, st.Args...); err != nil {
			return fmt.Errorf("%s: %w", st.Query, err)
		}
	}
	return r.waitForReplication(ctx, conn, d)
}

// statement is one SQL statement with its placeholder arguments.
type statement struct {
	Query string
	Args  []any
}

// dialectForVersion picks the SQL keywords for a VERSION() string such as "8.0.43-log".
func dialectForVersion(version string) (sqlDialect, error) {
	var major, minor, patch int
	if _, err := fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch); err != nil {
		return sqlDialect{}, fmt.Errorf("parse version %q: %w", version, err)
	}
	return dialectFor(major, minor, patch), nil
}

// replicationPlan returns the statements that point the replica at the source, positioned
// where the backup was taken: by GTID when gtid_mode is ON, else by binlog file and position.
func replicationPlan(d sqlDialect, info BinlogInfo, gtidMode string, c Config) ([]statement, error) {
	useGTID := gtidMode == "ON"
	if useGTID && info.GTIDSet == "" {
		return nil, errors.New("replica has gtid_mode=ON but the backup recorded no GTID set; is GTID enabled on the source?")
	}
	if !useGTID && gtidMode != "OFF" {
		// OFF_PERMISSIVE / ON_PERMISSIVE: neither positioning mode is safe to pick blindly.
		return nil, fmt.Errorf("replica gtid_mode is %s; set it to ON or OFF before reseeding", gtidMode)
	}
	if !useGTID && info.File == "" {
		return nil, errors.New("backup recorded no binlog position; is binary logging enabled on the source?")
	}

	p := d.prefix
	change := fmt.Sprintf("%s %sHOST = ?, %sPORT = ?, %sUSER = ?, %sPASSWORD = ?, %s = 1",
		d.change, p, p, p, p, d.publicKey)
	args := []any{c.SourceMySQLHost, c.SourceMySQLPort, c.ReplUser, c.ReplPass}

	plan := []statement{{Query: d.stop}, {Query: d.reset}}
	if useGTID {
		plan = append(plan,
			statement{Query: d.resetBinlogs},
			statement{Query: "SET GLOBAL gtid_purged = ?", Args: []any{info.GTIDSet}})
		change += fmt.Sprintf(", %sAUTO_POSITION = 1", p)
	} else {
		change += fmt.Sprintf(", %sLOG_FILE = ?, %sLOG_POS = ?", p, p)
		args = append(args, info.File, info.Pos)
	}
	return append(plan, statement{Query: change, Args: args}, statement{Query: d.start}), nil
}

// openReplicaDB connects to the replica's mysqld through the SSH connection, so port 3306
// doesn't need to be reachable from where go-reseed runs.
func (r *Reseeder) openReplicaDB() (*sql.DB, error) {
	netName := "ssh-" + r.replica.Name()
	mysql.RegisterDialContext(netName, func(ctx context.Context, addr string) (net.Conn, error) {
		return r.replica.Dial(ctx, addr)
	})
	cfg := mysql.NewConfig()
	cfg.Net = netName
	cfg.Addr = r.cfg.ReplicaMySQLAddr
	cfg.User = r.cfg.ReplicaMySQLUser
	cfg.Passwd = r.cfg.ReplicaMySQLPass
	cfg.TLSConfig = "preferred"
	cfg.InterpolateParams = true // CHANGE REPLICATION SOURCE can't take server-side placeholders
	cfg.Timeout = 10 * time.Second
	return sql.Open("mysql", cfg.FormatDSN())
}

func waitForMySQL(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := db.PingContext(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("mysqld not reachable after %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func (r *Reseeder) waitForReplication(ctx context.Context, conn *sql.Conn, d sqlDialect) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		st, err := replicaStatus(ctx, conn, d.status)
		if err != nil {
			return err
		}
		running, err := replicationHealthy(st, d)
		if err != nil {
			return err
		}
		if running {
			fmt.Fprintf(r.out, "    replication running, %s behind source\n", orUnknown(st["Seconds_Behind_Source"], st["Seconds_Behind_Master"]))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replication not running after 60s (io=%s sql=%s)", st[d.ioRunning], st[d.sqlRunning])
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// replicationHealthy reports whether both replication threads are running. It returns an
// error once replication has failed for good, and false, nil while it is still starting.
func replicationHealthy(st map[string]string, d sqlDialect) (bool, error) {
	io, sqlT := st[d.ioRunning], st[d.sqlRunning]
	if io == "Yes" && sqlT == "Yes" {
		return true, nil
	}
	if lastErr := strings.TrimSpace(st["Last_IO_Error"] + " " + st["Last_SQL_Error"]); lastErr != "" && io != "Connecting" {
		return false, fmt.Errorf("replication not running (io=%s sql=%s): %s", io, sqlT, lastErr)
	}
	return false, nil
}

func replicaStatus(ctx context.Context, conn *sql.Conn, query string) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, fmt.Errorf("%s returned no rows", query)
	}
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	st := make(map[string]string, len(cols))
	for i, col := range cols {
		st[col] = string(vals[i])
	}
	return st, rows.Err()
}

func orUnknown(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v + "s"
		}
	}
	return "unknown"
}
