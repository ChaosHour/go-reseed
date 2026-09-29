//go:build integration

// Package integration reseeds the Docker lab replica end to end. It runs inside the lab
// network via docker-compose.test.yml:
//
//	make test-integration
package integration

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/ChaosHour/go-reseed/internal/agent"
	"github.com/ChaosHour/go-reseed/internal/remote"
	"github.com/ChaosHour/go-reseed/internal/reseed"
)

// TestMain lets this test binary act as the go-reseed agent: the reseed uploads the running
// binary (linux/amd64 inside the test container) to the lab hosts and runs `<it> agent <op>`.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		os.Exit(agent.Main(context.Background(), "integration-test", os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type labEnv struct {
	source, replica string
	sshKey          string
	rootPass        string
	replPass        string
	verbose         bool
}

func loadEnv(t *testing.T) labEnv {
	t.Helper()
	e := labEnv{
		source:   os.Getenv("LAB_SOURCE"),
		replica:  os.Getenv("LAB_REPLICA"),
		sshKey:   os.Getenv("LAB_SSH_KEY"),
		rootPass: os.Getenv("LAB_ROOT_PASSWORD"),
		replPass: os.Getenv("LAB_REPL_PASSWORD"),
		verbose:  os.Getenv("LAB_VERBOSE") != "",
	}
	if e.source == "" || e.replica == "" || e.sshKey == "" {
		t.Skip("LAB_SOURCE, LAB_REPLICA and LAB_SSH_KEY not set; run via `make test-integration`")
	}
	return e
}

func TestReseed(t *testing.T) {
	env := loadEnv(t)
	ctx := t.Context()

	src := openDB(t, env.source, env.rootPass)
	setupLab(t, ctx, env, src)

	for _, compress := range []string{"zstd", "none"} {
		t.Run("compress="+compress, func(t *testing.T) {
			reseedAndVerify(t, ctx, env, src, compress)
		})
	}
}

// setupLab points the replica at the source and loads test data, unless already done.
func setupLab(t *testing.T, ctx context.Context, env labEnv, src *sql.DB) {
	t.Helper()
	rep := openDB(t, env.replica, env.rootPass)
	defer rep.Close()

	mustExec(t, ctx, rep, "STOP REPLICA")
	mustExec(t, ctx, rep, "RESET REPLICA ALL")
	mustExec(t, ctx, rep, "CHANGE REPLICATION SOURCE TO SOURCE_HOST = ?, SOURCE_USER = 'repl', SOURCE_PASSWORD = ?, SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1",
		env.source, env.replPass)
	mustExec(t, ctx, rep, "START REPLICA")

	mustExec(t, ctx, src, "CREATE DATABASE IF NOT EXISTS lab")
	mustExec(t, ctx, src, "CREATE TABLE IF NOT EXISTS lab.t (id INT AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(200), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	if queryString(t, ctx, src, "SELECT COUNT(*) FROM lab.t") == "0" {
		mustExec(t, ctx, src, `INSERT INTO lab.t (pad) SELECT REPEAT('x', 200) FROM
			(SELECT 1 FROM information_schema.columns LIMIT 500) a, (SELECT 1 FROM information_schema.columns LIMIT 200) b`)
	}
	waitForGTIDs(t, ctx, src, rep)
}

func reseedAndVerify(t *testing.T, ctx context.Context, env labEnv, src *sql.DB, compress string) {
	// An errant transaction on the replica, which the reseed must discard.
	rep := openDB(t, env.replica, env.rootPass)
	mustExec(t, ctx, rep, "CREATE DATABASE IF NOT EXISTS errant")
	oldUUID := queryString(t, ctx, rep, "SELECT @@server_uuid")
	rep.Close()

	// Write to the source for the whole reseed.
	wctx, stopWriter := context.WithCancel(ctx)
	var written atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for wctx.Err() == nil {
			if _, err := src.ExecContext(wctx, "INSERT INTO lab.t (pad) VALUES ('during-reseed')"); err == nil {
				written.Add(1)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	runReseed(t, ctx, env, compress)
	stopWriter()
	<-writerDone

	rep = openDB(t, env.replica, env.rootPass)
	defer rep.Close()
	waitForGTIDs(t, ctx, src, rep)

	st := replicaStatus(t, ctx, rep)
	if st["Replica_IO_Running"] != "Yes" || st["Replica_SQL_Running"] != "Yes" {
		t.Fatalf("replication not running: io=%s sql=%s io_err=%q sql_err=%q",
			st["Replica_IO_Running"], st["Replica_SQL_Running"], st["Last_IO_Error"], st["Last_SQL_Error"])
	}

	srcGTIDs := queryString(t, ctx, src, "SELECT @@GLOBAL.gtid_executed")
	repGTIDs := queryString(t, ctx, rep, "SELECT @@GLOBAL.gtid_executed")
	if repGTIDs != srcGTIDs {
		t.Errorf("gtid_executed differs:\n source  %s\n replica %s", srcGTIDs, repGTIDs)
	}
	if strings.Contains(repGTIDs, oldUUID) {
		t.Errorf("errant GTID from %s survived the reseed: %s", oldUUID, repGTIDs)
	}
	if n := queryString(t, ctx, rep, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'errant'"); n != "0" {
		t.Error("errant schema survived the reseed")
	}

	newUUID := queryString(t, ctx, rep, "SELECT @@server_uuid")
	if srcUUID := queryString(t, ctx, src, "SELECT @@server_uuid"); newUUID == srcUUID {
		t.Errorf("replica kept the source's server_uuid %s; auto.cnf was not removed", newUUID)
	}

	srcSum := checksum(t, ctx, src, "lab.t")
	repSum := checksum(t, ctx, rep, "lab.t")
	if srcSum != repSum {
		t.Errorf("CHECKSUM TABLE lab.t: source %s, replica %s", srcSum, repSum)
	}
	if written.Load() == 0 {
		t.Error("no writes reached the source during the reseed; the load generator isn't working")
	}
	t.Logf("verified: %s rows, checksum %s, %d rows written during the reseed",
		queryString(t, ctx, rep, "SELECT COUNT(*) FROM lab.t"), repSum, written.Load())
}

func runReseed(t *testing.T, ctx context.Context, env labEnv, compress string) {
	t.Helper()

	// xtrabackup is chatty: keep its output in a file and show the tail only on failure.
	logf, err := os.CreateTemp("", "reseed-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		logf.Close()
		if t.Failed() {
			t.Logf("last lines of %s:\n%s", logf.Name(), tail(logf.Name(), 40))
		}
	})
	var logw io.Writer = logf
	if env.verbose {
		logw = io.MultiWriter(logf, t.Output())
	}

	opts := remote.Options{User: "dba", KeyFile: env.sshKey, InsecureHostKey: true, Sudo: true, Log: logw}
	source, err := remote.Connect(env.source, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	replica, err := remote.Connect(env.replica, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	cfg := reseed.Config{
		SourceHost:       env.source,
		ReplicaHost:      env.replica,
		SourceDatadir:    "/var/lib/mysql",
		Datadir:          "/var/lib/mysql",
		StreamAddr:       env.replica,
		Port:             4000,
		Parallel:         2,
		Compress:         compress,
		UseMemory:        "256M",
		StopCmd:          "mysqlctl stop",
		StartCmd:         "mysqlctl start",
		StatusCmd:        "mysqlctl status",
		MySQLOSUser:      "mysql",
		ReplicaMySQLAddr: "127.0.0.1:3306",
		ReplicaMySQLUser: "root",
		ReplicaMySQLPass: env.rootPass,
		SourceMySQLHost:  env.source,
		SourceMySQLPort:  3306,
		ReplUser:         "repl",
		ReplPass:         env.replPass,
		AgentDir:         "/tmp",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	rs := reseed.New(cfg, source, replica, t.Output())
	steps, err := rs.Select("", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.Run(ctx, steps); err != nil {
		t.Fatal(err)
	}
}

func openDB(t *testing.T, host, pass string) *sql.DB {
	t.Helper()
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = host + ":3306"
	cfg.User = "root"
	cfg.Passwd = pass
	cfg.TLSConfig = "preferred"
	cfg.InterpolateParams = true
	cfg.Timeout = 5 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := db.PingContext(t.Context())
		if err == nil {
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: mysql not reachable: %v", host, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func waitForGTIDs(t *testing.T, ctx context.Context, src, rep *sql.DB) {
	t.Helper()
	gtids := queryString(t, ctx, src, "SELECT @@GLOBAL.gtid_executed")
	if got := queryString(t, ctx, rep, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 120)", gtids); got != "0" {
		t.Fatalf("replica did not reach source's gtid_executed %s within 120s", gtids)
	}
}

func mustExec(t *testing.T, ctx context.Context, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func queryString(t *testing.T, ctx context.Context, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var s sql.NullString
	if err := db.QueryRowContext(ctx, q, args...).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return strings.ReplaceAll(s.String, "\n", "")
}

func checksum(t *testing.T, ctx context.Context, db *sql.DB, table string) string {
	t.Helper()
	var name string
	var sum sql.NullInt64
	if err := db.QueryRowContext(ctx, "CHECKSUM TABLE "+table).Scan(&name, &sum); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprint(sum.Int64)
}

func replicaStatus(t *testing.T, ctx context.Context, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if !rows.Next() {
		t.Fatal("SHOW REPLICA STATUS returned no rows")
	}
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	st := make(map[string]string, len(cols))
	for i, c := range cols {
		st[c] = string(vals[i])
	}
	return st
}

func tail(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return err.Error()
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}
