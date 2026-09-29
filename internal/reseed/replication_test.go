package reseed

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestDialectFor(t *testing.T) {
	tests := []struct {
		version                    string
		change, stop, resetBinlogs string
		ioKey                      string
	}{
		{"8.0.22", "CHANGE MASTER TO", "STOP SLAVE", "RESET MASTER", "Slave_IO_Running"},
		{"8.0.23", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET MASTER", "Replica_IO_Running"},
		{"8.0.43", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET MASTER", "Replica_IO_Running"},
		{"8.0.43-log", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET MASTER", "Replica_IO_Running"},
		{"8.0.36-28", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET MASTER", "Replica_IO_Running"},
		{"8.2.0", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET BINARY LOGS AND GTIDS", "Replica_IO_Running"},
		{"8.4.3-commercial", "CHANGE REPLICATION SOURCE TO", "STOP REPLICA", "RESET BINARY LOGS AND GTIDS", "Replica_IO_Running"},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			d, err := dialectForVersion(tt.version)
			if err != nil {
				t.Fatal(err)
			}
			if d.change != tt.change || d.stop != tt.stop || d.resetBinlogs != tt.resetBinlogs || d.ioRunning != tt.ioKey {
				t.Errorf("got change=%q stop=%q resetBinlogs=%q io=%q", d.change, d.stop, d.resetBinlogs, d.ioRunning)
			}
		})
	}

	if _, err := dialectForVersion("not-a-version"); err == nil {
		t.Error("expected error for unparseable version")
	}
}

func TestReplicationPlanGTID(t *testing.T) {
	c := validConfig()
	d := dialectFor(8, 0, 43)
	info := BinlogInfo{File: "binlog.000003", Pos: 197, GTIDSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-57"}

	plan, err := replicationPlan(d, info, "ON", c)
	if err != nil {
		t.Fatal(err)
	}
	want := []statement{
		{Query: "STOP REPLICA"},
		{Query: "RESET REPLICA ALL"},
		{Query: "RESET MASTER"},
		{Query: "SET GLOBAL gtid_purged = ?", Args: []any{info.GTIDSet}},
		{Query: "CHANGE REPLICATION SOURCE TO SOURCE_HOST = ?, SOURCE_PORT = ?, SOURCE_USER = ?, SOURCE_PASSWORD = ?, GET_SOURCE_PUBLIC_KEY = 1, SOURCE_AUTO_POSITION = 1",
			Args: []any{"db002", 3306, "repl", "replpw"}},
		{Query: "START REPLICA"},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Errorf("plan mismatch:\n got  %#v\n want %#v", plan, want)
	}
}

func TestReplicationPlanFilePosition(t *testing.T) {
	c := validConfig()
	d := dialectFor(8, 0, 43)
	info := BinlogInfo{File: "mysql-bin.000042", Pos: 4711}

	plan, err := replicationPlan(d, info, "OFF", c)
	if err != nil {
		t.Fatal(err)
	}
	want := []statement{
		{Query: "STOP REPLICA"},
		{Query: "RESET REPLICA ALL"},
		{Query: "CHANGE REPLICATION SOURCE TO SOURCE_HOST = ?, SOURCE_PORT = ?, SOURCE_USER = ?, SOURCE_PASSWORD = ?, GET_SOURCE_PUBLIC_KEY = 1, SOURCE_LOG_FILE = ?, SOURCE_LOG_POS = ?",
			Args: []any{"db002", 3306, "repl", "replpw", "mysql-bin.000042", uint64(4711)}},
		{Query: "START REPLICA"},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Errorf("plan mismatch:\n got  %#v\n want %#v", plan, want)
	}
	for _, st := range plan {
		if strings.Contains(st.Query, "RESET MASTER") || strings.Contains(st.Query, "gtid_purged") {
			t.Errorf("file/position plan must not touch GTIDs: %s", st.Query)
		}
	}
}

func TestReplicationPlanPre8023Syntax(t *testing.T) {
	plan, err := replicationPlan(dialectFor(8, 0, 22), BinlogInfo{File: "b.1", Pos: 4, GTIDSet: "u:1"}, "ON", validConfig())
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	for _, st := range plan {
		queries = append(queries, st.Query)
	}
	got := strings.Join(queries, "; ")
	for _, want := range []string{"STOP SLAVE", "RESET SLAVE ALL", "CHANGE MASTER TO MASTER_HOST = ?", "GET_MASTER_PUBLIC_KEY = 1", "MASTER_AUTO_POSITION = 1", "START SLAVE"} {
		if !strings.Contains(got, want) {
			t.Errorf("pre-8.0.23 plan missing %q: %s", want, got)
		}
	}
}

func TestReplicationPlanNeverEmbedsSecrets(t *testing.T) {
	c := validConfig()
	c.ReplPass = `p'w"; DROP DATABASE x; --`
	c.SourceMySQLHost = `db002'--`
	for _, mode := range []string{"ON", "OFF"} {
		plan, err := replicationPlan(dialectFor(8, 0, 43), BinlogInfo{File: "b.1", Pos: 4, GTIDSet: "u:1"}, mode, c)
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range plan {
			if strings.Contains(st.Query, c.ReplPass) || strings.Contains(st.Query, c.SourceMySQLHost) {
				t.Errorf("user input must be passed as args, not in the query: %s", st.Query)
			}
			if n := strings.Count(st.Query, "?"); n != len(st.Args) {
				t.Errorf("%d placeholders but %d args in %s", n, len(st.Args), st.Query)
			}
		}
	}
}

func TestReplicationPlanErrors(t *testing.T) {
	d := dialectFor(8, 0, 43)
	tests := []struct {
		name     string
		info     BinlogInfo
		gtidMode string
		wantErr  string
	}{
		{"gtid on but backup has no gtid set", BinlogInfo{File: "b.1", Pos: 4}, "ON", "no GTID set"},
		{"permissive gtid mode", BinlogInfo{File: "b.1", Pos: 4, GTIDSet: "u:1"}, "ON_PERMISSIVE", "ON_PERMISSIVE"},
		{"off permissive", BinlogInfo{File: "b.1", Pos: 4}, "OFF_PERMISSIVE", "OFF_PERMISSIVE"},
		{"no binlog position", BinlogInfo{}, "OFF", "no binlog position"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := replicationPlan(d, tt.info, tt.gtidMode, validConfig())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestReplicationHealthy(t *testing.T) {
	d := dialectFor(8, 0, 43)
	tests := []struct {
		name    string
		st      map[string]string
		running bool
		wantErr string
	}{
		{"both running", map[string]string{"Replica_IO_Running": "Yes", "Replica_SQL_Running": "Yes"}, true, ""},
		{"io connecting", map[string]string{"Replica_IO_Running": "Connecting", "Replica_SQL_Running": "Yes"}, false, ""},
		{"io connecting with transient error", map[string]string{"Replica_IO_Running": "Connecting", "Replica_SQL_Running": "Yes",
			"Last_IO_Error": "error connecting to source"}, false, ""},
		{"io stopped with error", map[string]string{"Replica_IO_Running": "No", "Replica_SQL_Running": "Yes",
			"Last_IO_Error": "Access denied for user 'repl'"}, false, "Access denied"},
		{"sql stopped with error", map[string]string{"Replica_IO_Running": "Yes", "Replica_SQL_Running": "No",
			"Last_SQL_Error": "Duplicate entry"}, false, "Duplicate entry"},
		{"not started yet", map[string]string{"Replica_IO_Running": "No", "Replica_SQL_Running": "No"}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			running, err := replicationHealthy(tt.st, d)
			if running != tt.running {
				t.Errorf("running = %v, want %v", running, tt.running)
			}
			if tt.wantErr == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}

	old := dialectFor(8, 0, 22)
	if ok, err := replicationHealthy(map[string]string{"Slave_IO_Running": "Yes", "Slave_SQL_Running": "Yes"}, old); !ok || err != nil {
		t.Errorf("pre-8.0.23 status keys not recognized: %v %v", ok, err)
	}
}

func TestOrUnknown(t *testing.T) {
	if got := orUnknown("", "5"); got != "5s" {
		t.Errorf("got %q", got)
	}
	if got := orUnknown("0", "5"); got != "0s" {
		t.Errorf("got %q", got)
	}
	if got := orUnknown("", ""); got != "unknown" {
		t.Errorf("got %q", got)
	}
}

func TestReplicationDryRunTouchesNothing(t *testing.T) {
	c := validConfig()
	c.DryRun = true
	replica := &fakeHost{name: "db003"}
	var out bytes.Buffer
	r := New(c, &fakeHost{name: "db002"}, replica, &out)
	r.repAgent = &agentConn{host: replica, path: "/tmp/agent", dry: true}

	if err := r.replication(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(replica.commands()) != 0 {
		t.Errorf("dry run executed commands: %v", replica.commands())
	}
	for _, want := range []string{"STOP REPLICA", "CHANGE REPLICATION SOURCE TO", "START REPLICA", "xtrabackup_binlog_info"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out.String())
		}
	}
}
