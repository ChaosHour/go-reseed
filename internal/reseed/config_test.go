package reseed

import (
	"strings"
	"testing"
)

// validConfig returns a Config that passes Validate; tests change one field at a time.
func validConfig() Config {
	return Config{
		SourceHost:       "db002",
		ReplicaHost:      "db003",
		SourceDatadir:    "/var/lib/mysql",
		Datadir:          "/var/lib/mysql",
		StreamAddr:       "db003",
		Port:             4000,
		Parallel:         4,
		Compress:         "zstd",
		UseMemory:        "2G",
		StopCmd:          "systemctl stop 'mysqld'",
		StartCmd:         "systemctl start 'mysqld'",
		StatusCmd:        "systemctl is-active --quiet 'mysqld'",
		MySQLOSUser:      "mysql",
		ReplicaMySQLAddr: "127.0.0.1:3306",
		ReplicaMySQLUser: "root",
		ReplicaMySQLPass: "rootpw",
		SourceMySQLHost:  "db002",
		SourceMySQLPort:  3306,
		ReplUser:         "repl",
		ReplPass:         "replpw",
		AgentDir:         "/tmp",
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	c := validConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	c.Logdir = "/db/logs01"
	c.Compress = "none"
	if err := c.Validate(); err != nil {
		t.Fatalf("valid config with logdir rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"missing source", func(c *Config) { c.SourceHost = "" }, "--source and --replica are required"},
		{"missing replica", func(c *Config) { c.ReplicaHost = "" }, "--source and --replica are required"},
		{"same host", func(c *Config) { c.ReplicaHost = "db002" }, "same host"},
		{"same host with default port", func(c *Config) { c.ReplicaHost = "db002:22" }, "same host"},
		{"same host different case", func(c *Config) { c.ReplicaHost = "DB002" }, "same host"},
		{"relative datadir", func(c *Config) { c.Datadir = "var/lib/mysql" }, "--datadir: \"var/lib/mysql\" must be an absolute path"},
		{"root datadir", func(c *Config) { c.Datadir = "/" }, "--datadir: \"/\" is too close to /"},
		{"top-level datadir", func(c *Config) { c.Datadir = "/db" }, "too close to /"},
		{"datadir escaping via ..", func(c *Config) { c.Datadir = "/db/data01/.." }, "too close to /"},
		{"empty datadir", func(c *Config) { c.Datadir = "" }, "--datadir"},
		{"top-level source datadir", func(c *Config) { c.SourceDatadir = "/var" }, "--source-datadir"},
		{"relative logdir", func(c *Config) { c.Logdir = "logs" }, "--logdir"},
		{"logdir inside datadir", func(c *Config) { c.Logdir = "/var/lib/mysql/redo" }, "inside --datadir"},
		{"bad compression", func(c *Config) { c.Compress = "gzip" }, "--compress must be"},
		{"relative agent dir", func(c *Config) { c.AgentDir = "tmp" }, "--agent-dir"},
		{"zero parallel", func(c *Config) { c.Parallel = 0 }, "--parallel"},
		{"port zero", func(c *Config) { c.Port = 0 }, "--port"},
		{"port too high", func(c *Config) { c.Port = 70000 }, "--port"},
		{"stream addr with path", func(c *Config) { c.StreamAddr = "db003/../../etc" }, "--stream-addr"},
		{"stream addr with shell", func(c *Config) { c.StreamAddr = "db003;rm -rf /" }, "--stream-addr"},
		{"empty stream addr", func(c *Config) { c.StreamAddr = "" }, "--stream-addr"},
		{"empty stop cmd", func(c *Config) { c.StopCmd = "" }, "--stop-cmd"},
		{"empty start cmd", func(c *Config) { c.StartCmd = "" }, "--start-cmd"},
		{"empty status cmd", func(c *Config) { c.StatusCmd = "" }, "--status-cmd"},
		{"no root password", func(c *Config) { c.ReplicaMySQLPass = "" }, "MYSQL_ROOT_PASSWORD"},
		{"no repl password", func(c *Config) { c.ReplPass = "" }, "MYSQL_REPL_PASSWORD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestValidateAllowsSameHostOnDifferentPorts(t *testing.T) {
	// The Docker lab runs both hosts behind 127.0.0.1 on different SSH ports.
	c := validConfig()
	c.SourceHost, c.ReplicaHost = "127.0.0.1:2221", "127.0.0.1:2222"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateDryRunNeedsNoPasswords(t *testing.T) {
	c := validConfig()
	c.DryRun = true
	c.ReplicaMySQLPass, c.ReplPass = "", ""
	if err := c.Validate(); err != nil {
		t.Fatalf("dry run should not need passwords: %v", err)
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	c := validConfig()
	c.Datadir = "/"
	c.Compress = "gzip"
	c.Parallel = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"--datadir", "--compress", "--parallel"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}

func TestSSHAddr(t *testing.T) {
	tests := map[string]string{
		"db1":          "db1:22",
		"db1:22":       "db1:22",
		"db1:2222":     "db1:2222",
		"DB1.Example":  "db1.example:22",
		"10.0.0.3":     "10.0.0.3:22",
		"[::1]:2222":   "[::1]:2222",
		"fe80::1":      "[fe80::1]:22",
		"127.0.0.1:22": "127.0.0.1:22",
	}
	for in, want := range tests {
		if got := SSHAddr(in); got != want {
			t.Errorf("SSHAddr(%q) = %q, want %q", in, got, want)
		}
	}
}
