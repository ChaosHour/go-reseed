package reseed

import (
	"errors"
	"fmt"
	"net"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ChaosHour/go-reseed/internal/agent"
)

// Config holds everything a reseed needs. Passwords come from the environment.
type Config struct {
	SourceHost  string // SSH host of the source (master)
	ReplicaHost string // SSH host of the replica being rebuilt

	SourceDatadir string // datadir on the source to back up
	Datadir       string // datadir on the replica
	Logdir        string // optional innodb_log_group_home_dir on the replica

	StreamAddr  string // address the source uses to reach the replica's receiver
	Port        int
	Parallel    int
	Compress    string // zstd, lz4, quicklz or none
	UseMemory   string
	BackupArgs  string // extra args for xtrabackup --backup on the source
	StopCmd     string // stops mysqld on the replica
	StartCmd    string // starts mysqld on the replica
	StatusCmd   string // exits 0 while mysqld is running on the replica
	MySQLOSUser string // OS user that owns the datadir

	ReplicaMySQLAddr string // mysqld address as seen from the replica host
	ReplicaMySQLUser string
	ReplicaMySQLPass string
	SourceMySQLHost  string // host the replica replicates from
	SourceMySQLPort  int
	ReplUser         string
	ReplPass         string

	AgentDir      string        // where the agent binary is uploaded on each host
	AgentBinary   string        // local agent binary to upload; overrides AgentBinDir
	AgentBinDir   string        // local directory holding go-reseed-linux-<arch> binaries
	ProgressEvery time.Duration // interval between transfer progress lines; 0 means 10s

	SkipSpaceCheck bool
	DryRun         bool
}

// Validate checks for settings that could make a run destructive in the wrong place.
func (c *Config) Validate() error {
	var errs []error
	if c.SourceHost == "" || c.ReplicaHost == "" {
		errs = append(errs, errors.New("--source and --replica are required"))
	}
	if c.SourceHost != "" && SSHAddr(c.SourceHost) == SSHAddr(c.ReplicaHost) {
		errs = append(errs, fmt.Errorf("--source and --replica are the same host (%s)", SSHAddr(c.SourceHost)))
	}
	for _, d := range []struct{ flag, dir string }{
		{"--datadir", c.Datadir}, {"--source-datadir", c.SourceDatadir}, {"--logdir", c.Logdir},
	} {
		if d.dir == "" && d.flag == "--logdir" {
			continue
		}
		if err := agent.CheckWipeDir(d.dir); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", d.flag, err))
		}
	}
	if c.Logdir != "" && isWithin(c.Logdir, c.Datadir) && path.Clean(c.Logdir) != path.Clean(c.Datadir) {
		errs = append(errs, fmt.Errorf("--logdir %s is inside --datadir %s; the wipe would empty it twice and move-redo would move it into itself", c.Logdir, c.Datadir))
	}
	if !hostnameRE.MatchString(c.StreamAddr) {
		errs = append(errs, fmt.Errorf("--stream-addr %q must be a host name or IP address", c.StreamAddr))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("--port %d is out of range", c.Port))
	}
	for _, cmd := range []struct{ flag, val string }{
		{"--stop-cmd", c.StopCmd}, {"--start-cmd", c.StartCmd}, {"--status-cmd", c.StatusCmd},
	} {
		if strings.TrimSpace(cmd.val) == "" {
			errs = append(errs, fmt.Errorf("%s must not be empty", cmd.flag))
		}
	}
	switch c.Compress {
	case "zstd", "lz4", "quicklz", "none":
	default:
		errs = append(errs, fmt.Errorf("--compress must be zstd, lz4, quicklz or none, got %q", c.Compress))
	}
	if !path.IsAbs(c.AgentDir) {
		errs = append(errs, fmt.Errorf("--agent-dir %q must be an absolute path", c.AgentDir))
	}
	if c.Parallel < 1 {
		errs = append(errs, errors.New("--parallel must be at least 1"))
	}
	if !c.DryRun {
		if c.ReplicaMySQLPass == "" {
			errs = append(errs, errors.New("MYSQL_ROOT_PASSWORD is not set"))
		}
		if c.ReplPass == "" {
			errs = append(errs, errors.New("MYSQL_REPL_PASSWORD is not set"))
		}
	}
	return errors.Join(errs...)
}

var hostnameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._:-]*[A-Za-z0-9])?$`)

// SSHAddr normalizes an SSH host to host:port, defaulting the port to 22.
func SSHAddr(host string) string {
	if _, _, err := net.SplitHostPort(host); err == nil {
		return strings.ToLower(host)
	}
	return net.JoinHostPort(strings.ToLower(host), "22")
}

func isWithin(dir, parent string) bool {
	dir, parent = path.Clean(dir), path.Clean(parent)
	return dir == parent || strings.HasPrefix(dir, parent+"/")
}
