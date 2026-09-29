// Command go-reseed rebuilds a MySQL 8.0 replica from a streamed xtrabackup of its source.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ChaosHour/go-reseed/internal/agent"
	"github.com/ChaosHour/go-reseed/internal/remote"
	"github.com/ChaosHour/go-reseed/internal/reseed"
)

// version is set at build time: -ldflags "-X main.version=...".
var version = "dev"

func main() {
	// On the database hosts go-reseed runs as its own agent: `go-reseed agent <op>`.
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		code := agent.Main(ctx, version, os.Args[2:], os.Stdin, os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfg                         reseed.Config
		sshUser, sshKey, knownHosts string
		insecureHostKey, noSudo     bool
		fromStep, toStep, logFile   string
		listSteps, yes, verbose     bool
		showVersion                 bool
		service                     string
	)
	flag.StringVar(&cfg.SourceHost, "source", "", "SSH host of the source to back up (host or host:port)")
	flag.StringVar(&cfg.ReplicaHost, "replica", "", "SSH host of the replica to rebuild (host or host:port)")
	flag.StringVar(&cfg.Datadir, "datadir", "/var/lib/mysql", "replica datadir (emptied by the wipe step)")
	flag.StringVar(&cfg.SourceDatadir, "source-datadir", "", "source datadir (default: same as --datadir)")
	flag.StringVar(&cfg.Logdir, "logdir", "", "replica innodb_log_group_home_dir, if separate from the datadir (also emptied)")
	flag.StringVar(&cfg.StreamAddr, "stream-addr", "", "address the source uses to reach the replica's receiver (default: --replica host)")
	flag.IntVar(&cfg.Port, "port", 4000, "TCP port for the backup stream (open it from source to replica)")
	flag.IntVar(&cfg.Parallel, "parallel", 4, "xtrabackup copy, compress and decompress threads")
	flag.StringVar(&cfg.Compress, "compress", "zstd", "stream compression: zstd, lz4, quicklz or none")
	flag.StringVar(&cfg.UseMemory, "use-memory", "2G", "memory for xtrabackup --prepare")
	flag.StringVar(&cfg.BackupArgs, "backup-args", "", "extra arguments for xtrabackup --backup, e.g. --defaults-extra-file=/root/.xtrabackup.cnf")
	flag.StringVar(&service, "service", "mysqld", "systemd unit for mysqld on the replica (often 'mysql' on Debian/Ubuntu)")
	flag.StringVar(&cfg.StopCmd, "stop-cmd", "", "command that stops mysqld on the replica (default: systemctl stop <service>)")
	flag.StringVar(&cfg.StartCmd, "start-cmd", "", "command that starts mysqld on the replica (default: systemctl start <service>)")
	flag.StringVar(&cfg.StatusCmd, "status-cmd", "", "command that exits 0 while mysqld runs on the replica (default: systemctl is-active --quiet <service>)")
	flag.StringVar(&cfg.MySQLOSUser, "mysql-os-user", "mysql", "OS user and group that owns the replica's datadir")
	flag.StringVar(&cfg.ReplicaMySQLAddr, "replica-mysql-addr", "127.0.0.1:3306", "replica mysqld address, as seen from the replica host")
	flag.StringVar(&cfg.ReplicaMySQLUser, "replica-mysql-user", "root", "admin user on the replica (password from MYSQL_ROOT_PASSWORD)")
	flag.StringVar(&cfg.SourceMySQLHost, "source-mysql-host", "", "host the replica replicates from (default: --source host)")
	flag.IntVar(&cfg.SourceMySQLPort, "source-mysql-port", 3306, "port the replica replicates from")
	flag.StringVar(&cfg.ReplUser, "repl-user", "repl", "replication user (password from MYSQL_REPL_PASSWORD)")
	flag.StringVar(&cfg.AgentDir, "agent-dir", "/tmp", "directory on each host the agent is uploaded to (must allow exec)")
	flag.StringVar(&cfg.AgentBinary, "agent-binary", "", "local linux binary of go-reseed to upload as the agent (default: go-reseed-linux-<arch> next to this binary)")
	flag.BoolVar(&cfg.SkipSpaceCheck, "skip-space-check", false, "continue even if the replica looks short on disk space")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "print what would run without connecting to anything")

	flag.StringVar(&sshUser, "ssh-user", os.Getenv("USER"), "SSH user")
	flag.StringVar(&sshKey, "ssh-key", "", "SSH private key file (ssh-agent is used if running)")
	flag.StringVar(&knownHosts, "known-hosts", "", "known_hosts file (default ~/.ssh/known_hosts)")
	flag.BoolVar(&insecureHostKey, "insecure-ignore-host-key", false, "skip SSH host key verification")
	flag.BoolVar(&noSudo, "no-sudo", false, "don't wrap remote commands in sudo (implied when --ssh-user is root)")

	flag.StringVar(&fromStep, "from-step", "", "resume from this step")
	flag.StringVar(&toStep, "to-step", "", "stop after this step")
	flag.BoolVar(&listSteps, "list-steps", false, "list the steps and exit")
	flag.BoolVar(&yes, "yes", false, "don't ask for confirmation before destructive steps")
	flag.StringVar(&logFile, "log-file", "", "file for remote command output (default go-reseed-<time>.log)")
	flag.BoolVar(&verbose, "verbose", false, "also print remote command output to the terminal")
	flag.BoolVar(&showVersion, "version", false, "print the version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println("go-reseed", version)
		return nil
	}

	if cfg.StopCmd == "" {
		cfg.StopCmd = "systemctl stop " + remote.Quote(service)
	}
	if cfg.StartCmd == "" {
		cfg.StartCmd = "systemctl start " + remote.Quote(service)
	}
	if cfg.StatusCmd == "" {
		cfg.StatusCmd = "systemctl is-active --quiet " + remote.Quote(service)
	}
	if exe, err := os.Executable(); err == nil {
		cfg.AgentBinDir = filepath.Dir(exe)
	}
	if cfg.SourceDatadir == "" {
		cfg.SourceDatadir = cfg.Datadir
	}
	if cfg.StreamAddr == "" {
		cfg.StreamAddr = hostOnly(cfg.ReplicaHost)
	}
	if cfg.SourceMySQLHost == "" {
		cfg.SourceMySQLHost = hostOnly(cfg.SourceHost)
	}
	cfg.ReplicaMySQLPass = os.Getenv("MYSQL_ROOT_PASSWORD")
	cfg.ReplPass = os.Getenv("MYSQL_REPL_PASSWORD")

	if listSteps {
		for _, s := range reseed.New(cfg, nil, nil, os.Stdout).Steps() {
			fmt.Printf("%-12s %s\n", s.Name, s.Description)
		}
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var source, replica remote.Host
	if cfg.DryRun {
		fmt.Println("DRY RUN: nothing will be executed")
		source = &remote.DryRunHost{HostName: cfg.SourceHost, Out: os.Stdout}
		replica = &remote.DryRunHost{HostName: cfg.ReplicaHost, Out: os.Stdout}
	} else {
		if logFile == "" {
			logFile = "go-reseed-" + time.Now().Format("20060102-150405") + ".log"
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		var logw io.Writer = f
		if verbose {
			logw = io.MultiWriter(f, os.Stderr)
		}
		fmt.Printf("remote command output is logged to %s\n", logFile)

		opts := remote.Options{
			User: sshUser, KeyFile: sshKey, KnownHosts: knownHosts,
			InsecureHostKey: insecureHostKey, Sudo: !noSudo && sshUser != "root", Log: logw,
		}
		s, err := remote.Connect(cfg.SourceHost, opts)
		if err != nil {
			return err
		}
		defer s.Close()
		r, err := remote.Connect(cfg.ReplicaHost, opts)
		if err != nil {
			return err
		}
		defer r.Close()
		source, replica = s, r
	}

	rs := reseed.New(cfg, source, replica, os.Stdout)
	steps, err := rs.Select(fromStep, toStep)
	if err != nil {
		return err
	}
	if !cfg.DryRun && !yes {
		if err := confirm(cfg, steps); err != nil {
			return err
		}
	}
	return rs.Run(ctx, steps)
}

// confirm makes the operator type the replica's name before anything destructive runs.
func confirm(cfg reseed.Config, steps []reseed.Step) error {
	var destructive []string
	for _, s := range steps {
		if s.Destructive {
			destructive = append(destructive, s.Name)
		}
	}
	if len(destructive) == 0 {
		return nil
	}
	fmt.Printf("\nThis will run %s on %s, DESTROYING everything in %s", strings.Join(destructive, ", "), cfg.ReplicaHost, cfg.Datadir)
	if cfg.Logdir != "" {
		fmt.Printf(" and %s", cfg.Logdir)
	}
	fmt.Printf(",\nand rebuild it from %s:%s.\n\nType the replica host name to continue: ", cfg.SourceHost, cfg.SourceDatadir)
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	if strings.TrimSpace(line) != cfg.ReplicaHost {
		return fmt.Errorf("confirmation did not match %q; aborting", cfg.ReplicaHost)
	}
	return nil
}

// hostOnly strips the port from "host:port" or "[v6]:port"; a bare host is returned as is.
func hostOnly(hostport string) string {
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return host
	}
	return hostport
}
