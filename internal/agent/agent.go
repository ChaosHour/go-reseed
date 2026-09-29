// Package agent is the part of go-reseed that runs on the database hosts.
//
// go-reseed uploads its own binary to each host and runs `go-reseed agent <op> [flags]` there
// (through sudo). Each op does its work with the Go standard library and reports results as
// JSON lines on stdout; diagnostics go to stderr. The only external programs an op starts are
// the ones Go can't replace: xtrabackup, xbstream and, if installed, restorecon.
package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Protocol is bumped whenever ops or events change incompatibly. The orchestrator refuses
// an agent that reports a different protocol.
const Protocol = 1

// Event is one JSON line an op writes to stdout. Which fields are set depends on the op.
type Event struct {
	Event      string   `json:"event"` // "result", "listening", "progress" or "done"
	Version    string   `json:"version,omitempty"`
	Protocol   int      `json:"protocol,omitempty"`
	SHA256     string   `json:"sha256,omitempty"`
	Bytes      int64    `json:"bytes,omitempty"`
	Addr       string   `json:"addr,omitempty"`
	Missing    []string `json:"missing,omitempty"`
	DirExists  bool     `json:"dir_exists,omitempty"`
	DirBytes   int64    `json:"dir_bytes,omitempty"`
	AvailBytes int64    `json:"avail_bytes,omitempty"`
	Content    string   `json:"content,omitempty"`
	Moved      []string `json:"moved,omitempty"`
	Note       string   `json:"note,omitempty"`
}

// Emitter writes events as JSON lines. It is safe for concurrent use.
type Emitter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewEmitter(w io.Writer) *Emitter { return &Emitter{enc: json.NewEncoder(w)} }

func (e *Emitter) Emit(ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.enc.Encode(ev)
}

// Main runs one agent op and returns the process exit code.
func Main(ctx context.Context, version string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: go-reseed agent <op> [flags]  (ops: "+strings.Join(opNames(), ", ")+")")
		return 2
	}
	name, args := args[0], args[1:]
	o, ok := ops[name]
	if !ok {
		fmt.Fprintf(stderr, "unknown agent op %q\n", name)
		return 2
	}
	fs := flag.NewFlagSet("agent "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	run := o(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	env := &env{ctx: ctx, version: version, stdin: stdin, stderr: stderr, em: NewEmitter(stdout), args: fs.Args()}
	if err := run(env); err != nil {
		fmt.Fprintf(stderr, "agent %s: %v\n", name, err)
		return 1
	}
	return 0
}

type env struct {
	ctx     context.Context
	version string
	stdin   io.Reader
	stderr  io.Writer
	em      *Emitter
	args    []string // positional arguments after the flags
}

// An op registers its flags and returns the function that runs it.
type op func(fs *flag.FlagSet) func(*env) error

var ops = map[string]op{
	"version": func(fs *flag.FlagSet) func(*env) error {
		return func(e *env) error {
			exe, err := os.Executable()
			if err != nil {
				return err
			}
			sum, err := FileSHA256(exe)
			if err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result", Version: e.version, Protocol: Protocol, SHA256: sum})
			return nil
		}
	},
	"preflight": func(fs *flag.FlagSet) func(*env) error {
		tools := fs.String("tools", "", "comma-separated commands that must be on PATH")
		dir := fs.String("dir", "", "directory to report on")
		return func(e *env) error {
			missing := MissingTools(splitList(*tools))
			exists := false
			if st, err := os.Stat(*dir); err == nil && st.IsDir() {
				exists = true
			}
			e.em.Emit(Event{Event: "result", Missing: missing, DirExists: exists})
			return nil
		}
	},
	"space": func(fs *flag.FlagSet) func(*env) error {
		dir := fs.String("dir", "", "directory to measure")
		create := fs.Bool("create", false, "create the directory if it's missing")
		return func(e *env) error {
			if *create {
				if err := os.MkdirAll(*dir, 0o750); err != nil {
					return err
				}
			}
			used, err := DirSize(*dir)
			if err != nil {
				return err
			}
			avail, err := Avail(*dir)
			if err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result", DirBytes: used, AvailBytes: avail})
			return nil
		}
	},
	"wipe": func(fs *flag.FlagSet) func(*env) error {
		var dirs stringList
		fs.Var(&dirs, "dir", "directory to empty (repeatable)")
		return func(e *env) error {
			if err := Wipe(dirs, "/proc"); err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result"})
			return nil
		}
	},
	"move-redo": func(fs *flag.FlagSet) func(*env) error {
		datadir := fs.String("datadir", "", "datadir holding the redo logs")
		logdir := fs.String("logdir", "", "destination innodb_log_group_home_dir")
		return func(e *env) error {
			moved, err := MoveRedo(*datadir, *logdir)
			if err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result", Moved: moved})
			return nil
		}
	},
	"finalize": func(fs *flag.FlagSet) func(*env) error {
		datadir := fs.String("datadir", "", "restored datadir")
		logdir := fs.String("logdir", "", "separate redo log directory, if any")
		owner := fs.String("owner", "mysql", "user (and group) that must own the files")
		return func(e *env) error {
			note, err := Finalize(e.ctx, *datadir, *logdir, *owner)
			if err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result", Note: note})
			return nil
		}
	},
	"check-backup": func(fs *flag.FlagSet) func(*env) error {
		dir := fs.String("dir", "", "directory the backup was extracted into")
		return func(e *env) error {
			if err := CheckBackup(*dir); err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result"})
			return nil
		}
	},
	"binlog-info": func(fs *flag.FlagSet) func(*env) error {
		dir := fs.String("dir", "", "restored datadir")
		return func(e *env) error {
			b, err := os.ReadFile(filepath.Join(*dir, "xtrabackup_binlog_info"))
			if err != nil {
				return err
			}
			e.em.Emit(Event{Event: "result", Content: string(b)})
			return nil
		}
	},
	"receive": func(fs *flag.FlagSet) func(*env) error {
		listen := fs.String("listen", ":4000", "address to listen on")
		acceptTimeout := fs.Duration("accept-timeout", 2*time.Minute, "how long to wait for the sender")
		progress := fs.Duration("progress", 10*time.Second, "interval between progress events")
		return func(e *env) error {
			token, ctx, cancel, err := readToken(e.ctx, e.stdin)
			if err != nil {
				return err
			}
			defer cancel()
			if len(e.args) == 0 {
				return errors.New("receive needs the extract command after --")
			}
			return Receive(ctx, ReceiveOptions{Listen: *listen, Token: token, AcceptTimeout: *acceptTimeout,
				Progress: *progress, Extract: e.args}, e.em, e.stderr)
		}
	},
	"send": func(fs *flag.FlagSet) func(*env) error {
		to := fs.String("to", "", "receiver address, host:port")
		dialTimeout := fs.Duration("dial-timeout", 30*time.Second, "how long to keep trying to connect")
		progress := fs.Duration("progress", 10*time.Second, "interval between progress events")
		return func(e *env) error {
			token, ctx, cancel, err := readToken(e.ctx, e.stdin)
			if err != nil {
				return err
			}
			defer cancel()
			if len(e.args) == 0 {
				return errors.New("send needs the backup command after --")
			}
			return Send(ctx, SendOptions{To: *to, Token: token, DialTimeout: *dialTimeout,
				Progress: *progress, Backup: e.args}, e.em, e.stderr)
		}
	},
}

func opNames() []string {
	names := make([]string, 0, len(ops))
	for n := range ops {
		names = append(names, n)
	}
	return names
}

// readToken reads the connection token from the first line of stdin. The rest of stdin is
// then watched: it reaches EOF when go-reseed's SSH session ends, and the returned context
// is cancelled so the op stops its child processes instead of running on unattended.
func readToken(ctx context.Context, stdin io.Reader) (string, context.Context, context.CancelFunc, error) {
	br := bufio.NewReader(stdin)
	line, err := br.ReadString('\n')
	token := strings.TrimSpace(line)
	if err != nil || len(token) < 32 {
		return "", nil, nil, errors.New("expected a token of at least 32 characters on the first line of stdin")
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		_, _ = io.Copy(io.Discard, br)
		cancel()
	}()
	return token, ctx, cancel, nil
}

// FileSHA256 returns the hex SHA-256 of a file.
func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }
