// Package remote runs commands on hosts over SSH, optionally via sudo.
package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Host executes commands on one machine.
type Host interface {
	Name() string
	// Run executes argv (as root unless disabled) without a shell interpreting it. stdin
	// may be nil; the remote side then sees EOF only once Run returns. stdout may be nil.
	// A non-zero exit status is reported as an *ExitError.
	Run(ctx context.Context, argv []string, stdin io.Reader, stdout io.Writer) error
	// Dial opens a TCP connection from the remote host, e.g. to 127.0.0.1:3306.
	Dial(ctx context.Context, addr string) (net.Conn, error)
	Close() error
}

// Shell runs a shell command line with sh -c and returns its stdout. It is for commands the
// user supplies as shell (such as --stop-cmd), and the few one-liners go-reseed needs before
// its agent is in place.
func Shell(ctx context.Context, h Host, cmd string) (string, error) {
	var out bytes.Buffer
	err := h.Run(ctx, []string{"sh", "-c", cmd}, nil, &out)
	return out.String(), err
}

// ExitError reports a remote command that ran and exited non-zero.
type ExitError struct {
	Host   string
	Cmd    string
	Status int
	Stderr string // the last few KB of stderr
}

func (e *ExitError) Error() string {
	msg := fmt.Sprintf("%s: %s exited with status %d", e.Host, e.Cmd, e.Status)
	if e.Stderr != "" {
		msg += "\n" + e.Stderr
	}
	return msg
}

// WaitReader returns a reader that blocks until ctx is done and then reports EOF. Used as
// stdin, it tells a remote process "go-reseed is still here" until ctx ends.
func WaitReader(ctx context.Context) io.Reader { return waitReader{ctx} }

type waitReader struct{ ctx context.Context }

func (w waitReader) Read([]byte) (int, error) {
	<-w.ctx.Done()
	return 0, io.EOF
}

// CommandLine quotes argv into one line for a POSIX shell, which is how sshd runs it.
func CommandLine(argv []string) string {
	q := make([]string, len(argv))
	for i, a := range argv {
		q[i] = Quote(a)
	}
	return strings.Join(q, " ")
}

// Options configures SSH connections.
type Options struct {
	User            string
	KeyFile         string // optional; ssh-agent is used when available
	KnownHosts      string // defaults to ~/.ssh/known_hosts
	InsecureHostKey bool
	Sudo            bool
	Log             io.Writer // receives remote stderr, prefixed with the host name
}

// SSHHost is a Host backed by a real SSH connection.
type SSHHost struct {
	name   string
	client *ssh.Client
	sudo   bool
	log    io.Writer
}

// Connect dials host ("name" or "name:port") and authenticates.
func Connect(host string, o Options) (*SSHHost, error) {
	addr := host
	if _, _, err := net.SplitHostPort(host); err != nil {
		addr = net.JoinHostPort(host, "22")
	}

	var auths []ssh.AuthMethod
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		if conn, err := net.Dial("unix", sock); err == nil {
			auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		}
	}
	if o.KeyFile != "" {
		pem, err := os.ReadFile(o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read ssh key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(pem)
		if err != nil {
			return nil, fmt.Errorf("parse ssh key %s: %w", o.KeyFile, err)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	if len(auths) == 0 {
		return nil, errors.New("no ssh auth available: start ssh-agent or pass --ssh-key")
	}

	hostKeyCB := ssh.InsecureIgnoreHostKey()
	if !o.InsecureHostKey {
		path := o.KnownHosts
		if path == "" {
			home, _ := os.UserHomeDir()
			path = filepath.Join(home, ".ssh", "known_hosts")
		}
		cb, err := knownhosts.New(path)
		if err != nil {
			return nil, fmt.Errorf("load known_hosts: %w", err)
		}
		hostKeyCB = cb
	}

	client, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{
		User:            o.User,
		Auth:            auths,
		HostKeyCallback: hostKeyCB,
	})
	if err != nil {
		return nil, fmt.Errorf("ssh %s: %w", addr, err)
	}
	return &SSHHost{name: host, client: client, sudo: o.Sudo, log: o.Log}, nil
}

func (h *SSHHost) Name() string { return h.name }

func (h *SSHHost) Close() error { return h.client.Close() }

func (h *SSHHost) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return h.client.DialContext(ctx, "tcp", addr)
}

func (h *SSHHost) Run(ctx context.Context, argv []string, stdin io.Reader, stdout io.Writer) error {
	sess, err := h.client.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Our own stdin pump, not sess.Stdin: Session.Wait waits for sess.Stdin to be drained,
	// and a WaitReader is only drained when Run returns.
	in, err := sess.StdinPipe()
	if err != nil {
		return err
	}
	if stdin == nil {
		stdin = WaitReader(runCtx)
	}
	go func() {
		_, _ = io.Copy(in, stdin)
		_ = in.Close()
	}()

	if stdout == nil {
		stdout = io.Discard
	}
	tail := &tailBuffer{max: 4096}
	sess.Stdout = stdout
	sess.Stderr = io.MultiWriter(tail, &prefixWriter{prefix: "[" + h.name + "] ", w: h.log})

	line := CommandLine(argv)
	if h.sudo {
		line = "sudo -n -H " + line
	}
	fmt.Fprintf(h.log, "[%s] $ %s\n", h.name, CommandLine(argv))

	done := make(chan error, 1)
	go func() { done <- sess.Run(line) }()
	select {
	case err = <-done:
	case <-ctx.Done():
		// Closing the session gives the remote process EOF on stdin; the agent treats that
		// as "stop", and kills xtrabackup or xbstream.
		_ = sess.Signal(ssh.SIGTERM)
		_ = sess.Close()
		return ctx.Err()
	}
	var ee *ssh.ExitError
	if errors.As(err, &ee) {
		return &ExitError{Host: h.name, Cmd: CommandLine(argv), Status: ee.ExitStatus(), Stderr: tail.String()}
	}
	if err != nil {
		return fmt.Errorf("%s: %s: %w\n%s", h.name, CommandLine(argv), err, tail.String())
	}
	return nil
}

// DryRunHost prints commands instead of running them.
type DryRunHost struct {
	HostName string
	Out      io.Writer
}

func (d *DryRunHost) Name() string { return d.HostName }
func (d *DryRunHost) Close() error { return nil }
func (d *DryRunHost) Run(_ context.Context, argv []string, _ io.Reader, _ io.Writer) error {
	if len(argv) == 3 && argv[0] == "sh" && argv[1] == "-c" {
		fmt.Fprintf(d.Out, "  [%s] $ %s\n", d.HostName, argv[2])
	} else {
		fmt.Fprintf(d.Out, "  [%s] $ %s\n", d.HostName, CommandLine(argv))
	}
	return nil
}
func (d *DryRunHost) Dial(context.Context, string) (net.Conn, error) {
	return nil, errors.New("dry run: no connection")
}

// Quote makes s safe to use as one shell word, quoting only when needed.
func Quote(s string) string {
	if s != "" && safeWord.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

var safeWord = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// tailBuffer keeps the last max bytes written, for error messages.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

// prefixWriter prefixes each line written to w.
type prefixWriter struct {
	mu      sync.Mutex
	prefix  string
	w       io.Writer
	midLine bool
}

func (p *prefixWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, line := range bytes.SplitAfter(b, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if !p.midLine {
			io.WriteString(p.w, p.prefix)
		}
		p.w.Write(line)
		p.midLine = line[len(line)-1] != '\n'
	}
	return len(b), nil
}
