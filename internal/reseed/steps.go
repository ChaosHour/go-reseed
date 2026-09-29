// Package reseed rebuilds a MySQL 8.0 replica from a streamed xtrabackup of its source.
package reseed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ChaosHour/go-reseed/internal/agent"
	"github.com/ChaosHour/go-reseed/internal/remote"
)

// Step is one named phase of a reseed.
type Step struct {
	Name        string
	Description string
	Destructive bool
	Run         func(ctx context.Context) error
}

// Reseeder runs the reseed steps against a source and a replica.
type Reseeder struct {
	cfg     Config
	source  remote.Host
	replica remote.Host
	out     io.Writer // progress messages

	srcAgent, repAgent *agentConn // set by Run
}

func New(cfg Config, source, replica remote.Host, out io.Writer) *Reseeder {
	if cfg.ProgressEvery == 0 {
		cfg.ProgressEvery = 10 * time.Second
	}
	return &Reseeder{cfg: cfg, source: source, replica: replica, out: &lockedWriter{w: out}}
}

// lockedWriter serializes writes: agents deploy, and streams report, concurrently.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return len(p), nil
	}
	return l.w.Write(p)
}

// Steps returns every step in execution order.
func (r *Reseeder) Steps() []Step {
	return []Step{
		{Name: "preflight", Description: "check tools, directories and disk space on both hosts", Run: r.preflight},
		{Name: "stop", Description: "stop mysqld on the replica", Destructive: true, Run: r.stop},
		{Name: "wipe", Description: "empty the replica's datadir (and logdir)", Destructive: true, Run: r.wipe},
		{Name: "stream", Description: "stream xtrabackup from source into the replica's datadir", Destructive: true, Run: r.stream},
		{Name: "decompress", Description: "decompress the backup on the replica", Run: r.decompress},
		{Name: "prepare", Description: "apply the redo log (xtrabackup --prepare)", Run: r.prepare},
		{Name: "move-redo", Description: "move redo logs to --logdir", Run: r.moveRedo},
		{Name: "finalize", Description: "remove auto.cnf, fix ownership and permissions", Run: r.finalize},
		{Name: "start", Description: "start mysqld on the replica", Run: r.start},
		{Name: "replication", Description: "point the replica at the source and start replication", Run: r.replication},
	}
}

// Select returns the steps from `from` through `to` inclusive; empty means first/last.
func (r *Reseeder) Select(from, to string) ([]Step, error) {
	steps := r.Steps()
	start, end := 0, len(steps)-1
	for i, s := range steps {
		if s.Name == from {
			start = i
		}
		if s.Name == to {
			end = i
		}
	}
	if from != "" && steps[start].Name != from {
		return nil, fmt.Errorf("unknown step %q", from)
	}
	if to != "" && steps[end].Name != to {
		return nil, fmt.Errorf("unknown step %q", to)
	}
	if start > end {
		return nil, fmt.Errorf("--from-step %s comes after --to-step %s", from, to)
	}
	return steps[start : end+1], nil
}

// Run deploys the agent to both hosts, executes the given steps in order, stopping at the
// first failure, and removes the agents again.
func (r *Reseeder) Run(ctx context.Context, steps []Step) error {
	fmt.Fprintln(r.out, "==> deploying the go-reseed agent")
	var g errgroup.Group
	g.Go(func() (err error) { r.srcAgent, err = r.deployAgent(ctx, r.source); return err })
	g.Go(func() (err error) { r.repAgent, err = r.deployAgent(ctx, r.replica); return err })
	defer func() {
		r.srcAgent.cleanup()
		r.repAgent.cleanup()
	}()
	if err := g.Wait(); err != nil {
		return fmt.Errorf("deploy agent: %w", err)
	}

	for i, s := range steps {
		fmt.Fprintf(r.out, "==> [%d/%d] %s: %s\n", i+1, len(steps), s.Name, s.Description)
		began := time.Now()
		if err := s.Run(ctx); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("step %s: interrupted; the agents stopped xtrabackup/xbstream and were removed\n(resume with --from-step %s)", s.Name, s.Name)
			}
			return fmt.Errorf("step %s: %w\n(fix the problem, then resume with --from-step %s)", s.Name, err, s.Name)
		}
		fmt.Fprintf(r.out, "    done in %s\n", time.Since(began).Round(time.Second))
	}
	return nil
}

func (r *Reseeder) preflight(ctx context.Context) error {
	c := r.cfg
	replicaTools := []string{"xtrabackup", "xbstream"}
	switch c.Compress {
	case "zstd":
		replicaTools = append(replicaTools, "zstd")
	case "lz4":
		replicaTools = append(replicaTools, "lz4")
	case "quicklz":
		replicaTools = append(replicaTools, "qpress")
	}

	src, err := r.srcAgent.call(ctx, "preflight", "--tools", "xtrabackup", "--dir", c.SourceDatadir)
	if err != nil {
		return err
	}
	rep, err := r.repAgent.call(ctx, "preflight", "--tools", strings.Join(replicaTools, ","), "--dir", c.Datadir)
	if err != nil {
		return err
	}
	if c.DryRun {
		return nil
	}
	var problems []string
	if len(src.Missing) > 0 {
		problems = append(problems, fmt.Sprintf("%s is missing %s", r.source.Name(), strings.Join(src.Missing, ", ")))
	}
	if len(rep.Missing) > 0 {
		problems = append(problems, fmt.Sprintf("%s is missing %s", r.replica.Name(), strings.Join(rep.Missing, ", ")))
	}
	if !src.DirExists {
		problems = append(problems, fmt.Sprintf("%s has no directory %s", r.source.Name(), c.SourceDatadir))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}

	// The replica needs room for the source's datadir; what's in its own datadir now will be wiped.
	srcSpace, err := r.srcAgent.call(ctx, "space", "--dir", c.SourceDatadir)
	if err != nil {
		return err
	}
	repSpace, err := r.repAgent.call(ctx, "space", "--dir", c.Datadir, "--create")
	if err != nil {
		return err
	}
	need, have := srcSpace.DirBytes, repSpace.AvailBytes+repSpace.DirBytes
	fmt.Fprintf(r.out, "    source datadir %s, replica space available %s\n", humanBytes(need), humanBytes(have))
	if err := spaceOK(need, have); err != nil {
		if !c.SkipSpaceCheck {
			return fmt.Errorf("%w (use --skip-space-check to override)", err)
		}
		fmt.Fprintln(r.out, "    WARNING: replica may run out of space")
	}
	return nil
}

// spaceOK wants 10% headroom over the source datadir's size.
func spaceOK(need, have int64) error {
	if have < need+need/10 {
		return fmt.Errorf("replica has %s available but the source datadir is %s", humanBytes(have), humanBytes(need))
	}
	return nil
}

func (r *Reseeder) stop(ctx context.Context) error {
	_, err := remote.Shell(ctx, r.replica, r.cfg.StopCmd)
	return err
}

func (r *Reseeder) wipe(ctx context.Context) error {
	c := r.cfg
	// --status-cmd exits 0 while mysqld runs. The agent also refuses on its own if it finds
	// a mysqld process working in the datadir.
	_, err := remote.Shell(ctx, r.replica, c.StatusCmd)
	switch {
	case err == nil && !c.DryRun:
		return fmt.Errorf("mysqld is still running on %s (--status-cmd succeeded); refusing to wipe", r.replica.Name())
	case err != nil && !isExit(err):
		return err
	}
	var args []string
	for _, dir := range r.replicaDirs() {
		args = append(args, "--dir", dir)
	}
	_, err = r.repAgent.call(ctx, "wipe", args...)
	return err
}

// stream starts the agent's receiver on the replica, which pipes into xbstream, then the
// agent's sender on the source, which pipes xtrabackup's output straight to it. Both sides
// SHA-256 the bytes they handle.
func (r *Reseeder) stream(ctx context.Context) error {
	c := r.cfg
	token, err := newToken()
	if err != nil {
		return err
	}
	port := strconv.Itoa(c.Port)
	recvArgs := append([]string{"--listen", ":" + port, "--progress", c.ProgressEvery.String(), "--"}, r.extractArgv()...)
	sendArgs := append([]string{"--to", net.JoinHostPort(c.StreamAddr, port), "--progress", "0s", "--"}, r.backupArgv()...)
	if c.DryRun {
		ignore := func(agent.Event) {}
		if err := r.repAgent.stream(ctx, "receive", recvArgs, token, ignore); err != nil {
			return err
		}
		return r.srcAgent.stream(ctx, "send", sendArgs, token, ignore)
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(sctx)

	listening := make(chan struct{})
	var once sync.Once
	var recv, sent agent.Event
	progress := newProgressPrinter(r.out)

	g.Go(func() error {
		return r.repAgent.stream(gctx, "receive", recvArgs, token, func(e agent.Event) {
			switch e.Event {
			case "listening":
				once.Do(func() { close(listening) })
			case "progress":
				progress.print(e.Bytes)
			case "done":
				recv = e
			}
		})
	})
	select {
	case <-listening:
	case <-gctx.Done():
		return g.Wait()
	case <-time.After(30 * time.Second):
		cancel()
		_ = g.Wait()
		return fmt.Errorf("receiver on %s did not start listening within 30s", r.replica.Name())
	}
	g.Go(func() error {
		return r.srcAgent.stream(gctx, "send", sendArgs, token, func(e agent.Event) {
			if e.Event == "done" {
				sent = e
			}
		})
	})
	if err := g.Wait(); err != nil {
		return err
	}
	if err := verifyTransfer(sent, recv); err != nil {
		return err
	}
	fmt.Fprintf(r.out, "    %s streamed; sha256 %s matches on both hosts\n", humanBytes(recv.Bytes), recv.SHA256)

	_, err = r.repAgent.call(ctx, "check-backup", "--dir", c.Datadir)
	return err
}

// verifyTransfer checks that the replica received exactly what the source sent.
func verifyTransfer(sent, recv agent.Event) error {
	switch {
	case sent.SHA256 == "" || recv.SHA256 == "":
		return errors.New("transfer did not report a checksum from both hosts")
	case sent.Bytes == 0:
		return errors.New("the source sent no data")
	case sent.Bytes != recv.Bytes:
		return fmt.Errorf("byte count mismatch: source sent %d, replica received %d", sent.Bytes, recv.Bytes)
	case sent.SHA256 != recv.SHA256:
		return fmt.Errorf("checksum mismatch: source %s, replica %s", sent.SHA256, recv.SHA256)
	}
	return nil
}

// backupArgv is the xtrabackup command the agent runs on the source.
func (r *Reseeder) backupArgv() []string {
	c := r.cfg
	argv := []string{"xtrabackup", "--backup", "--stream=xbstream", "--parallel=" + strconv.Itoa(c.Parallel), "--target-dir={tmpdir}"}
	if c.Compress != "none" {
		argv = append(argv, "--compress="+c.Compress, "--compress-threads="+strconv.Itoa(c.Parallel))
	}
	return append(argv, strings.Fields(c.BackupArgs)...)
}

// extractArgv is the xbstream command the agent feeds on the replica.
func (r *Reseeder) extractArgv() []string {
	return []string{"xbstream", "-x", "--parallel=" + strconv.Itoa(r.cfg.Parallel), "-C", r.cfg.Datadir}
}

func (r *Reseeder) decompress(ctx context.Context) error {
	if r.cfg.Compress == "none" {
		fmt.Fprintln(r.out, "    skipped: backup was not compressed")
		return nil
	}
	return r.replica.Run(ctx, []string{"xtrabackup", "--decompress", "--remove-original",
		"--parallel=" + strconv.Itoa(r.cfg.Parallel), "--target-dir=" + r.cfg.Datadir}, nil, nil)
}

func (r *Reseeder) prepare(ctx context.Context) error {
	return r.replica.Run(ctx, []string{"xtrabackup", "--prepare",
		"--use-memory=" + r.cfg.UseMemory, "--target-dir=" + r.cfg.Datadir}, nil, nil)
}

func (r *Reseeder) moveRedo(ctx context.Context) error {
	c := r.cfg
	if c.Logdir == "" || c.Logdir == c.Datadir {
		fmt.Fprintln(r.out, "    skipped: no separate --logdir")
		return nil
	}
	ev, err := r.repAgent.call(ctx, "move-redo", "--datadir", c.Datadir, "--logdir", c.Logdir)
	if err == nil && !c.DryRun {
		fmt.Fprintf(r.out, "    moved to %s: %s\n", c.Logdir, orNone(ev.Moved))
	}
	return err
}

func (r *Reseeder) finalize(ctx context.Context) error {
	c := r.cfg
	args := []string{"--datadir", c.Datadir, "--owner", c.MySQLOSUser}
	if c.Logdir != "" {
		args = append(args, "--logdir", c.Logdir)
	}
	ev, err := r.repAgent.call(ctx, "finalize", args...)
	if err == nil && ev.Note != "" {
		fmt.Fprintf(r.out, "    %s\n", ev.Note)
	}
	return err
}

func (r *Reseeder) start(ctx context.Context) error {
	_, err := remote.Shell(ctx, r.replica, r.cfg.StartCmd)
	return err
}

func (r *Reseeder) replicaDirs() []string {
	dirs := []string{r.cfg.Datadir}
	if r.cfg.Logdir != "" && r.cfg.Logdir != r.cfg.Datadir {
		dirs = append(dirs, r.cfg.Logdir)
	}
	return dirs
}

// progressPrinter prints transfer progress with the rate since the previous line.
type progressPrinter struct {
	out   io.Writer
	last  int64
	lastT time.Time
}

func newProgressPrinter(out io.Writer) *progressPrinter {
	return &progressPrinter{out: out, lastT: time.Now()}
}

func (p *progressPrinter) print(bytes int64) {
	now := time.Now()
	rate := float64(bytes-p.last) / now.Sub(p.lastT).Seconds()
	fmt.Fprintf(p.out, "    %s received (%s/s)\n", humanBytes(bytes), humanBytes(int64(rate)))
	p.last, p.lastT = bytes, now
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "nothing to move"
	}
	return strings.Join(s, ", ")
}

func humanBytes(n int64) string {
	const unit = 1024
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	f := float64(n)
	i := 0
	for f >= unit && i < len(units)-1 {
		f /= unit
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
}
