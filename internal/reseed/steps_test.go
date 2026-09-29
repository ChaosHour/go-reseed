package reseed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChaosHour/go-reseed/internal/agent"
	"github.com/ChaosHour/go-reseed/internal/remote"
)

// The test binary plays three roles, chosen by how it's invoked:
//
//   - `<copy> agent <op> ...`: the real go-reseed agent. Tests upload the test binary as the
//     agent, so the full deploy → verify → run → clean up path is exercised.
//   - `xtrabackup ...`, `xbstream ...`, `zstd`: fakes, via symlinks on PATH. They append
//     their argv to $FAKE_LOG so tests can check what ran, in what order.
func TestMain(m *testing.M) {
	switch filepath.Base(os.Args[0]) {
	case "xtrabackup":
		os.Exit(fakeXtrabackup(os.Args[1:]))
	case "xbstream":
		os.Exit(fakeXbstream(os.Args[1:]))
	case "zstd":
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		os.Exit(agent.Main(context.Background(), "test", os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

func logInvocation(name string, args []string) {
	if p := os.Getenv("FAKE_LOG"); p != "" {
		f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			fmt.Fprintln(f, name+" "+strings.Join(args, " "))
			f.Close()
		}
	}
}

// fakePayload stands in for an xbstream archive.
func fakePayload() []byte {
	b := make([]byte, 300<<10)
	for i := range b {
		b[i] = byte(i*31 + i/7)
	}
	return b
}

func flagValue(args []string, name string) string {
	for i, a := range args {
		if v, ok := strings.CutPrefix(a, name+"="); ok {
			return v
		}
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func fakeXtrabackup(args []string) int {
	logInvocation("xtrabackup", args)
	target := flagValue(args, "--target-dir")
	switch {
	case slices.Contains(args, "--backup"):
		if st, err := os.Stat(target); err != nil || !st.IsDir() {
			fmt.Fprintln(os.Stderr, "fake xtrabackup: --target-dir must be an existing directory")
			return 3
		}
		if pids := os.Getenv("FAKE_PIDS"); pids != "" {
			os.WriteFile(filepath.Join(pids, "xtrabackup"), []byte(strconv.Itoa(os.Getpid())), 0o644)
		}
		p := fakePayload()
		if os.Getenv("FAKE_BACKUP_FAIL") == "1" {
			os.Stdout.Write(p[:len(p)/2])
			fmt.Fprintln(os.Stderr, "fake xtrabackup: simulated failure")
			return 1
		}
		if os.Getenv("FAKE_BACKUP_SLOW") == "1" {
			for {
				if _, err := os.Stdout.Write(p[:1024]); err != nil {
					return 1
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		os.Stdout.Write(p)
	case slices.Contains(args, "--decompress"):
		os.WriteFile(filepath.Join(target, ".decompressed"), nil, 0o644)
	case slices.Contains(args, "--prepare"):
		os.WriteFile(filepath.Join(target, ".prepared"), nil, 0o644)
		os.WriteFile(filepath.Join(target, "ib_logfile0"), []byte("redo"), 0o644)
		os.MkdirAll(filepath.Join(target, "#innodb_redo"), 0o755)
		os.WriteFile(filepath.Join(target, "#innodb_redo", "#ib_redo0"), []byte("redo"), 0o644)
	}
	return 0
}

func fakeXbstream(args []string) int {
	logInvocation("xbstream", args)
	dir := flagValue(args, "-C")
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		return 1
	}
	files := map[string]string{
		"backup.xbs":             string(b),
		"auto.cnf":               "[auto]\nserver-uuid=source-uuid\n",
		"xtrabackup_binlog_info": "binlog.000003\t197\tuuid:1-10\n",
		"db1/t.ibd.zst":          "compressed",
	}
	if os.Getenv("FAKE_NO_CHECKPOINTS") != "1" {
		files["xtrabackup_checkpoints"] = "backup_type = full-backuped\nfrom_lsn = 0\n"
	}
	for name, content := range files {
		p := filepath.Join(dir, name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			return 1
		}
	}
	return 0
}

// localHost runs argv on this machine, the way SSHHost runs it on a remote one.
type localHost struct {
	name string
	// mangleUpload, if set, rewrites the agent upload stream (to test integrity checks).
	mangleUpload func(io.Reader) io.Reader
}

func (h *localHost) Name() string { return h.name }
func (h *localHost) Close() error { return nil }
func (h *localHost) Dial(context.Context, string) (net.Conn, error) {
	return nil, errors.New("localHost: no MySQL here")
}
func (h *localHost) Run(ctx context.Context, argv []string, stdin io.Reader, stdout io.Writer) error {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if stdin != nil {
		if h.mangleUpload != nil && len(argv) > 2 && argv[2] == uploadScript {
			stdin = h.mangleUpload(stdin)
		}
		w, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		go func() {
			_, _ = io.Copy(w, stdin)
			_ = w.Close()
		}()
	}
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) && ctx.Err() == nil {
		return &remote.ExitError{Host: h.name, Cmd: remote.CommandLine(argv), Status: ee.ExitCode(), Stderr: stderr.String()}
	}
	if err != nil {
		return fmt.Errorf("%s: %w: %s", h.name, err, stderr.String())
	}
	return nil
}

// lab is a source and a replica on the local machine, with fake xtrabackup/xbstream.
type lab struct {
	cfg        Config
	root       string
	srcData    string
	repData    string
	repLogs    string
	invocation string // FAKE_LOG
	pids       string // FAKE_PIDS
	fakeBin    string
	source     *localHost
	replica    *localHost
}

func newLab(t *testing.T) *lab {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	me, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	root := t.TempDir()
	l := &lab{
		root:       root,
		srcData:    filepath.Join(root, "source", "data"),
		repData:    filepath.Join(root, "replica", "data"),
		repLogs:    filepath.Join(root, "replica", "logs"),
		invocation: filepath.Join(root, "invocations.log"),
		pids:       filepath.Join(root, "pids"),
		fakeBin:    filepath.Join(root, "fakebin"),
		source:     &localHost{name: "src.local"},
		replica:    &localHost{name: "rep.local"},
	}
	for _, d := range []string{l.pids, l.fakeBin, filepath.Join(root, "agents")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"xtrabackup", "xbstream", "zstd"} {
		if err := os.Symlink(exe, filepath.Join(l.fakeBin, name)); err != nil {
			t.Fatal(err)
		}
	}
	// Only the fakes and base system tools: a developer machine may have the real
	// xtrabackup/xbstream installed, and tests must never pick those up.
	t.Setenv("PATH", strings.Join([]string{l.fakeBin, "/usr/bin", "/bin", "/usr/sbin", "/sbin"}, string(os.PathListSeparator)))
	t.Setenv("FAKE_LOG", l.invocation)
	t.Setenv("FAKE_PIDS", l.pids)

	writeFile(t, filepath.Join(l.srcData, "ibdata1"), "source data")
	writeFile(t, filepath.Join(l.srcData, "db1", "t.ibd"), "source table")
	writeFile(t, filepath.Join(l.repData, "stale.ibd"), "old replica data")
	writeFile(t, filepath.Join(l.repData, "auto.cnf"), "old uuid")
	writeFile(t, filepath.Join(l.repLogs, "ib_logfile0"), "old redo")

	l.cfg = validConfig()
	l.cfg.SourceHost, l.cfg.ReplicaHost = "src.local", "rep.local"
	l.cfg.SourceDatadir, l.cfg.Datadir, l.cfg.Logdir = l.srcData, l.repData, l.repLogs
	l.cfg.StreamAddr = "127.0.0.1"
	l.cfg.Port = freePort(t)
	l.cfg.StopCmd, l.cfg.StartCmd, l.cfg.StatusCmd = "true", "true", "false"
	l.cfg.MySQLOSUser = me.Username
	l.cfg.AgentBinary = exe
	l.cfg.AgentDir = filepath.Join(root, "agents") // both local "hosts" share it; mktemp keeps uploads apart
	l.cfg.ProgressEvery = 20 * time.Millisecond
	return l
}

// run reseeds the local "replica" from the local "source", returning the progress output.
func (l *lab) run(t *testing.T, ctx context.Context, from, to string) (string, error) {
	t.Helper()
	var out syncBuffer
	r := New(l.cfg, l.source, l.replica, &out)
	steps, err := r.Select(from, to)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Run(ctx, steps)
	return out.String(), err
}

func (l *lab) invocations() string {
	b, _ := os.ReadFile(l.invocation)
	return string(b)
}

func (l *lab) agentFiles() []string {
	var left []string
	entries, _ := os.ReadDir(filepath.Join(l.root, "agents"))
	for _, e := range entries {
		left = append(left, e.Name())
	}
	return left
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func writeFile(t *testing.T, p, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool { _, err := os.Lstat(p); return err == nil }

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func mode(t *testing.T, p string) os.FileMode {
	t.Helper()
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return st.Mode().Perm()
}

// --- end to end, locally -----------------------------------------------------------------

func TestLocalReseedEndToEnd(t *testing.T) {
	l := newLab(t)
	out, err := l.run(t, context.Background(), "", "start") // replication needs a real MySQL
	if err != nil {
		t.Fatalf("%v\n--- output ---\n%s", err, out)
	}

	// The replica holds exactly what the source streamed.
	got, _ := os.ReadFile(filepath.Join(l.repData, "backup.xbs"))
	if !bytes.Equal(got, fakePayload()) {
		t.Errorf("replica received %d bytes, want the %d-byte stream", len(got), len(fakePayload()))
	}
	if !strings.Contains(out, "matches on both hosts") {
		t.Errorf("no checksum confirmation in output:\n%s", out)
	}

	// Old replica data is gone; the source is untouched.
	if exists(filepath.Join(l.repData, "stale.ibd")) {
		t.Error("wipe left old replica data behind")
	}
	if b, _ := os.ReadFile(filepath.Join(l.srcData, "db1", "t.ibd")); string(b) != "source table" {
		t.Error("the source datadir was modified")
	}

	// decompress and prepare ran, redo logs moved, auto.cnf removed, modes fixed.
	for _, f := range []string{".decompressed", ".prepared"} {
		if !exists(filepath.Join(l.repData, f)) {
			t.Errorf("%s marker missing: step didn't run", f)
		}
	}
	for _, f := range []string{"ib_logfile0", "#innodb_redo/#ib_redo0"} {
		if !exists(filepath.Join(l.repLogs, f)) || exists(filepath.Join(l.repData, f)) {
			t.Errorf("%s not moved to the logdir", f)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(l.repLogs, "ib_logfile0")); string(b) != "redo" {
		t.Errorf("logdir holds %q; the old redo log should have been wiped and replaced", b)
	}
	if exists(filepath.Join(l.repData, "auto.cnf")) {
		t.Error("auto.cnf survived; the replica would clone the source's server_uuid")
	}
	if m := mode(t, filepath.Join(l.repData, "backup.xbs")); m != 0o640 {
		t.Errorf("file mode %v, want 0640", m)
	}
	if m := mode(t, filepath.Join(l.repData, "db1")); m != 0o750 {
		t.Errorf("dir mode %v, want 0750", m)
	}

	// The external tools ran in the right order with the right arguments.
	inv := l.invocations()
	iBackup := strings.Index(inv, "xtrabackup --backup --stream=xbstream")
	iExtract := strings.Index(inv, "xbstream -x")
	iDecomp := strings.Index(inv, "xtrabackup --decompress --remove-original")
	iPrep := strings.Index(inv, "xtrabackup --prepare --use-memory=2G --target-dir="+l.repData)
	if iBackup < 0 || iExtract < 0 || iDecomp < 0 || iPrep < 0 || iDecomp < iBackup || iPrep < iDecomp {
		t.Errorf("unexpected tool invocations:\n%s", inv)
	}
	if !strings.Contains(inv, "--compress=zstd") {
		t.Errorf("backup not compressed:\n%s", inv)
	}

	// Both agents were deployed, verified and removed.
	if !strings.Contains(out, "agent on src.local") || !strings.Contains(out, "agent on rep.local") {
		t.Errorf("agents not deployed:\n%s", out)
	}
	if left := l.agentFiles(); len(left) != 0 {
		t.Errorf("agent binaries left on hosts: %v", left)
	}
}

func TestLocalReseedUncompressed(t *testing.T) {
	l := newLab(t)
	l.cfg.Compress = "none"
	os.Remove(filepath.Join(l.fakeBin, "zstd")) // not needed, so not required
	out, err := l.run(t, context.Background(), "", "start")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if exists(filepath.Join(l.repData, ".decompressed")) {
		t.Error("decompress ran for an uncompressed backup")
	}
	if strings.Contains(l.invocations(), "--compress") {
		t.Error("xtrabackup was asked to compress")
	}
}

func TestLocalWipeRefusedWhileMySQLRuns(t *testing.T) {
	l := newLab(t)
	l.cfg.StatusCmd = "true" // "mysqld is running"
	_, err := l.run(t, context.Background(), "", "start")
	if err == nil || !strings.Contains(err.Error(), "still running") {
		t.Fatalf("want refusal, got %v", err)
	}
	if !exists(filepath.Join(l.repData, "stale.ibd")) {
		t.Error("replica data deleted despite the refusal")
	}
	if strings.Contains(l.invocations(), "--backup") {
		t.Error("the backup started after the wipe was refused")
	}
	if left := l.agentFiles(); len(left) != 0 {
		t.Errorf("agents not cleaned up after a failure: %v", left)
	}
}

func TestLocalMissingToolStopsBeforeAnythingDestructive(t *testing.T) {
	l := newLab(t)
	os.Remove(filepath.Join(l.fakeBin, "xbstream"))
	marker := filepath.Join(l.root, "stopped")
	l.cfg.StopCmd = "touch " + marker
	_, err := l.run(t, context.Background(), "", "start")
	if err == nil || !strings.Contains(err.Error(), "rep.local is missing xbstream") || !strings.Contains(err.Error(), "step preflight") {
		t.Fatalf("want preflight failure naming xbstream, got %v", err)
	}
	if exists(marker) {
		t.Error("mysqld was stopped even though preflight failed")
	}
	if !exists(filepath.Join(l.repData, "stale.ibd")) {
		t.Error("replica touched even though preflight failed")
	}
}

func TestLocalMissingSourceDatadir(t *testing.T) {
	l := newLab(t)
	l.cfg.SourceDatadir = filepath.Join(l.root, "source", "nope")
	_, err := l.run(t, context.Background(), "", "preflight")
	if err == nil || !strings.Contains(err.Error(), "has no directory") {
		t.Fatalf("got %v", err)
	}
}

func TestLocalSpaceCheck(t *testing.T) {
	l := newLab(t)
	// Make the source look bigger than the replica's disk with a sparse file.
	avail, err := agent.Avail(l.root)
	if err != nil {
		t.Skip(err)
	}
	f, err := os.Create(filepath.Join(l.srcData, "huge.ibd"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(avail * 2); err != nil {
		f.Close()
		t.Skipf("filesystem can't make a sparse file that large: %v", err)
	}
	f.Close()

	_, err = l.run(t, context.Background(), "", "preflight")
	if err == nil || !strings.Contains(err.Error(), "--skip-space-check") {
		t.Fatalf("want space failure, got %v", err)
	}
	l.cfg.SkipSpaceCheck = true
	out, err := l.run(t, context.Background(), "", "preflight")
	if err != nil || !strings.Contains(out, "WARNING") {
		t.Fatalf("skip-space-check should warn and continue: %v\n%s", err, out)
	}
}

func TestLocalBackupFailureStopsTheRun(t *testing.T) {
	l := newLab(t)
	t.Setenv("FAKE_BACKUP_FAIL", "1")
	out, err := l.run(t, context.Background(), "", "start")
	if err == nil || !strings.Contains(err.Error(), "step stream") {
		t.Fatalf("want stream failure, got %v\n%s", err, out)
	}
	if strings.Contains(out, "matches on both hosts") {
		t.Error("a failed stream was reported as verified")
	}
	if exists(filepath.Join(l.repData, ".prepared")) {
		t.Error("prepare ran after a failed stream")
	}
	if left := l.agentFiles(); len(left) != 0 {
		t.Errorf("agents not cleaned up: %v", left)
	}
}

func TestLocalIncompleteBackupFails(t *testing.T) {
	l := newLab(t)
	t.Setenv("FAKE_NO_CHECKPOINTS", "1")
	_, err := l.run(t, context.Background(), "", "stream")
	if err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("want incomplete-backup failure, got %v", err)
	}
}

func TestLocalInterruptStopsBackupAndCleansUp(t *testing.T) {
	l := newLab(t)
	t.Setenv("FAKE_BACKUP_SLOW", "1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := l.run(t, ctx, "", "stream")
		errc <- err
	}()

	pidFile := filepath.Join(l.pids, "xtrabackup")
	deadline := time.Now().Add(20 * time.Second)
	for !exists(pidFile) {
		if time.Now().After(deadline) {
			t.Fatal("backup never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(string(b))
	time.Sleep(200 * time.Millisecond)
	cancel() // Ctrl-C

	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "interrupted") {
			t.Errorf("want an interrupted error, got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	// The backup process must not outlive go-reseed.
	gone := false
	for range 100 {
		if syscall.Kill(pid, 0) != nil {
			gone = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !gone {
		syscall.Kill(pid, syscall.SIGKILL)
		t.Errorf("xtrabackup (pid %d) still running after the interrupt", pid)
	}
	if left := l.agentFiles(); len(left) != 0 {
		t.Errorf("agents not cleaned up after the interrupt: %v", left)
	}
}

func TestLocalResumeFromPrepare(t *testing.T) {
	l := newLab(t)
	if _, err := l.run(t, context.Background(), "", "decompress"); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(l.repData, ".prepared")) {
		t.Fatal("--to-step decompress ran prepare")
	}
	if _, err := l.run(t, context.Background(), "prepare", "finalize"); err != nil {
		t.Fatal(err)
	}
	if !exists(filepath.Join(l.repData, ".prepared")) || exists(filepath.Join(l.repData, "auto.cnf")) {
		t.Error("resume did not complete prepare..finalize")
	}
	if n := strings.Count(l.invocations(), "--backup"); n != 1 {
		t.Errorf("resuming re-ran the backup (%d backups)", n)
	}
}

func TestLocalDeployRejectsCorruptUpload(t *testing.T) {
	l := newLab(t)
	l.replica.mangleUpload = func(r io.Reader) io.Reader { return io.MultiReader(r, strings.NewReader("tampered")) }
	_, err := l.run(t, context.Background(), "", "preflight")
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("want a corrupt-upload error, got %v", err)
	}
	if left := l.agentFiles(); len(left) != 0 {
		t.Errorf("corrupt agent left on the host: %v", left)
	}
}

// --- orchestration details ------------------------------------------------------------------

// fakeHost records argv and answers with respond (default: success, no output).
type fakeHost struct {
	name    string
	respond func(argv []string) (string, error)

	mu   sync.Mutex
	cmds [][]string
}

func (f *fakeHost) Name() string { return f.name }
func (f *fakeHost) Close() error { return nil }
func (f *fakeHost) Dial(context.Context, string) (net.Conn, error) {
	return nil, errors.New("fakeHost: no network")
}
func (f *fakeHost) Run(_ context.Context, argv []string, _ io.Reader, stdout io.Writer) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, argv)
	f.mu.Unlock()
	if f.respond == nil {
		return nil
	}
	out, err := f.respond(argv)
	if stdout != nil {
		io.WriteString(stdout, out)
	}
	return err
}
func (f *fakeHost) commands() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cmds)
}

func TestStepsOrderAndDestructiveFlags(t *testing.T) {
	r := New(validConfig(), nil, nil, nil)
	var names, destructive []string
	for _, s := range r.Steps() {
		names = append(names, s.Name)
		if s.Destructive {
			destructive = append(destructive, s.Name)
		}
	}
	wantNames := []string{"preflight", "stop", "wipe", "stream", "decompress", "prepare", "move-redo", "finalize", "start", "replication"}
	if !slices.Equal(names, wantNames) {
		t.Errorf("steps = %v, want %v", names, wantNames)
	}
	if want := []string{"stop", "wipe", "stream"}; !slices.Equal(destructive, want) {
		t.Errorf("destructive steps = %v, want %v (the confirmation prompt relies on this)", destructive, want)
	}
}

func TestSelect(t *testing.T) {
	r := New(validConfig(), nil, nil, nil)
	tests := []struct {
		from, to string
		want     []string
		wantErr  string
	}{
		{"", "", []string{"preflight", "stop", "wipe", "stream", "decompress", "prepare", "move-redo", "finalize", "start", "replication"}, ""},
		{"prepare", "", []string{"prepare", "move-redo", "finalize", "start", "replication"}, ""},
		{"", "stream", []string{"preflight", "stop", "wipe", "stream"}, ""},
		{"stream", "stream", []string{"stream"}, ""},
		{"replication", "", []string{"replication"}, ""},
		{"start", "prepare", nil, "comes after"},
		{"bogus", "", nil, `unknown step "bogus"`},
		{"", "bogus", nil, `unknown step "bogus"`},
	}
	for _, tt := range tests {
		t.Run(tt.from+".."+tt.to, func(t *testing.T) {
			steps, err := r.Select(tt.from, tt.to)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("want error %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, s := range steps {
				got = append(got, s.Name)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func dryReseeder(out *bytes.Buffer) *Reseeder {
	c := validConfig()
	c.DryRun = true
	w := &syncWriter{w: out}
	return New(c, &remote.DryRunHost{HostName: "db002", Out: w}, &remote.DryRunHost{HostName: "db003", Out: w}, w)
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestRunStopsAtFirstFailureWithResumeHint(t *testing.T) {
	var out bytes.Buffer
	r := dryReseeder(&out)
	var ran []string
	step := func(name string, err error) Step {
		return Step{Name: name, Description: name, Run: func(context.Context) error {
			ran = append(ran, name)
			return err
		}}
	}
	err := r.Run(context.Background(), []Step{step("a", nil), step("b", errors.New("boom")), step("c", nil)})
	if err == nil {
		t.Fatal("expected error")
	}
	if !slices.Equal(ran, []string{"a", "b"}) {
		t.Errorf("ran %v; steps after a failure must not run", ran)
	}
	for _, want := range []string{"step b: boom", "--from-step b"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should contain %q", err, want)
		}
	}
}

func TestDryRunPrintsEveryStepAndTouchesNothing(t *testing.T) {
	var out bytes.Buffer
	r := dryReseeder(&out)
	steps, _ := r.Select("", "")
	if err := r.Run(context.Background(), steps); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{
		"upload the go-reseed agent",
		"agent preflight --tools xtrabackup,xbstream,zstd",
		"systemctl stop",
		"agent wipe --dir /var/lib/mysql",
		"agent receive --listen :4000",
		"agent send --to db003:4000",
		"xtrabackup --prepare",
		"agent finalize --datadir /var/lib/mysql --owner mysql",
		"CHANGE REPLICATION SOURCE TO",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("dry run output missing %q", want)
		}
	}
	if strings.Index(s, "agent receive") > strings.Index(s, "agent send") {
		t.Error("dry run should show the receiver starting before the sender")
	}
}

func TestWipeDistinguishesStoppedFromUnreachable(t *testing.T) {
	tests := []struct {
		name      string
		statusErr error
		wantErr   string
		wantWipe  bool
	}{
		{"mysqld stopped (status exits 3)", &remote.ExitError{Status: 3}, "", true},
		{"mysqld running (status exits 0)", nil, "still running", false},
		{"ssh failure is not 'stopped'", errors.New("connection reset"), "connection reset", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replica := &fakeHost{name: "db003", respond: func(argv []string) (string, error) {
				if argv[0] == "sh" {
					return "", tt.statusErr
				}
				return `{"event":"result"}` + "\n", nil
			}}
			r := New(validConfig(), nil, replica, &bytes.Buffer{})
			r.repAgent = &agentConn{host: replica, path: "/tmp/agent"}
			err := r.wipe(context.Background())
			if tt.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("want %q, got %v", tt.wantErr, err)
			}
			wiped := false
			for _, argv := range replica.commands() {
				if len(argv) > 2 && argv[2] == "wipe" {
					wiped = true
				}
			}
			if wiped != tt.wantWipe {
				t.Errorf("wipe ran = %v, want %v", wiped, tt.wantWipe)
			}
		})
	}
}

func TestVerifyTransfer(t *testing.T) {
	ok := agent.Event{SHA256: "abc", Bytes: 10}
	tests := []struct {
		name       string
		sent, recv agent.Event
		wantErr    string
	}{
		{"match", ok, ok, ""},
		{"checksum mismatch", ok, agent.Event{SHA256: "abd", Bytes: 10}, "checksum mismatch"},
		{"short transfer", ok, agent.Event{SHA256: "abc", Bytes: 9}, "byte count mismatch"},
		{"no data", agent.Event{SHA256: "e3b0", Bytes: 0}, agent.Event{SHA256: "e3b0", Bytes: 0}, "no data"},
		{"missing receiver report", ok, agent.Event{}, "did not report"},
		{"missing sender report", agent.Event{}, ok, "did not report"},
	}
	for _, tt := range tests {
		err := verifyTransfer(tt.sent, tt.recv)
		if tt.wantErr == "" && err != nil {
			t.Errorf("%s: %v", tt.name, err)
		}
		if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
			t.Errorf("%s: want %q, got %v", tt.name, tt.wantErr, err)
		}
	}
}

func TestSpaceOK(t *testing.T) {
	tests := []struct {
		need, have int64
		ok         bool
	}{
		{1000, 5000, true}, {1000, 1100, true}, {1000, 1099, false}, {0, 0, true}, {1 << 40, 1 << 39, false},
	}
	for _, tt := range tests {
		if err := spaceOK(tt.need, tt.have); (err == nil) != tt.ok {
			t.Errorf("spaceOK(%d, %d) = %v", tt.need, tt.have, err)
		}
	}
}

func TestBackupAndExtractArgv(t *testing.T) {
	c := validConfig()
	c.Parallel = 8
	c.BackupArgs = "--defaults-extra-file=/root/.xb.cnf  --slave-info"
	r := New(c, nil, nil, nil)
	want := []string{"xtrabackup", "--backup", "--stream=xbstream", "--parallel=8", "--target-dir={tmpdir}",
		"--compress=zstd", "--compress-threads=8", "--defaults-extra-file=/root/.xb.cnf", "--slave-info"}
	if got := r.backupArgv(); !slices.Equal(got, want) {
		t.Errorf("backupArgv = %q\nwant        %q", got, want)
	}
	c.Compress = "none"
	if got := New(c, nil, nil, nil).backupArgv(); slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "--compress") }) {
		t.Errorf("--compress none still compresses: %q", got)
	}
	if got, want := r.extractArgv(), []string{"xbstream", "-x", "--parallel=8", "-C", "/var/lib/mysql"}; !slices.Equal(got, want) {
		t.Errorf("extractArgv = %q, want %q", got, want)
	}
}

func TestAgentBinaryLookup(t *testing.T) {
	for in, want := range map[string]string{"x86_64\n": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64\n": "arm64"} {
		if got, err := goArch(in); err != nil || got != want {
			t.Errorf("goArch(%q) = %q, %v", in, got, err)
		}
	}
	if _, err := goArch("ppc64le"); err == nil {
		t.Error("unsupported architecture accepted")
	}

	// Pick an architecture this test binary is not, so the lookup must use AgentBinDir.
	other := "arm64"
	if runtime.GOARCH == "arm64" {
		other = "amd64"
	}
	dir := t.TempDir()
	c := validConfig()
	c.AgentBinDir = dir
	r := New(c, nil, nil, nil)
	if _, err := r.agentBinary(other); err == nil || !strings.Contains(err.Error(), "make build") {
		t.Errorf("missing agent binary should point at `make build`, got %v", err)
	}
	want := filepath.Join(dir, "go-reseed-linux-"+other)
	writeFile(t, want, "elf")
	if p, err := r.agentBinary(other); err != nil || p != want {
		t.Errorf("agentBinary = %q, %v; want %q", p, err, want)
	}
	c.AgentBinary = "/custom/agent"
	if p, _ := New(c, nil, nil, nil).agentBinary("amd64"); p != "/custom/agent" {
		t.Errorf("--agent-binary not honored: %q", p)
	}
}

func TestEventWriterHandlesSplitLines(t *testing.T) {
	var got []agent.Event
	w := &eventWriter{on: func(e agent.Event) { got = append(got, e) }}
	for _, chunk := range []string{`{"event":"listen`, `ing","addr":":4000"}` + "\n" + `{"event":"progress","bytes":5}`, "\n\n", `{"event":"done","sha256":"ab","bytes":9}` + "\n"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 3 || got[0].Addr != ":4000" || got[1].Bytes != 5 || got[2].SHA256 != "ab" {
		t.Errorf("decoded %+v", got)
	}
	if _, err := w.Write([]byte("not json\n")); err == nil {
		t.Error("garbage from the agent must be an error")
	}
}

func TestHumanBytes(t *testing.T) {
	tests := map[int64]string{
		0: "0 B", 512: "512 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 5 << 20: "5.0 MiB",
		212 << 30: "212.0 GiB", 3 << 40: "3.0 TiB", 2 << 50: "2.0 PiB",
	}
	for in, want := range tests {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestProgressPrinter(t *testing.T) {
	var out bytes.Buffer
	p := newProgressPrinter(&out)
	p.lastT = time.Now().Add(-2 * time.Second)
	p.print(20 << 20)
	if !strings.Contains(out.String(), "20.0 MiB received (") || !strings.Contains(out.String(), "MiB/s)") {
		t.Errorf("got %q", out.String())
	}
}
