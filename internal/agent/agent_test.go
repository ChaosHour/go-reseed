package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary doubles as the external commands the agent runs, chosen by the name it
// is invoked as (tests symlink it into a temp dir):
//
//	fake-backup <n>        writes n deterministic bytes to stdout; FAKE_FAIL=1 exits 1 halfway
//	fake-extract <file>    copies stdin to file
//	fake-slow              writes forever, slowly (for cancellation tests)
func TestMain(m *testing.M) {
	switch filepath.Base(os.Args[0]) {
	case "fake-backup":
		n := 0
		fmt.Sscan(os.Args[1], &n)
		half := n / 2
		if os.Getenv("FAKE_FAIL") == "1" {
			os.Stdout.Write(payload(half))
			fmt.Fprintln(os.Stderr, "fake-backup: simulated failure")
			os.Exit(1)
		}
		os.Stdout.Write(payload(n))
		os.Exit(0)
	case "fake-extract":
		f, err := os.Create(os.Args[1])
		if err != nil {
			os.Exit(1)
		}
		if _, err := io.Copy(f, os.Stdin); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	case "fake-slow":
		for {
			if _, err := os.Stdout.Write(payload(1024)); err != nil {
				os.Exit(1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	os.Exit(m.Run())
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// fakes symlinks the test binary under the fake command names and returns their paths.
func fakes(t *testing.T) map[string]string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	m := map[string]string{}
	for _, name := range []string{"fake-backup", "fake-extract", "fake-slow"} {
		p := filepath.Join(dir, name)
		if err := os.Symlink(exe, p); err != nil {
			t.Fatal(err)
		}
		m[name] = p
	}
	return m
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

const token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// collect decodes the events written to buf.
func collect(t *testing.T, buf *bytes.Buffer) []Event {
	t.Helper()
	var evs []Event
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("bad event %q: %v", line, err)
		}
		evs = append(evs, ev)
	}
	return evs
}

func last(evs []Event, kind string) (Event, bool) {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Event == kind {
			return evs[i], true
		}
	}
	return Event{}, false
}

// --- streaming ---------------------------------------------------------------------------

func TestSendReceiveTransfersExactBytes(t *testing.T) {
	f := fakes(t)
	addr := freePort(t)
	out := filepath.Join(t.TempDir(), "received")
	const n = 3<<20 + 17 // a few MB, not a multiple of any buffer size

	var recvOut, sendOut bytes.Buffer
	recvErr := make(chan error, 1)
	go func() {
		recvErr <- Receive(context.Background(), ReceiveOptions{
			Listen: addr, Token: token, AcceptTimeout: 10 * time.Second,
			Extract: []string{f["fake-extract"], out},
		}, NewEmitter(&recvOut), io.Discard)
	}()
	err := Send(context.Background(), SendOptions{
		To: addr, Token: token, DialTimeout: 10 * time.Second,
		Backup: []string{f["fake-backup"], fmt.Sprint(n)},
	}, NewEmitter(&sendOut), io.Discard)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := <-recvErr; err != nil {
		t.Fatalf("receive: %v", err)
	}

	want := payload(n)
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes, want %d identical bytes", len(got), len(want))
	}
	sent, _ := last(collect(t, &sendOut), "done")
	recv, _ := last(collect(t, &recvOut), "done")
	if sent.SHA256 != sha(want) || recv.SHA256 != sha(want) {
		t.Errorf("checksums: sent %s, received %s, want %s", sent.SHA256, recv.SHA256, sha(want))
	}
	if sent.Bytes != int64(n) || recv.Bytes != int64(n) {
		t.Errorf("byte counts: sent %d, received %d, want %d", sent.Bytes, recv.Bytes, n)
	}
	if _, ok := last(collect(t, &recvOut), "listening"); !ok {
		t.Error("receiver never reported listening")
	}
}

func TestReceiveRejectsWrongToken(t *testing.T) {
	f := fakes(t)
	addr := freePort(t)
	out := filepath.Join(t.TempDir(), "received")
	var stderr bytes.Buffer
	recvErr := make(chan error, 1)
	go func() {
		recvErr <- Receive(context.Background(), ReceiveOptions{
			Listen: addr, Token: token, AcceptTimeout: 10 * time.Second,
			Extract: []string{f["fake-extract"], out},
		}, NewEmitter(io.Discard), &stderr)
	}()

	// An intruder with the wrong token is dropped without affecting the real transfer.
	var conn net.Conn
	var err error
	for range 50 {
		if conn, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "%s\n", strings.Repeat("x", len(token)))
	conn.Write([]byte("evil data"))
	conn.Close()

	if err := Send(context.Background(), SendOptions{To: addr, Token: token, DialTimeout: 10 * time.Second,
		Backup: []string{f["fake-backup"], "1000"}}, NewEmitter(io.Discard), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := <-recvErr; err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, payload(1000)) {
		t.Errorf("stream corrupted by the rejected connection: %q", got[:min(len(got), 20)])
	}
	if !strings.Contains(stderr.String(), "rejected connection") {
		t.Errorf("rejection not logged: %q", stderr.String())
	}
}

func TestReceiveTimesOutWithoutSender(t *testing.T) {
	f := fakes(t)
	err := Receive(context.Background(), ReceiveOptions{
		Listen: freePort(t), Token: token, AcceptTimeout: 300 * time.Millisecond,
		Extract: []string{f["fake-extract"], filepath.Join(t.TempDir(), "x")},
	}, NewEmitter(io.Discard), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no sender connected") {
		t.Fatalf("want timeout error, got %v", err)
	}
}

func TestSendFailsWhenBackupFails(t *testing.T) {
	f := fakes(t)
	addr := freePort(t)
	out := filepath.Join(t.TempDir(), "received")
	t.Setenv("FAKE_FAIL", "1")

	recvErr := make(chan error, 1)
	go func() {
		recvErr <- Receive(context.Background(), ReceiveOptions{Listen: addr, Token: token, AcceptTimeout: 10 * time.Second,
			Extract: []string{f["fake-extract"], out}}, NewEmitter(io.Discard), io.Discard)
	}()
	var sendOut bytes.Buffer
	err := Send(context.Background(), SendOptions{To: addr, Token: token, DialTimeout: 10 * time.Second,
		Backup: []string{f["fake-backup"], "100000"}}, NewEmitter(&sendOut), io.Discard)
	if err == nil {
		t.Fatal("a failed backup must fail the send")
	}
	if _, ok := last(collect(t, &sendOut), "done"); ok {
		t.Error("a failed send must not report done")
	}
	<-recvErr // the receiver sees a short stream; the orchestrator relies on the sender's error
}

func TestSendGivesUpWhenNobodyListens(t *testing.T) {
	f := fakes(t)
	err := Send(context.Background(), SendOptions{To: freePort(t), Token: token, DialTimeout: 700 * time.Millisecond,
		Backup: []string{f["fake-backup"], "10"}}, NewEmitter(io.Discard), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "connect to receiver") {
		t.Fatalf("want connection error, got %v", err)
	}
}

func TestCancellationStopsTheBackupProcess(t *testing.T) {
	f := fakes(t)
	addr := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	recvErr := make(chan error, 1)
	go func() {
		recvErr <- Receive(ctx, ReceiveOptions{Listen: addr, Token: token, AcceptTimeout: 10 * time.Second,
			Extract: []string{f["fake-extract"], filepath.Join(t.TempDir(), "x")}}, NewEmitter(io.Discard), io.Discard)
	}()
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- Send(ctx, SendOptions{To: addr, Token: token, DialTimeout: 10 * time.Second,
			Backup: []string{f["fake-slow"]}}, NewEmitter(io.Discard), io.Discard)
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	for name, ch := range map[string]chan error{"send": sendErr, "receive": recvErr} {
		select {
		case err := <-ch:
			if err == nil {
				t.Errorf("%s: want an error after cancellation", name)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not stop after cancellation; its child process would keep running", name)
		}
	}
}

func TestStdinEOFCancelsStreamingOps(t *testing.T) {
	// go-reseed keeps the agent's stdin open for the whole op. When its SSH session goes
	// away stdin hits EOF, and the op must stop rather than wait for a sender forever.
	f := fakes(t)
	pr, pw := io.Pipe()
	done := make(chan int, 1)
	go func() {
		done <- Main(context.Background(), "test", []string{"receive", "--listen", freePort(t), "--accept-timeout", "1m",
			"--", f["fake-extract"], filepath.Join(t.TempDir(), "x")}, pr, io.Discard, io.Discard)
	}()
	fmt.Fprintln(pw, token)
	time.Sleep(200 * time.Millisecond)
	pw.Close() // the SSH session ends
	select {
	case code := <-done:
		if code == 0 {
			t.Error("an abandoned receive should exit non-zero")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("receive kept running after stdin closed")
	}
}

func TestStreamingOpsRequireAToken(t *testing.T) {
	var stderr bytes.Buffer
	code := Main(context.Background(), "test", []string{"send", "--to", "127.0.0.1:1", "--", "true"},
		strings.NewReader("short\n"), io.Discard, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "token") {
		t.Errorf("code %d, stderr %q", code, stderr.String())
	}
}

func TestCopyHashedEmitsProgress(t *testing.T) {
	var out bytes.Buffer
	em := NewEmitter(&out)
	r := io.MultiReader(bytes.NewReader(payload(1000)), slowReader{50 * time.Millisecond}, bytes.NewReader(payload(10)))
	n, sum, err := copyHashed(io.Discard, r, 10*time.Millisecond, em)
	if err != nil || n != 1010 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if sum != sha(append(payload(1000), payload(10)...)) {
		t.Error("wrong checksum")
	}
	if _, ok := last(collect(t, &out), "progress"); !ok {
		t.Error("no progress events emitted")
	}
}

type slowReader struct{ d time.Duration }

func (s slowReader) Read([]byte) (int, error) { time.Sleep(s.d); return 0, io.EOF }

// --- filesystem --------------------------------------------------------------------------

func mustWrite(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool { _, err := os.Lstat(p); return err == nil }

func TestCheckWipeDir(t *testing.T) {
	for _, d := range []string{"/var/lib/mysql", "/db/data01", "/db/data01/"} {
		if err := CheckWipeDir(d); err != nil {
			t.Errorf("CheckWipeDir(%q) = %v", d, err)
		}
	}
	for _, d := range []string{"", "/", "/db", "/db/", "db/data01", "/db/data01/../..", "/var/lib/.."} {
		if err := CheckWipeDir(d); err == nil {
			t.Errorf("CheckWipeDir(%q) accepted an unsafe directory", d)
		}
	}
}

func TestWipe(t *testing.T) {
	root := t.TempDir()
	data, logs := filepath.Join(root, "data01"), filepath.Join(root, "logs01")
	sibling := filepath.Join(root, "keep-me")
	for _, f := range []string{filepath.Join(data, "ibdata1"), filepath.Join(data, ".hidden"),
		filepath.Join(data, "db1", "t.ibd"), filepath.Join(logs, "ib_logfile0"), sibling} {
		mustWrite(t, f)
	}
	os.Symlink(sibling, filepath.Join(data, "link-out")) // must remove the link, not its target

	if err := Wipe([]string{data, logs}, filepath.Join(root, "no-proc")); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{data, logs} {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Errorf("%s: entries %v, err %v; want an empty directory", dir, entries, err)
		}
	}
	if !exists(sibling) {
		t.Error("wipe followed a symlink or deleted outside the datadir")
	}

	fresh := filepath.Join(root, "new", "data")
	if err := Wipe([]string{fresh}, filepath.Join(root, "no-proc")); err != nil || !exists(fresh) {
		t.Errorf("wipe should create a missing datadir: %v", err)
	}
}

func TestWipeRefusesUnsafeDirs(t *testing.T) {
	for _, dirs := range [][]string{nil, {"/"}, {"/db"}, {"relative/dir"}} {
		if err := Wipe(dirs, "/nonexistent"); err == nil {
			t.Errorf("Wipe(%q) should refuse", dirs)
		}
	}
}

func TestWipeRefusesWhileMySQLRunsInDatadir(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data01")
	mustWrite(t, filepath.Join(data, "ibdata1"))

	// A fake /proc with a mysqld whose working directory is the datadir, and an unrelated
	// mysqld elsewhere.
	proc := filepath.Join(root, "proc")
	fakeProc := func(pid, comm, cwd string) {
		os.MkdirAll(filepath.Join(proc, pid), 0o755)
		os.WriteFile(filepath.Join(proc, pid, "comm"), []byte(comm+"\n"), 0o644)
		os.Symlink(cwd, filepath.Join(proc, pid, "cwd"))
	}
	fakeProc("100", "bash", data)
	fakeProc("200", "mysqld", filepath.Join(root, "other-instance"))
	if err := Wipe([]string{data}, proc); err != nil {
		t.Fatalf("unrelated processes must not block the wipe: %v", err)
	}
	mustWrite(t, filepath.Join(data, "ibdata1"))

	fakeProc("4242", "mysqld", data)
	err := Wipe([]string{data}, proc)
	if err == nil || !strings.Contains(err.Error(), "pid 4242") {
		t.Fatalf("want refusal naming pid 4242, got %v", err)
	}
	if !exists(filepath.Join(data, "ibdata1")) {
		t.Error("refused wipe still deleted files")
	}
}

func TestMoveRedo(t *testing.T) {
	root := t.TempDir()
	data, logs := filepath.Join(root, "data01"), filepath.Join(root, "logs01")
	for _, f := range []string{"ibdata1", "ib_logfile0", "ib_logfile1", "#innodb_redo/#ib_redo0", "mysql.ibd"} {
		mustWrite(t, filepath.Join(data, f))
	}
	moved, err := MoveRedo(data, logs)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(moved)
	if want := []string{"#innodb_redo", "ib_logfile0", "ib_logfile1"}; !slices.Equal(moved, want) {
		t.Errorf("moved %v, want %v", moved, want)
	}
	for _, f := range []string{"ib_logfile0", "ib_logfile1", "#innodb_redo/#ib_redo0"} {
		if !exists(filepath.Join(logs, f)) || exists(filepath.Join(data, f)) {
			t.Errorf("%s not moved", f)
		}
	}
	for _, f := range []string{"ibdata1", "mysql.ibd"} {
		if !exists(filepath.Join(data, f)) {
			t.Errorf("%s must stay in the datadir", f)
		}
	}
}

func TestMoveRedoAcrossFilesystems(t *testing.T) {
	orig := rename
	rename = func(string, string) error { return &os.LinkError{Op: "rename", Err: syscall.EXDEV} }
	t.Cleanup(func() { rename = orig })

	root := t.TempDir()
	data, logs := filepath.Join(root, "data01"), filepath.Join(root, "logs01")
	mustWrite(t, filepath.Join(data, "#innodb_redo", "#ib_redo7"))
	os.WriteFile(filepath.Join(data, "ib_logfile0"), []byte("redo bytes"), 0o640)

	if _, err := MoveRedo(data, logs); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(logs, "ib_logfile0")); string(b) != "redo bytes" {
		t.Errorf("content not copied: %q", b)
	}
	if !exists(filepath.Join(logs, "#innodb_redo", "#ib_redo7")) {
		t.Error("directory not copied")
	}
	if exists(filepath.Join(data, "ib_logfile0")) || exists(filepath.Join(data, "#innodb_redo")) {
		t.Error("originals not removed after the copy")
	}
}

func TestMoveRedoNeverOverwrites(t *testing.T) {
	root := t.TempDir()
	data, logs := filepath.Join(root, "data01"), filepath.Join(root, "logs01")
	mustWrite(t, filepath.Join(data, "ib_logfile0"))
	mustWrite(t, filepath.Join(logs, "ib_logfile0"))
	if _, err := MoveRedo(data, logs); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("want refusal, got %v", err)
	}
}

func TestMoveRedoNothingToMove(t *testing.T) {
	root := t.TempDir()
	moved, err := MoveRedo(filepath.Join(root, "data"), filepath.Join(root, "logs"))
	if err != nil || len(moved) != 0 {
		t.Errorf("moved %v, err %v", moved, err)
	}
}

func TestFinalize(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	root := t.TempDir()
	data, logs := filepath.Join(root, "data01"), filepath.Join(root, "logs01")
	for _, f := range []string{"auto.cnf", "ibdata1", "db1/t.ibd", "backup-my.cnf"} {
		mustWrite(t, filepath.Join(data, f))
	}
	mustWrite(t, filepath.Join(logs, "#innodb_redo", "#ib_redo0"))
	os.Chmod(filepath.Join(data, "ibdata1"), 0o777)
	outside := filepath.Join(root, "outside")
	mustWrite(t, outside)
	os.Chmod(outside, 0o604)
	os.Symlink(outside, filepath.Join(data, "link"))

	if _, err := Finalize(context.Background(), data, logs, me.Username); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(data, "auto.cnf")) {
		t.Error("auto.cnf not removed; the replica would clone the source's server_uuid")
	}
	check := func(p string, want os.FileMode) {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != want {
			t.Errorf("%s: mode %v, want %v", p, st.Mode().Perm(), want)
		}
	}
	check(data, 0o750)
	check(filepath.Join(data, "db1"), 0o750)
	check(filepath.Join(data, "ibdata1"), 0o640)
	check(filepath.Join(data, "db1", "t.ibd"), 0o640)
	check(filepath.Join(logs, "#innodb_redo"), 0o750)
	check(filepath.Join(logs, "#innodb_redo", "#ib_redo0"), 0o640)
	check(outside, 0o604) // symlink targets outside the datadir are left alone
}

func TestFinalizeUnknownOwner(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data01")
	mustWrite(t, filepath.Join(data, "ibdata1"))
	if _, err := Finalize(context.Background(), data, "", "no-such-user-go-reseed"); err == nil {
		t.Error("an unknown owner must be an error")
	}
}

func TestCheckBackup(t *testing.T) {
	tests := map[string]string{
		"backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 123\n": "",
		"backup_type = full-prepared\n":                             "full-prepared",
		"backup_type = incremental\n":                               "incremental",
		"from_lsn = 0\n":                                            "no backup_type",
	}
	for content, wantErr := range tests {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "xtrabackup_checkpoints"), []byte(content), 0o644)
		err := CheckBackup(dir)
		if wantErr == "" && err != nil {
			t.Errorf("%q: unexpected error %v", content, err)
		}
		if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
			t.Errorf("%q: want error containing %q, got %v", content, wantErr, err)
		}
	}
	if err := CheckBackup(t.TempDir()); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Errorf("missing checkpoints file: got %v", err)
	}
}

func TestDirSizeAndAvail(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a"), make([]byte, 1000), 0o644)
	os.MkdirAll(filepath.Join(dir, "sub"), 0o755)
	os.WriteFile(filepath.Join(dir, "sub", "b"), make([]byte, 24), 0o644)
	os.Symlink(filepath.Join(dir, "a"), filepath.Join(dir, "link")) // not counted twice
	if n, err := DirSize(dir); err != nil || n != 1024 {
		t.Errorf("DirSize = %d, %v; want 1024", n, err)
	}
	if n, err := DirSize(filepath.Join(dir, "missing")); err != nil || n != 0 {
		t.Errorf("missing dir: %d, %v", n, err)
	}
	if n, err := Avail(dir); err != nil || n <= 0 {
		t.Errorf("Avail = %d, %v", n, err)
	}
}

func TestMissingTools(t *testing.T) {
	got := MissingTools([]string{"go", "no-such-tool-xyz", "also-missing-abc"})
	if want := []string{"no-such-tool-xyz", "also-missing-abc"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// --- Main / op dispatch --------------------------------------------------------------------

func runOp(t *testing.T, args ...string) (Event, int, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(context.Background(), "v-test", args, strings.NewReader(""), &out, &errb)
	evs := collect(t, &out)
	if len(evs) == 0 {
		return Event{}, code, errb.String()
	}
	return evs[len(evs)-1], code, errb.String()
}

func TestMainVersionReportsProtocolAndSelfHash(t *testing.T) {
	ev, code, _ := runOp(t, "version")
	exe, _ := os.Executable()
	want, _ := FileSHA256(exe)
	if code != 0 || ev.Protocol != Protocol || ev.Version != "v-test" || ev.SHA256 != want {
		t.Errorf("code %d, event %+v; want protocol %d and sha256 %s", code, ev, Protocol, want)
	}
}

func TestMainOps(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data01")
	mustWrite(t, filepath.Join(data, "ibdata1"))
	os.WriteFile(filepath.Join(data, "xtrabackup_binlog_info"), []byte("binlog.000003\t197\tuuid:1-5\n"), 0o644)
	os.WriteFile(filepath.Join(data, "xtrabackup_checkpoints"), []byte("backup_type = full-backuped\n"), 0o644)

	if ev, code, _ := runOp(t, "preflight", "--tools", "go,no-such-tool-xyz", "--dir", data); code != 0 ||
		!slices.Equal(ev.Missing, []string{"no-such-tool-xyz"}) || !ev.DirExists {
		t.Errorf("preflight: code %d, %+v", code, ev)
	}
	if ev, code, _ := runOp(t, "space", "--dir", data); code != 0 || ev.DirBytes <= 0 || ev.AvailBytes <= 0 {
		t.Errorf("space: code %d, %+v", code, ev)
	}
	if ev, code, _ := runOp(t, "binlog-info", "--dir", data); code != 0 || !strings.HasPrefix(ev.Content, "binlog.000003\t197") {
		t.Errorf("binlog-info: code %d, %+v", code, ev)
	}
	if _, code, _ := runOp(t, "check-backup", "--dir", data); code != 0 {
		t.Errorf("check-backup: code %d", code)
	}
	if _, code, stderr := runOp(t, "wipe", "--dir", "/"); code == 0 || !strings.Contains(stderr, "too close to /") {
		t.Errorf("wipe / must fail: code %d, %s", code, stderr)
	}
	if _, code, _ := runOp(t, "wipe", "--dir", data); code != 0 {
		t.Errorf("wipe: code %d", code)
	}
	if entries, _ := os.ReadDir(data); len(entries) != 0 {
		t.Errorf("wipe op left %v", entries)
	}
}

func TestMainRejectsUnknownOps(t *testing.T) {
	for _, args := range [][]string{nil, {"format-disk"}, {"version", "--bogus"}} {
		if _, code, _ := runOp(t, args...); code != 2 {
			t.Errorf("%q: exit code %d, want 2", args, code)
		}
	}
}

func TestFirstErr(t *testing.T) {
	a, b := errors.New("a"), errors.New("b")
	if firstErr(nil, a, b) != a || firstErr(nil, nil) != nil {
		t.Error("firstErr picked the wrong error")
	}
}
