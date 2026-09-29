package remote

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestQuoteRoundTripsThroughBash(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	inputs := []string{
		"",
		"plain",
		"/db/data01",
		"it's",
		`a"b`,
		"$HOME and ${PATH}",
		"`id`",
		"$(rm -rf /)",
		"semi; colon && amp || pipe | end",
		"multi\nline",
		`back\slash`,
		"'''",
		"#innodb_redo",
		"glob * ? [a]",
	}
	for _, in := range inputs {
		out, err := exec.Command("bash", "-c", "printf %s "+Quote(in)).Output()
		if err != nil {
			t.Fatalf("Quote(%q) produced an invalid word: %v", in, err)
		}
		if string(out) != in {
			t.Errorf("Quote(%q) round-tripped to %q", in, out)
		}
	}
}

func TestTailBufferKeepsLastBytes(t *testing.T) {
	tb := &tailBuffer{max: 10}
	tb.Write([]byte("0123456789"))
	tb.Write([]byte("abcde"))
	if got := tb.String(); got != "56789abcde" {
		t.Errorf("got %q", got)
	}
	tb = &tailBuffer{max: 100}
	tb.Write([]byte("  error: disk full\n"))
	if got := tb.String(); got != "error: disk full" {
		t.Errorf("tail should be trimmed, got %q", got)
	}
}

func TestPrefixWriterPrefixesEachLine(t *testing.T) {
	var buf bytes.Buffer
	w := &prefixWriter{prefix: "[db003] ", w: &buf}
	// Writes that split lines arbitrarily, as SSH delivers them.
	for _, chunk := range []string{"first line\nsec", "ond line\n", "third\nfourth", "\n"} {
		w.Write([]byte(chunk))
	}
	want := "[db003] first line\n[db003] second line\n[db003] third\n[db003] fourth\n"
	if buf.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", buf.String(), want)
	}
}

func TestDryRunHostPrintsWithoutRunning(t *testing.T) {
	var buf bytes.Buffer
	h := &DryRunHost{HostName: "db003", Out: &buf}
	if err := h.Run(context.Background(), []string{"rm", "-rf", "/definitely/not"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(context.Background(), []string{"sh", "-c", "systemctl stop mysqld"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[db003] $ rm -rf /definitely/not", "[db003] $ systemctl stop mysqld"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("missing %q in %q", want, buf.String())
		}
	}
	if _, err := h.Dial(context.Background(), "127.0.0.1:3306"); err == nil {
		t.Error("dry-run Dial should fail")
	}
}

func TestQuoteLeavesSafeWordsAlone(t *testing.T) {
	for in, want := range map[string]string{
		"xtrabackup":              "xtrabackup",
		"--target-dir=/db/data01": "--target-dir=/db/data01",
		"db003:4000":              "db003:4000",
		"":                        "''",
		"{tmpdir}":                "'{tmpdir}'",
		"#innodb_redo":            "'#innodb_redo'",
		"a b":                     "'a b'",
	} {
		if got := Quote(in); got != want {
			t.Errorf("Quote(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestCommandLineRoundTripsArgv(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	argv := []string{"printf", "%s|", "plain", "with space", "it's", "$(id)", "", "*"}
	out, err := exec.Command("sh", "-c", CommandLine(argv)).Output()
	if err != nil {
		t.Fatal(err)
	}
	if want := "plain|with space|it's|$(id)||*|"; string(out) != want {
		t.Errorf("sh parsed %q, want %q", out, want)
	}
}

func TestWaitReaderBlocksUntilDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		_, err := WaitReader(ctx).Read(make([]byte, 1))
		got <- err
	}()
	select {
	case <-got:
		t.Fatal("WaitReader returned before its context ended")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-got:
		if err != io.EOF {
			t.Errorf("got %v, want io.EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReader did not return after cancel")
	}
}

func TestExitErrorMessage(t *testing.T) {
	err := &ExitError{Host: "db003", Cmd: "systemctl is-active mysqld", Status: 3, Stderr: "inactive"}
	for _, want := range []string{"db003", "systemctl is-active mysqld", "status 3", "inactive"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q missing %q", err.Error(), want)
		}
	}
}
