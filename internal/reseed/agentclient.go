package reseed

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ChaosHour/go-reseed/internal/agent"
	"github.com/ChaosHour/go-reseed/internal/remote"
)

// agentConn runs agent ops on one host through its uploaded agent binary.
type agentConn struct {
	host remote.Host
	path string // the agent binary on the host
	dry  bool
}

// uploadScript writes stdin to a new private file under $1 and prints its path. It is the
// only shell go-reseed needs on a host besides the user's own service commands.
const uploadScript = `umask 077 && f=$(mktemp "$1/go-reseed-agent.XXXXXX") && cat > "$f" && chmod 700 "$f" && printf '%s' "$f"`

// deployAgent uploads the agent binary that matches the host's architecture and checks
// that the uploaded copy runs and is byte-for-byte what was sent.
func (r *Reseeder) deployAgent(ctx context.Context, h remote.Host) (*agentConn, error) {
	if r.cfg.DryRun {
		path := r.cfg.AgentDir + "/go-reseed-agent.XXXXXX"
		fmt.Fprintf(r.out, "  [%s] upload the go-reseed agent for this host's CPU to %s\n", h.Name(), path)
		return &agentConn{host: h, path: path, dry: true}, nil
	}

	uname, err := remote.Shell(ctx, h, "uname -m")
	if err != nil {
		return nil, err
	}
	arch, err := goArch(uname)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", h.Name(), err)
	}
	bin, err := r.agentBinary(arch)
	if err != nil {
		return nil, err
	}
	want, err := agent.FileSHA256(bin)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(bin)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out bytes.Buffer
	if err := h.Run(ctx, []string{"sh", "-c", uploadScript, "sh", r.cfg.AgentDir}, f, &out); err != nil {
		return nil, fmt.Errorf("upload agent to %s:%s: %w", h.Name(), r.cfg.AgentDir, err)
	}
	path := strings.TrimSpace(out.String())
	if !strings.HasPrefix(path, r.cfg.AgentDir+"/go-reseed-agent.") {
		return nil, fmt.Errorf("%s: unexpected agent path %q", h.Name(), path)
	}
	a := &agentConn{host: h, path: path}

	ev, err := a.call(ctx, "version")
	if err != nil {
		a.cleanup()
		return nil, fmt.Errorf("the agent uploaded to %s:%s does not run (is %s mounted noexec? try --agent-dir): %w",
			h.Name(), path, r.cfg.AgentDir, err)
	}
	if ev.SHA256 != want {
		a.cleanup()
		return nil, fmt.Errorf("%s: uploaded agent is corrupt (sha256 %s, want %s)", h.Name(), ev.SHA256, want)
	}
	if ev.Protocol != agent.Protocol {
		a.cleanup()
		return nil, fmt.Errorf("%s: agent speaks protocol %d, want %d; rebuild with `make build`", h.Name(), ev.Protocol, agent.Protocol)
	}
	fmt.Fprintf(r.out, "    agent on %s: %s (linux/%s, sha256 %s…)\n", h.Name(), path, arch, want[:12])
	return a, nil
}

// cleanup deletes the uploaded agent. It uses its own context so it still runs after an
// interrupt.
func (a *agentConn) cleanup() {
	if a == nil || a.dry {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = a.host.Run(ctx, []string{"rm", "-f", a.path}, nil, nil)
}

// call runs a one-shot agent op and returns its result event.
func (a *agentConn) call(ctx context.Context, op string, args ...string) (agent.Event, error) {
	var out bytes.Buffer
	if err := a.host.Run(ctx, append([]string{a.path, "agent", op}, args...), nil, &out); err != nil {
		return agent.Event{}, err
	}
	if a.dry {
		return agent.Event{}, nil
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	var ev agent.Event
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil {
		return agent.Event{}, fmt.Errorf("%s: agent %s: bad output %q: %w", a.host.Name(), op, out.String(), err)
	}
	return ev, nil
}

// stream runs a long-lived agent op (send or receive), passing it the connection token on
// stdin and calling on for each event it emits. The op's stdin stays open until stream
// returns, so the agent stops its child process if go-reseed goes away.
func (a *agentConn) stream(ctx context.Context, op string, args []string, token string, on func(agent.Event)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stdin := io.MultiReader(strings.NewReader(token+"\n"), remote.WaitReader(ctx))
	w := &eventWriter{on: on}
	return a.host.Run(ctx, append([]string{a.path, "agent", op}, args...), stdin, w)
}

// eventWriter decodes JSON-line events as they arrive.
type eventWriter struct {
	buf []byte
	on  func(agent.Event)
}

func (w *eventWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := w.buf[:i]
		w.buf = w.buf[i+1:]
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var ev agent.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return 0, fmt.Errorf("bad agent event %q: %w", line, err)
		}
		w.on(ev)
	}
}

// agentBinary finds the local linux/<arch> go-reseed binary to upload.
func (r *Reseeder) agentBinary(arch string) (string, error) {
	if r.cfg.AgentBinary != "" {
		return r.cfg.AgentBinary, nil
	}
	if runtime.GOOS == "linux" && runtime.GOARCH == arch {
		return os.Executable()
	}
	p := filepath.Join(r.cfg.AgentBinDir, "go-reseed-linux-"+arch)
	if _, err := os.Stat(p); err != nil {
		return "", fmt.Errorf("no agent binary for linux/%s at %s: run `make build`, or pass --agent-binary", arch, p)
	}
	return p, nil
}

// goArch maps `uname -m` output to a Go architecture name.
func goArch(uname string) (string, error) {
	switch m := strings.TrimSpace(uname); m {
	case "x86_64", "amd64":
		return "amd64", nil
	case "aarch64", "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported CPU architecture %q", m)
	}
}

func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// isExit reports whether err is a remote command exiting non-zero (as opposed to, say, a
// lost connection).
func isExit(err error) bool {
	var ee *remote.ExitError
	return errors.As(err, &ee)
}
