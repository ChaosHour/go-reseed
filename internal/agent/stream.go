package agent

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ReceiveOptions configures Receive.
type ReceiveOptions struct {
	Listen        string        // e.g. ":4000"
	Token         string        // the sender must present this before any data
	AcceptTimeout time.Duration // how long to wait for an authorized sender
	Progress      time.Duration // interval between progress events; 0 disables them
	Extract       []string      // reads the stream on stdin, e.g. xbstream -x -C <datadir>
}

// Receive listens for one authorized sender and pipes its stream into Extract, hashing it on
// the way. It emits "listening" once the port is open, "progress" periodically, and "done"
// with the SHA-256 and byte count after Extract exits successfully.
func Receive(ctx context.Context, o ReceiveOptions, em *Emitter, stderr io.Writer) error {
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	em.Emit(Event{Event: "listening", Addr: ln.Addr().String()})

	conn, err := acceptAuthorized(ctx, ln, o.Token, o.AcceptTimeout, stderr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stopConn := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopConn()

	cmd := exec.CommandContext(ctx, o.Extract[0], o.Extract[1:]...)
	cmd.Stderr = stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	n, sum, copyErr := copyHashed(stdin, conn, o.Progress, em)
	stdin.Close()
	if copyErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if err := firstErr(ctx.Err(), copyErr, waitErr); err != nil {
		return fmt.Errorf("receive after %d bytes: %w", n, err)
	}
	em.Emit(Event{Event: "done", SHA256: sum, Bytes: n})
	return nil
}

// acceptAuthorized returns the first connection that presents the token, closing others.
func acceptAuthorized(ctx context.Context, ln net.Listener, token string, timeout time.Duration, stderr io.Writer) (net.Conn, error) {
	if tl, ok := ln.(*net.TCPListener); ok && timeout > 0 {
		_ = tl.SetDeadline(time.Now().Add(timeout))
	}
	want := []byte(token + "\n")
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, fmt.Errorf("no sender connected within %s", timeout)
			}
			return nil, err
		}
		got := make([]byte, len(want))
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, err = io.ReadFull(conn, got)
		_ = conn.SetReadDeadline(time.Time{})
		if err == nil && subtle.ConstantTimeCompare(got, want) == 1 {
			return conn, nil
		}
		fmt.Fprintf(stderr, "rejected connection from %s: bad or missing token\n", conn.RemoteAddr())
		conn.Close()
	}
}

// SendOptions configures Send.
type SendOptions struct {
	To          string        // receiver address, host:port
	Token       string        // presented to the receiver before any data
	DialTimeout time.Duration // how long to keep retrying the connection
	Progress    time.Duration // interval between progress events; 0 disables them
	Backup      []string      // writes the stream to stdout; "{tmpdir}" becomes a scratch directory
}

// Send runs Backup and streams its stdout to the receiver, hashing it on the way. It emits
// "done" with the SHA-256 and byte count after Backup exits successfully.
func Send(ctx context.Context, o SendOptions, em *Emitter, stderr io.Writer) error {
	tmp, err := os.MkdirTemp("", "go-reseed-backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	args := make([]string, len(o.Backup))
	for i, a := range o.Backup {
		args[i] = strings.ReplaceAll(a, "{tmpdir}", tmp)
	}

	conn, err := dialRetry(ctx, o.To, o.DialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	stopConn := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopConn()
	if _, err := io.WriteString(conn, o.Token+"\n"); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	n, sum, copyErr := copyHashed(conn, stdout, o.Progress, em)
	if copyErr != nil {
		_ = cmd.Process.Kill() // it would block writing to a pipe nobody reads
	}
	waitErr := cmd.Wait()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
	if err := firstErr(ctx.Err(), copyErr, waitErr); err != nil {
		return fmt.Errorf("send after %d bytes: %w", n, err)
	}
	em.Emit(Event{Event: "done", SHA256: sum, Bytes: n})
	return nil
}

func dialRetry(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var d net.Dialer
	for {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect to receiver %s: %w", addr, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// copyHashed copies src to dst while computing its SHA-256, emitting progress events.
func copyHashed(dst io.Writer, src io.Reader, every time.Duration, em *Emitter) (int64, string, error) {
	h := sha256.New()
	var count atomic.Int64
	if every > 0 {
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		// Stop the reporter before returning, so no progress event follows "done".
		defer wg.Wait()
		defer close(done)
		go func() {
			defer wg.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					em.Emit(Event{Event: "progress", Bytes: count.Load()})
				}
			}
		}()
	}
	n, err := io.Copy(io.MultiWriter(dst, h, counter{&count}), src)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

type counter struct{ n *atomic.Int64 }

func (c counter) Write(p []byte) (int, error) {
	c.n.Add(int64(len(p)))
	return len(p), nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
