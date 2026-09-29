package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// CheckWipeDir refuses directories that are unsafe to empty: relative paths and anything
// fewer than two levels below / (such as "/", "/db" or "/var").
func CheckWipeDir(dir string) error {
	if !path.IsAbs(dir) {
		return fmt.Errorf("%q must be an absolute path", dir)
	}
	if strings.Count(path.Clean(dir), "/") < 2 {
		return fmt.Errorf("%q is too close to / to be wiped safely", dir)
	}
	return nil
}

// MissingTools returns the commands that aren't on PATH.
func MissingTools(tools []string) []string {
	var missing []string
	for _, t := range tools {
		if _, err := exec.LookPath(t); err != nil {
			missing = append(missing, t)
		}
	}
	return missing
}

// DirSize returns the total size of the regular files under dir; 0 if dir doesn't exist.
func DirSize(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// Wipe empties each directory, creating it if needed. It refuses unsafe paths, and refuses
// outright if a mysqld process is running inside any of them (mysqld's working directory is
// its datadir). procRoot is normally /proc.
func Wipe(dirs []string, procRoot string) error {
	if len(dirs) == 0 {
		return errors.New("no directories given")
	}
	for _, dir := range dirs {
		if err := CheckWipeDir(dir); err != nil {
			return err
		}
		if pid, ok := mysqldIn(procRoot, dir); ok {
			return fmt.Errorf("mysqld (pid %d) is running in %s; refusing to wipe", pid, dir)
		}
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// mysqldIn reports a mysqld process whose working directory is dir or inside it.
func mysqldIn(procRoot, dir string) (int, bool) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return 0, false // no /proc (e.g. macOS): nothing to check
	}
	want := resolve(dir)
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "comm"))
		if err != nil || strings.TrimSpace(string(comm)) != "mysqld" {
			continue
		}
		cwd, err := os.Readlink(filepath.Join(procRoot, e.Name(), "cwd"))
		if err != nil {
			continue
		}
		if cwd = resolve(cwd); cwd == want || strings.HasPrefix(cwd, want+"/") {
			return pid, true
		}
	}
	return 0, false
}

func resolve(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return filepath.Clean(p)
}

// MoveRedo moves InnoDB redo logs from datadir to logdir: #innodb_redo (MySQL 8.0.30+) and
// ib_logfile* (earlier 8.0). It works across filesystems, and never overwrites.
func MoveRedo(datadir, logdir string) ([]string, error) {
	if err := os.MkdirAll(logdir, 0o750); err != nil {
		return nil, err
	}
	names, err := filepath.Glob(filepath.Join(datadir, "ib_logfile*"))
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(filepath.Join(datadir, "#innodb_redo")); err == nil {
		names = append(names, filepath.Join(datadir, "#innodb_redo"))
	}
	var moved []string
	for _, src := range names {
		dst := filepath.Join(logdir, filepath.Base(src))
		if _, err := os.Lstat(dst); err == nil {
			return moved, fmt.Errorf("%s already exists; refusing to overwrite", dst)
		}
		if err := move(src, dst); err != nil {
			return moved, err
		}
		moved = append(moved, filepath.Base(src))
	}
	return moved, nil
}

// rename is os.Rename, replaceable in tests to simulate a cross-filesystem move.
var rename = os.Rename

func move(src, dst string) error {
	err := rename(src, dst)
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	if err := copyTree(src, dst); err != nil {
		_ = os.RemoveAll(dst)
		return fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}
	return os.RemoveAll(src)
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.MkdirAll(target, info.Mode().Perm())
		case d.Type()&fs.ModeSymlink != 0:
			link, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		case d.Type().IsRegular():
			return copyFile(p, target, info.Mode().Perm())
		default:
			return fmt.Errorf("%s: unsupported file type %s", p, d.Type())
		}
	})
}

func copyFile(src, dst string, perm fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Finalize prepares a restored datadir for mysqld: removes auto.cnf so the replica gets its
// own server_uuid, gives everything to owner, and sets directories to 0750 and files to
// 0640. If restorecon is installed it also restores SELinux labels.
func Finalize(ctx context.Context, datadir, logdir, owner string) (string, error) {
	if err := os.Remove(filepath.Join(datadir, "auto.cnf")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	uid, gid, err := lookupOwner(owner)
	if err != nil {
		return "", err
	}
	dirs := []string{datadir}
	if logdir != "" && logdir != datadir {
		dirs = append(dirs, logdir)
	}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := os.Lchown(p, uid, gid); err != nil {
				return err
			}
			switch {
			case d.IsDir():
				return os.Chmod(p, 0o750)
			case d.Type().IsRegular():
				return os.Chmod(p, 0o640)
			}
			return nil // leave symlinks alone
		})
		if err != nil {
			return "", err
		}
	}

	restorecon, err := exec.LookPath("restorecon")
	if err != nil {
		return "restorecon not installed; SELinux labels not restored", nil
	}
	if out, err := exec.CommandContext(ctx, restorecon, append([]string{"-R"}, dirs...)...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("restorecon: %w: %s", err, out)
	}
	return "SELinux labels restored", nil
}

func lookupOwner(name string) (int, int, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return 0, 0, fmt.Errorf("look up user %q: %w", name, err)
	}
	gidStr := u.Gid
	if g, err := user.LookupGroup(name); err == nil {
		gidStr = g.Gid
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return 0, 0, err
	}
	gid, err := strconv.Atoi(gidStr)
	if err != nil {
		return 0, 0, err
	}
	return uid, gid, nil
}

// CheckBackup verifies that dir holds a complete, not yet prepared full backup.
func CheckBackup(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "xtrabackup_checkpoints"))
	if err != nil {
		return fmt.Errorf("backup is incomplete: %w", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == "backup_type" {
			if t := strings.TrimSpace(v); t != "full-backuped" {
				return fmt.Errorf("backup_type is %q, want full-backuped", t)
			}
			return nil
		}
	}
	return errors.New("xtrabackup_checkpoints has no backup_type")
}
