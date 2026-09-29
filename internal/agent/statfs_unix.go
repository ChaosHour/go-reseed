//go:build unix

package agent

import "golang.org/x/sys/unix"

// Avail returns the bytes available to unprivileged users on dir's filesystem.
func Avail(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
