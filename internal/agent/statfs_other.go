//go:build !unix

package agent

import "errors"

// Avail is only implemented on Unix; the agent only runs on Linux database hosts.
func Avail(string) (int64, error) {
	return 0, errors.New("free-space check is not supported on this platform")
}
