package reseed

import (
	"fmt"
	"strconv"
	"strings"
)

// BinlogInfo is the source's position at backup time, from xtrabackup_binlog_info.
type BinlogInfo struct {
	File    string
	Pos     uint64
	GTIDSet string // empty when GTIDs are disabled on the source
}

// ParseBinlogInfo parses xtrabackup_binlog_info, whose format is
// "<file>\t<pos>[\t<gtid set>]". A long GTID set may be split across lines.
func ParseBinlogInfo(s string) (BinlogInfo, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return BinlogInfo{}, fmt.Errorf("unexpected xtrabackup_binlog_info contents: %q", s)
	}
	pos, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return BinlogInfo{}, fmt.Errorf("bad binlog position %q: %w", fields[1], err)
	}
	return BinlogInfo{
		File:    fields[0],
		Pos:     pos,
		GTIDSet: strings.Join(fields[2:], ""),
	}, nil
}
