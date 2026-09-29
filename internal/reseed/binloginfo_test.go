package reseed

import "testing"

func TestParseBinlogInfo(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want BinlogInfo
	}{
		{
			name: "file and position",
			in:   "mysql-bin.000042\t157\n",
			want: BinlogInfo{File: "mysql-bin.000042", Pos: 157},
		},
		{
			name: "with gtid set",
			in:   "binlog.000003\t4711\t3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5\n",
			want: BinlogInfo{File: "binlog.000003", Pos: 4711, GTIDSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"},
		},
		{
			name: "multi-line gtid set",
			in:   "binlog.000003\t4711\t3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n4a6b7c8d-71ca-11e1-9e33-c80aa9429562:1-9\n",
			want: BinlogInfo{File: "binlog.000003", Pos: 4711, GTIDSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4a6b7c8d-71ca-11e1-9e33-c80aa9429562:1-9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBinlogInfo(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}

	if _, err := ParseBinlogInfo("garbage"); err == nil {
		t.Error("expected error for malformed input")
	}
}
