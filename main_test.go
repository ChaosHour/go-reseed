package main

import "testing"

func TestHostOnly(t *testing.T) {
	tests := map[string]string{
		"db002":          "db002",
		"db002:22":       "db002",
		"10.0.0.3:2222":  "10.0.0.3",
		"[::1]:2222":     "::1",
		"fe80::1":        "fe80::1",
		"db.example.com": "db.example.com",
	}
	for in, want := range tests {
		if got := hostOnly(in); got != want {
			t.Errorf("hostOnly(%q) = %q, want %q", in, got, want)
		}
	}
}
