package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOverwritePlanValidation(t *testing.T) {
	mappings := []DatabasePair{{Source: "src", Target: "dst"}, {Source: "other", Target: "other_dst"}, {Source: "other", Target: "dst"}}
	valid := overwritePlan{Tables: []overwriteTable{
		{SourceDatabase: "src", TargetDatabase: "dst", Table: "events", Partitions: []string{"p1", "p2"}},
		{SourceDatabase: "other", TargetDatabase: "other_dst", Table: "orders", Partitions: []string{"p3"}},
	}}
	if err := validateOverwritePlan(valid, mappings); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		plan overwritePlan
	}{
		{"empty", overwritePlan{}},
		{"missing mapping", overwritePlan{Tables: []overwriteTable{{SourceDatabase: "src", TargetDatabase: "wrong", Table: "events", Partitions: []string{"p1"}}}}},
		{"missing partitions", overwritePlan{Tables: []overwriteTable{{SourceDatabase: "src", TargetDatabase: "dst", Table: "events"}}}},
		{"duplicate partition", overwritePlan{Tables: []overwriteTable{{SourceDatabase: "src", TargetDatabase: "dst", Table: "events", Partitions: []string{"p1", "p1"}}}}},
		{"duplicate table", overwritePlan{Tables: []overwriteTable{valid.Tables[0], valid.Tables[0]}}},
		{"duplicate target", overwritePlan{Tables: []overwriteTable{valid.Tables[0], {SourceDatabase: "other", TargetDatabase: "dst", Table: "events", Partitions: []string{"p3"}}}}},
		{"whitespace table", overwritePlan{Tables: []overwriteTable{{SourceDatabase: "src", TargetDatabase: "dst", Table: " events", Partitions: []string{"p1"}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateOverwritePlan(tc.plan, mappings); err == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
}

func TestLoadOverwritePlanRejectsUnknownAndTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	mappings := []DatabasePair{{Source: "src", Target: "dst"}}
	valid := `{"tables":[{"sourceDatabase":"src","targetDatabase":"dst","table":"events","partitions":["p1"]}]}`
	for _, tc := range []struct {
		name string
		data string
		want string
	}{
		{"valid", valid, ""},
		{"unknown", strings.Replace(valid, `"table":"events"`, `"table":"events","typo":1`, 1), "unknown field"},
		{"trailing", valid + ` {}`, "exactly one JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.data), 0o600); err != nil {
				t.Fatal(err)
			}
			plan, err := loadOverwritePlan(path, mappings)
			if tc.want == "" && (err != nil || len(plan.Tables) != 1) {
				t.Fatalf("unexpected plan: %+v, %v", plan, err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestAutoPartitionTargetMayBeInitiallyMissing(t *testing.T) {
	source := partition{Name: "p_source", Range: "[types: [DATETIMEV2]; keys: [2026-09-01 00:00:00]; ..types: [DATETIMEV2]; keys: [2026-09-01 01:00:00])"}
	target, found, err := matchTargetPartition(source, nil)
	if err != nil || found || target.Name != "" {
		t.Fatalf("unexpected missing target match: %+v %v %v", target, found, err)
	}
	if !isAutoPartitionDDL("CREATE TABLE x (dt DATETIME) AUTO PARTITION BY RANGE (date_trunc(dt, 'hour'))()") {
		t.Fatal("AUTO PARTITION target not recognized")
	}
}
