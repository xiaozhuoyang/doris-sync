package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFullTableComparisonOutput(t *testing.T) {
	item := statusFullTable{Mode: "full-table", Comparison: &fullTableComparison{
		SourceRows: 12, TargetRows: 10, Difference: -2,
	}}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"sourceRows":12`, `"targetRows":10`, `"difference":-2`} {
		if !strings.Contains(string(data), expected) {
			t.Fatalf("missing %s in %s", expected, data)
		}
	}
}

func TestBuildDayComparison(t *testing.T) {
	var source, target [24]int64
	source[9], source[10] = 3, 7
	target[9], target[10] = 3, 5
	got := buildDayComparison("2026-09-29", "Asia/Shanghai", source, target)
	if len(got.Hours) != 24 || got.Hours[0].Hour != "00:00" || got.Hours[23].Hour != "23:00" {
		t.Fatalf("hour coverage: %+v", got)
	}
	if got.SourceTotal != 10 || got.TargetTotal != 8 || got.Difference != -2 || got.Hours[10].Difference != -2 {
		t.Fatalf("comparison: %+v", got)
	}
}

func TestTaskStatusDateRequiresConfig(t *testing.T) {
	err := runTaskCommand([]string{"status", "--date", "2026-09-29"}, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("expected config validation, got %v", err)
	}
}

func TestCompareTaskDayRejectsDSTTransition(t *testing.T) {
	_, err := compareTaskDay(context.Background(), Endpoint{}, Endpoint{}, time.Minute,
		statusHourly{TimeZone: "America/New_York"}, "2026-03-08")
	if err == nil || !strings.Contains(err.Error(), "daylight-saving") {
		t.Fatalf("expected DST validation before connecting, got %v", err)
	}
}
