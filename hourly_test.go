package main

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHourlyBoundsAndSQL(t *testing.T) {
	if err := validateHourlyTimeField([]column{{Name: "event_time", Type: "DATEV2"}}, "event_time"); err == nil {
		t.Fatal("DATE accepted for hourly windows")
	}
	start, err := parseHourlyStart("2026-09-29 08:00:00", "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseHourlyStart("2026-09-29 08:30:00", "Asia/Shanghai"); err == nil {
		t.Fatal("unaligned start accepted")
	}
	if _, err := parseHourlyStart("2026-09-29 08:00:00", "bad/zone"); err == nil {
		t.Fatal("invalid zone accepted")
	}
	end := start.Add(time.Hour)
	cutoff := hourlyCutoff(time.Date(2026, 9, 29, 10, 42, 0, 0, start.Location()))
	if !end.Equal(cutoff) || start.Add(2*time.Hour).Before(cutoff) {
		t.Fatalf("unexpected eligibility cutoff %s", cutoff)
	}
	india, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	if got := hourlyCutoff(time.Date(2026, 9, 29, 10, 42, 0, 0, india)); got.Format(hourlyLayout) != "2026-09-29 09:00:00" {
		t.Fatalf("half-hour timezone cutoff is %s", got)
	}
	where := hourlyWhere("event_time", start, end)
	if where != " WHERE `event_time` >= '2026-09-29 08:00:00' AND `event_time` < '2026-09-29 09:00:00'" {
		t.Fatalf("wrong half-open window: %s", where)
	}
	statement := outfileTableWhereSQL("src", "events", "s3://bucket/window/", []string{"event_time", "id"}, where, S3Options{Region: "cn-shanghai", MaxFileSize: "1024MB"})
	if !strings.Contains(statement, "FROM `src`.`events` WHERE") || strings.Contains(statement, "PARTITION(") {
		t.Fatalf("hourly OUTFILE incorrectly restricted to one partition: %s", statement)
	}
}

func TestHourlyCheckpointContinuity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	state.Mode, state.TimeField = "hourly-window", "event_time"
	if err := state.checkHourlySettings("2026-09-29 08:00:00", "Asia/Shanghai"); err != nil {
		t.Fatal(err)
	}
	if err := state.saveMetadata(); err != nil {
		t.Fatal(err)
	}
	pair := DatabasePair{Source: "src", Target: "dst"}
	entry, err := state.loadHourly(pair, "events", "2026-09-29 08:00:00")
	if err != nil {
		t.Fatal(err)
	}
	entry.Pending = &hourlyPending{From: entry.Next, To: "2026-09-29 09:00:00", Label: "ps_first", ImportStarted: true}
	if err := state.saveHourly(pair, "events", entry); err != nil {
		t.Fatal(err)
	}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}
	state, err = openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	if err := state.checkHourlySettings("2026-09-29 07:00:00", "Asia/Shanghai"); err == nil {
		t.Fatal("changed start accepted for existing state")
	}
	entry, err = state.loadHourly(pair, "events", "2026-09-29 08:00:00")
	if err != nil || entry.Next != "2026-09-29 08:00:00" || entry.Pending.Label != "ps_first" {
		t.Fatalf("pending window lost after restart: %+v, %v", entry, err)
	}
	s := &syncer{state: state, logger: log.New(io.Discard, "", 0)}
	if err := s.finishHourly(pair, "events", entry); err != nil {
		t.Fatal(err)
	}
	if entry.Next != "2026-09-29 09:00:00" || entry.Pending != nil {
		t.Fatalf("checkpoint did not advance exactly one hour: %+v", entry)
	}
	if err := s.finishHourly(pair, "events", entry); err == nil {
		t.Fatal("checkpoint advanced without pending window")
	}
	report, err := readStatus(path)
	if err != nil || report.Mode != "hourly-window" || len(report.HourlyWindows) != 1 || report.HourlyWindows[0].NextStart != "2026-09-29 09:00:00" {
		t.Fatalf("wrong status: %+v, %v", report, err)
	}
}

func TestManagedHourlyCompletionClearsStaleError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	pair := DatabasePair{Source: "src", Target: "dst"}
	task := hourlyTask{Pair: pair, Table: "events", TimeField: "event_time", Start: "2026-09-29 08:00:00", TimeZone: "UTC"}
	if err := state.addHourlyTask(task); err != nil {
		t.Fatal(err)
	}
	entry, err := state.loadHourly(pair, task.Table, task.Start)
	if err != nil {
		t.Fatal(err)
	}
	entry.Pending = &hourlyPending{From: task.Start, To: "2026-09-29 09:00:00"}
	if err := state.saveHourly(pair, task.Table, entry); err != nil {
		t.Fatal(err)
	}
	if err := state.saveHourlyTaskResult(task, 0, fmt.Errorf("previous failure")); err != nil {
		t.Fatal(err)
	}
	s := &syncer{state: state, logger: log.New(io.Discard, "", 0)}
	if err := s.finishHourly(pair, task.Table, entry); err != nil {
		t.Fatal(err)
	}
	entry.Pending = &hourlyPending{From: entry.Next, To: "2026-09-29 10:00:00", BackupReady: true}
	if err := state.saveHourly(pair, task.Table, entry); err != nil {
		t.Fatal(err)
	}
	report, err := readStatus(path)
	if err != nil {
		t.Fatal(err)
	}
	got := report.HourlyWindows[0]
	if got.Phase != "importing" || got.LastError != "" || got.LastSuccessAt == "" || got.NextStart != "2026-09-29 09:00:00" {
		t.Fatalf("stale task status after completed window: %+v", got)
	}
}
