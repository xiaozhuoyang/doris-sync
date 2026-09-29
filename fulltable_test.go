package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFullTableTaskRegistrationAndStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	var output bytes.Buffer
	args := []string{"add", "--mode", "full-table", "--interval", "1h", "--state-file", path,
		"--source-db", "src", "--target-db", "dst", "--table", "small"}
	if err := runTaskCommand(args, &output); err != nil {
		t.Fatal(err)
	}
	if err := runTaskCommand(args, &output); err == nil {
		t.Fatal("duplicate full-table task was accepted")
	}
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	if err := state.addHourlyTask(hourlyTask{Pair: DatabasePair{Source: "src", Target: "dst"}, Table: "small", TimeField: "event_time", Start: "2026-09-29 08:00:00", TimeZone: "UTC"}); err == nil {
		t.Fatal("same table accepted in two task modes")
	}
	tasks, err := state.listFullTableTasks()
	if err != nil || len(tasks) != 1 || tasks[0].Interval != time.Hour {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	output.Reset()
	if err := runTaskCommand([]string{"status", "--state-file", path, "--source-db", "src", "--target-db", "dst", "--table", "small"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"mode": "full-table"`) || !strings.Contains(output.String(), `"phase": "queued"`) {
		t.Fatalf("unexpected status: %s", output.String())
	}
	if err := state.updateFullTableTask(tasks[0], "waiting", nil, true); err != nil {
		t.Fatal(err)
	}
	tasks, err = state.listFullTableTasks()
	if err != nil {
		t.Fatal(err)
	}
	due, err := fullTableDue(tasks[0], time.Now())
	if err != nil || due {
		t.Fatalf("task should not run before its interval: due=%t err=%v", due, err)
	}
	due, err = fullTableDue(tasks[0], time.Now().Add(2*time.Hour))
	if err != nil || !due {
		t.Fatalf("task should run after its interval: due=%t err=%v", due, err)
	}
	output.Reset()
	if err := runTaskCommand([]string{"status", "--state-file", path, "--source-db", "src", "--target-db", "dst", "--table", "small"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"nextDueAt"`) || !strings.Contains(output.String(), `"phase": "waiting"`) {
		t.Fatalf("unexpected completed status: %s", output.String())
	}
}

func TestWholeTableOverwriteSQL(t *testing.T) {
	cfg := S3Options{Region: "cn-shanghai", MaxFileSize: "1024MB"}
	stmt := overwriteTableSQL("dst", "small", "s3://bucket/path/*.parquet", []string{"id", "value"}, false, cfg)
	if !strings.Contains(stmt, "INSERT OVERWRITE TABLE `dst`.`small` (`id`, `value`) SELECT `id`, `value` FROM s3(") || strings.Contains(stmt, "PARTITION(") {
		t.Fatalf("incorrect whole-table overwrite: %s", stmt)
	}
	empty := overwriteTableSQL("dst", "small", "", []string{"id", "value"}, true, cfg)
	if !strings.Contains(empty, "FROM `dst`.`small` WHERE 1 = 0") || strings.Contains(empty, "FROM s3(") {
		t.Fatalf("incorrect empty overwrite: %s", empty)
	}
}
