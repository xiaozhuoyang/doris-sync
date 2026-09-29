package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagedTasksLiveRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	service, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer service.close()
	if err := service.checkManagedHourlySettings(); err != nil {
		t.Fatal(err)
	}
	service.Mode = "hourly-window"
	if err := service.saveMetadata(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	args := []string{"add", "--state-file", path, "--source-db", "src", "--target-db", "dst", "--table", "events", "--time-field", "event_time", "--start", "2026-09-29 08:00:00", "--time-zone", "Asia/Shanghai"}
	if err := runTaskCommand(args, &output); err != nil {
		t.Fatal(err)
	}
	if err := runTaskCommand(args, &output); err == nil {
		t.Fatal("duplicate task was accepted")
	}
	tasks, err := service.listHourlyTasks()
	if err != nil || len(tasks) != 1 || tasks[0].Table != "events" {
		t.Fatalf("running service did not see new task: %+v, %v", tasks, err)
	}
	output.Reset()
	if err := runTaskCommand([]string{"status", "--state-file", path, "--source-db", "src", "--target-db", "dst", "--table", "events"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"phase": "queued"`) || !strings.Contains(output.String(), `"nextStart": "2026-09-29 08:00:00"`) {
		t.Fatalf("new task status is wrong: %s", output.String())
	}
	entry, err := service.loadHourly(tasks[0].Pair, tasks[0].Table, tasks[0].Start)
	if err != nil {
		t.Fatal(err)
	}
	entry.Pending = &hourlyPending{From: entry.Next, To: "2026-09-29 09:00:00", Prefix: "pending/"}
	if err := service.saveHourly(tasks[0].Pair, tasks[0].Table, entry); err != nil {
		t.Fatal(err)
	}
	if err := service.saveHourlyTaskResult(tasks[0], 0, nil); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runTaskCommand([]string{"status", "--state-file", path, "--source-db", "src", "--target-db", "dst", "--table", "events"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"phase": "backing_up"`) {
		t.Fatalf("pending task status is wrong: %s", output.String())
	}
	if err := service.checkHourlySettings("2026-09-29 08:00:00", "Asia/Shanghai"); err == nil {
		t.Fatal("managed state accepted static hourly settings")
	}
}
