package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type hourlyTask struct {
	Pair          DatabasePair
	Table         string
	TimeField     string
	Start         string
	TimeZone      string
	LastError     string
	LastSuccessAt string
}

func (s *syncState) checkManagedHourlySettings() error {
	var value string
	err := s.db.QueryRow("SELECT value FROM metadata WHERE key='hourly_start'").Scan(&value)
	if err == nil {
		return fmt.Errorf("state file belongs to static hourly-window mode; use a new state file for managed tasks")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return persistenceError(err)
	}
	err = s.db.QueryRow("SELECT value FROM metadata WHERE key='hourly_managed'").Scan(&value)
	if err == nil && value != "1" {
		return fmt.Errorf("invalid hourly_managed state setting %q", value)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return persistenceError(err)
	}
	_, err = s.db.Exec("INSERT OR IGNORE INTO metadata(key,value) VALUES('hourly_managed','1')")
	return persistenceError(err)
}

func (s *syncState) addHourlyTask(task hourlyTask) error {
	if task.Pair.Source == "" || task.Pair.Target == "" || task.Table == "" || task.TimeField == "" {
		return fmt.Errorf("source-db, target-db, table, and time-field are required")
	}
	if strings.TrimSpace(task.Pair.Source) != task.Pair.Source || strings.TrimSpace(task.Pair.Target) != task.Pair.Target || strings.TrimSpace(task.Table) != task.Table || strings.TrimSpace(task.TimeField) != task.TimeField {
		return fmt.Errorf("task identifiers must not have surrounding whitespace")
	}
	if _, err := parseHourlyStart(task.Start, task.TimeZone); err != nil {
		return err
	}
	if s.Mode != "" && s.Mode != "hourly-window" {
		return fmt.Errorf("state file uses mode %q; managed tasks require a new state file", s.Mode)
	}
	if err := s.checkManagedHourlySettings(); err != nil {
		return err
	}
	var exists int
	err := s.db.QueryRow("SELECT 1 FROM full_table_task WHERE source_db=? AND target_db=? AND table_name=?", task.Pair.Source, task.Pair.Target, task.Table).Scan(&exists)
	if err == nil {
		return fmt.Errorf("table already has a full-table task")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return persistenceError(err)
	}
	s.Mode, s.TimeField = "hourly-window", ""
	if err := s.saveMetadata(); err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO hourly_task(source_db,target_db,table_name,time_field,start_time,time_zone,created_at)
		VALUES(?,?,?,?,?,?,?)`, task.Pair.Source, task.Pair.Target, task.Table, task.TimeField, task.Start, task.TimeZone, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("add task %s.%s: %w", task.Pair.Source, task.Table, err)
	}
	return nil
}

func (s *syncState) listHourlyTasks() ([]hourlyTask, error) {
	rows, err := s.db.Query(`SELECT source_db,target_db,table_name,time_field,start_time,time_zone,last_error,last_success_at
		FROM hourly_task ORDER BY source_db,target_db,table_name`)
	if err != nil {
		return nil, persistenceError(err)
	}
	defer rows.Close()
	var tasks []hourlyTask
	for rows.Next() {
		var task hourlyTask
		if err := rows.Scan(&task.Pair.Source, &task.Pair.Target, &task.Table, &task.TimeField, &task.Start, &task.TimeZone, &task.LastError, &task.LastSuccessAt); err != nil {
			return nil, persistenceError(err)
		}
		tasks = append(tasks, task)
	}
	return tasks, persistenceError(rows.Err())
}

func (s *syncState) saveHourlyTaskResult(task hourlyTask, completed int, runErr error) error {
	message := ""
	if runErr != nil {
		message = runErr.Error()
	}
	_, err := s.db.Exec("UPDATE hourly_task SET last_error=?,last_success_at=CASE WHEN ?>0 THEN ? ELSE last_success_at END WHERE source_db=? AND target_db=? AND table_name=?",
		message, completed, time.Now().UTC().Format(time.RFC3339Nano), task.Pair.Source, task.Pair.Target, task.Table)
	return persistenceError(err)
}
