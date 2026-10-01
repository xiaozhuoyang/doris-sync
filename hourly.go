package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const hourlyLayout = "2006-01-02 15:04:05"

type hourlyPending struct {
	From          string   `json:"from"`
	To            string   `json:"to"`
	Prefix        string   `json:"prefix"`
	Objects       []string `json:"objects,omitempty"`
	BackupReady   bool     `json:"backupReady"`
	ImportStarted bool     `json:"importStarted"`
	Label         string   `json:"label"`
	Attempt       int      `json:"attempt"`
}

type hourlyState struct {
	Next    string         `json:"next"`
	Pending *hourlyPending `json:"pending,omitempty"`
}

func parseHourlyStart(value, zone string) (time.Time, error) {
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return time.Time{}, err
	}
	start, err := time.ParseInLocation(hourlyLayout, value, loc)
	if err != nil || start.Format(hourlyLayout) != value || start.Minute() != 0 || start.Second() != 0 {
		return time.Time{}, fmt.Errorf("hourlyStart/--window-start must be an exact hour in %s, e.g. 2026-09-29 08:00:00", zone)
	}
	return start, nil
}

func validateHourlyTimeField(columns []column, name string) error {
	if err := validateTimeField(columns, name); err != nil {
		return err
	}
	for _, col := range columns {
		if strings.EqualFold(col.Name, name) && strings.HasPrefix(strings.ToUpper(col.Type), "DATETIME") {
			return nil
		}
	}
	return fmt.Errorf("hourly-window time field %q must be DATETIME", name)
}

func hourlyCutoff(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location()).Add(-time.Hour)
}

func (s *syncState) checkHourlySettings(start, zone string) error {
	var managed string
	err := s.db.QueryRow("SELECT value FROM metadata WHERE key='hourly_managed'").Scan(&managed)
	if err == nil {
		return fmt.Errorf("state file belongs to managed hourly tasks; use a new state file for static hourly-window mode")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return persistenceError(err)
	}
	for key, value := range map[string]string{"hourly_start": start, "time_zone": zone} {
		var existing string
		err := s.db.QueryRow("SELECT value FROM metadata WHERE key=?", key).Scan(&existing)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return persistenceError(err)
		}
		if err == nil && existing != value {
			return fmt.Errorf("state file uses %s=%q, cannot restart with %q", key, existing, value)
		}
		if _, err := s.db.Exec("INSERT OR IGNORE INTO metadata(key,value) VALUES(?,?)", key, value); err != nil {
			return persistenceError(err)
		}
	}
	return nil
}

func (s *syncState) loadHourly(pair DatabasePair, table, start string) (*hourlyState, error) {
	var raw string
	err := s.db.QueryRow("SELECT state_json FROM hourly_state WHERE source_db=? AND target_db=? AND table_name=?", pair.Source, pair.Target, table).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		entry := &hourlyState{Next: start}
		return entry, s.saveHourly(pair, table, entry)
	}
	if err != nil {
		return nil, persistenceError(err)
	}
	var entry hourlyState
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return nil, fmt.Errorf("hourly state %s.%s: %w", pair.Source, table, err)
	}
	if entry.Next == "" || (entry.Pending != nil && entry.Pending.From != entry.Next) {
		return nil, fmt.Errorf("invalid hourly checkpoint for %s.%s", pair.Source, table)
	}
	return &entry, nil
}

func (s *syncState) saveHourly(pair DatabasePair, table string, entry *hourlyState) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO hourly_state(source_db,target_db,table_name,state_json) VALUES(?,?,?,?)
		ON CONFLICT(source_db,target_db,table_name) DO UPDATE SET state_json=excluded.state_json`, pair.Source, pair.Target, table, string(raw))
	return persistenceError(err)
}

func (s *syncer) cycleHourly(ctx context.Context) error {
	if s.options.ManagedTasks {
		return s.cycleManagedHourly(ctx)
	}
	s.logger.Printf("CYCLE_START sync_mode=hourly-window")
	var failures []error
	var processed int
	for _, pair := range s.options.Databases {
		if err := s.target.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(pair.Target)); err != nil {
			return err
		}
		tables, err := s.source.tables(ctx, pair.Source)
		if err != nil {
			return err
		}
		targetNames, err := s.target.tables(ctx, pair.Target)
		if err != nil {
			return err
		}
		targetTables := make(map[string]bool, len(targetNames))
		for _, table := range targetNames {
			targetTables[table] = true
		}
		for _, table := range tables {
			if !s.options.allowsTable(table) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			ready, err := s.ensureTargetTable(ctx, pair, table, targetTables, &stats{})
			if err == nil && ready {
				var count int
				count, err = s.syncHourlyTable(ctx, hourlyTask{Pair: pair, Table: table, TimeField: s.state.TimeField, Start: s.options.HourlyStart, TimeZone: s.options.TimeZone})
				processed += count
			}
			if err != nil {
				if errors.Is(err, errStatePersistence) {
					return err
				}
				failures = append(failures, fmt.Errorf("%s.%s: %w", pair.Source, table, err))
				s.logger.Printf("TABLE_FAILED database=%s table=%s error=%q", pair.Source, table, err)
			}
		}
	}
	s.logger.Printf("HOURLY_SUMMARY windows_completed=%d failed_tables=%d", processed, len(failures))
	return errors.Join(failures...)
}

func (s *syncer) cycleManagedHourly(ctx context.Context) error {
	s.logger.Printf("CYCLE_START sync_mode=hourly-window managed_tasks=true")
	tasks, err := s.state.listHourlyTasks()
	if err != nil {
		return err
	}
	allowed := make(map[string]bool, len(s.options.Databases))
	for _, pair := range s.options.Databases {
		allowed[stateKey(pair.Source, pair.Target, "", "")] = true
	}
	targetTables := make(map[string]map[string]bool)
	var failures []error
	var processed int
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		var count int
		var taskErr error
		if !allowed[stateKey(task.Pair.Source, task.Pair.Target, "", "")] {
			taskErr = fmt.Errorf("database mapping %s -> %s is not configured", task.Pair.Source, task.Pair.Target)
		} else {
			byName := targetTables[task.Pair.Target]
			if byName == nil {
				if taskErr = s.target.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(task.Pair.Target)); taskErr == nil {
					var names []string
					names, taskErr = s.target.tables(ctx, task.Pair.Target)
					if taskErr == nil {
						byName = make(map[string]bool, len(names))
						for _, name := range names {
							byName[name] = true
						}
						targetTables[task.Pair.Target] = byName
					}
				}
			}
			if taskErr == nil {
				var ready bool
				ready, taskErr = s.ensureTargetTable(ctx, task.Pair, task.Table, byName, &stats{})
				if taskErr == nil && !ready {
					taskErr = fmt.Errorf("source table is not a supported OLAP table")
				}
			}
			if taskErr == nil {
				count, taskErr = s.syncHourlyTable(ctx, task)
			}
		}
		if err := s.state.saveHourlyTaskResult(task, count, taskErr); err != nil {
			return err
		}
		processed += count
		if taskErr != nil {
			if errors.Is(taskErr, errStatePersistence) {
				return taskErr
			}
			failures = append(failures, fmt.Errorf("%s.%s: %w", task.Pair.Source, task.Table, taskErr))
			s.logger.Printf("TASK_FAILED database=%s table=%s error=%q", task.Pair.Source, task.Table, taskErr)
		}
	}
	fullTasks, err := s.state.listFullTableTasks()
	if err != nil {
		return err
	}
	fullCompleted := 0
	for _, task := range fullTasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		due, taskErr := fullTableDue(task, time.Now())
		if taskErr == nil && !due {
			continue
		}
		if taskErr == nil && !allowed[stateKey(task.Pair.Source, task.Pair.Target, "", "")] {
			taskErr = fmt.Errorf("database mapping %s -> %s is not configured", task.Pair.Source, task.Pair.Target)
		}
		if taskErr == nil {
			byName := targetTables[task.Pair.Target]
			if byName == nil {
				if taskErr = s.target.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(task.Pair.Target)); taskErr == nil {
					var names []string
					names, taskErr = s.target.tables(ctx, task.Pair.Target)
					if taskErr == nil {
						byName = make(map[string]bool, len(names))
						for _, name := range names {
							byName[name] = true
						}
						targetTables[task.Pair.Target] = byName
					}
				}
			}
			if taskErr == nil {
				taskErr = s.state.updateFullTableTask(task, "backing_up", nil, false)
			}
			if taskErr == nil {
				taskErr = s.syncFullTable(ctx, task, byName)
			}
		}
		if errors.Is(taskErr, errStatePersistence) {
			return taskErr
		}
		phase := "waiting"
		if taskErr != nil {
			phase = "error"
		}
		if err := s.state.updateFullTableTask(task, phase, taskErr, taskErr == nil); err != nil {
			return err
		}
		if taskErr != nil {
			failures = append(failures, fmt.Errorf("%s.%s: %w", task.Pair.Source, task.Table, taskErr))
			s.logger.Printf("TASK_FAILED database=%s table=%s error=%q", task.Pair.Source, task.Table, taskErr)
		} else {
			fullCompleted++
		}
	}
	s.logger.Printf("HOURLY_SUMMARY tasks=%d full_table_tasks=%d windows_completed=%d full_table_completed=%d failed_tasks=%d", len(tasks), len(fullTasks), processed, fullCompleted, len(failures))
	return errors.Join(failures...)
}

func (s *syncer) syncHourlyTable(ctx context.Context, task hourlyTask) (int, error) {
	pair, table := task.Pair, task.Table
	cols, err := s.source.columns(ctx, pair.Source, table)
	if err != nil {
		return 0, err
	}
	if err := validateHourlyTimeField(cols, task.TimeField); err != nil {
		return 0, err
	}
	targetCols, err := s.target.columns(ctx, pair.Target, table)
	if err != nil {
		return 0, err
	}
	if err := validateTargetColumns(cols, targetCols); err != nil {
		return 0, err
	}
	columnJSON, _ := json.Marshal(cols)
	hash := sha256.Sum256(columnJSON)
	schemaHash := hex.EncodeToString(hash[:])
	key := stateKey(pair.Source, pair.Target, table, "")
	if previous := s.state.Tables[key]; previous != "" && previous != schemaHash {
		return 0, fmt.Errorf("source schema changed; migrate target table before continuing")
	}
	s.state.Tables[key] = schemaHash
	if err := s.state.saveTable(key); err != nil {
		return 0, err
	}
	entry, err := s.state.loadHourly(pair, table, task.Start)
	if err != nil {
		return 0, err
	}
	loc, _ := time.LoadLocation(task.TimeZone)
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = col.Name
	}
	completed := 0
	for {
		if err := ctx.Err(); err != nil {
			return completed, err
		}
		start, err := parseHourlyStart(entry.Next, task.TimeZone)
		if err != nil {
			return completed, err
		}
		end := start.Add(time.Hour)
		if end.Format(hourlyLayout) <= start.Format(hourlyLayout) {
			return completed, fmt.Errorf("timeZone %s has a repeated hour; use a fixed-offset zone for hourly-window mode", task.TimeZone)
		}
		if end.After(hourlyCutoff(time.Now().In(loc))) {
			return completed, nil
		}
		if err := s.syncHourlyWindow(ctx, task, columns, entry, start, end); err != nil {
			return completed, err
		}
		completed++
	}
}

func hourlyWhere(field string, start, end time.Time) string {
	return " WHERE " + ident(field) + " >= " + sqlLiteral(start.Format(hourlyLayout)) + " AND " + ident(field) + " < " + sqlLiteral(end.Format(hourlyLayout))
}

func (s *syncer) syncHourlyWindow(ctx context.Context, task hourlyTask, columns []string, entry *hourlyState, start, end time.Time) error {
	pair, table := task.Pair, task.Table
	from, to := start.Format(hourlyLayout), end.Format(hourlyLayout)
	if entry.Pending == nil {
		entry.Pending = &hourlyPending{From: from, To: to}
		if err := s.state.saveHourly(pair, table, entry); err != nil {
			return err
		}
	}
	p := entry.Pending
	if p.From != from || p.To != to {
		return fmt.Errorf("pending hourly window %s to %s does not match cursor %s to %s", p.From, p.To, from, to)
	}
	if !p.BackupReady {
		prefix, err := backupGenerationPrefix(s.store.partitionPrefix(pair.Source, pair.Target, table, "hourly_"+start.UTC().Format("20060102T150405Z")))
		if err != nil {
			return err
		}
		p.Prefix, p.Objects = prefix, nil
		p.Label = deltaLabel(stateKey(pair.Source, pair.Target, table, "hourly"), from, to, prefix, 0)
		if err := s.state.saveHourly(pair, table, entry); err != nil {
			return err
		}
		where := hourlyWhere(task.TimeField, start, end)
		probe, err := s.source.query(ctx, "SELECT 1 FROM "+qtable(pair.Source, table)+where+" LIMIT 1")
		if err != nil {
			return err
		}
		if len(probe) == 0 {
			s.logger.Printf("HOURLY_EMPTY database=%s table=%s from=%s to=%s", pair.Source, table, from, to)
			return s.finishHourly(pair, table, entry)
		}
		s.logger.Printf("HOURLY_BACKUP_START database=%s table=%s from=%s to=%s uri=%s", pair.Source, table, from, to, s.store.uri(p.Prefix))
		if _, err := s.source.query(ctx, outfileTableWhereSQL(pair.Source, table, s.store.uri(p.Prefix), columns, where, s.options.S3)); err != nil {
			return fmt.Errorf("hourly OUTFILE: %w", err)
		}
		p.Objects, err = s.store.list(ctx, p.Prefix)
		if err != nil {
			return err
		}
		if len(p.Objects) == 0 {
			return fmt.Errorf("hourly OUTFILE returned no files for nonempty window")
		}
		p.BackupReady = true
		if err := s.state.saveHourly(pair, table, entry); err != nil {
			return err
		}
	}
	if p.Prefix == "" || p.Label == "" || len(p.Objects) == 0 {
		return fmt.Errorf("hourly backup checkpoint is incomplete for %s to %s", from, to)
	}
	actual, err := s.store.list(ctx, p.Prefix)
	if err != nil {
		return err
	}
	if !equalStrings(actual, p.Objects) {
		return fmt.Errorf("hourly backup files changed for %s to %s", from, to)
	}
	if p.ImportStarted {
		// A prior process may have committed before it could save the checkpoint.
		return fmt.Errorf("hourly import %s outcome is unknown after interruption; verify target rows for %s to %s before retrying", p.Label, from, to)
	}
	p.ImportStarted = true
	if err := s.state.saveHourly(pair, table, entry); err != nil {
		return err
	}
	s.logger.Printf("HOURLY_IMPORT_START database=%s table=%s from=%s to=%s files=%d label=%s", pair.Target, table, from, to, len(p.Objects), p.Label)
	if err := s.target.exec(ctx, importLabeledSQL(pair.Target, table, s.store.uri(p.Prefix)+"*.parquet", columns, p.Label, s.options.S3)); err != nil {
		return fmt.Errorf("hourly import %s returned an error; outcome must be verified before retrying: %w", p.Label, err)
	}
	return s.finishHourly(pair, table, entry)
}

func (s *syncer) finishHourly(pair DatabasePair, table string, entry *hourlyState) error {
	p := entry.Pending
	if p == nil || p.From != entry.Next {
		return fmt.Errorf("hourly checkpoint has no matching pending window")
	}
	completed := *entry
	completed.Next, completed.Pending = p.To, nil
	if err := s.state.saveCompletedHourly(pair, table, &completed); err != nil {
		return err
	}
	*entry = completed
	s.logger.Printf("HOURLY_DONE database=%s table=%s next=%s", pair.Source, table, entry.Next)
	return nil
}

func (s *syncState) saveCompletedHourly(pair DatabasePair, table string, entry *hourlyState) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return persistenceError(err)
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO hourly_state(source_db,target_db,table_name,state_json) VALUES(?,?,?,?)
		ON CONFLICT(source_db,target_db,table_name) DO UPDATE SET state_json=excluded.state_json`, pair.Source, pair.Target, table, string(raw))
	if err != nil {
		return persistenceError(err)
	}
	_, err = tx.Exec(`UPDATE hourly_task SET last_error='',last_success_at=?
		WHERE source_db=? AND target_db=? AND table_name=?`, time.Now().UTC().Format(time.RFC3339Nano), pair.Source, pair.Target, table)
	if err != nil {
		return persistenceError(err)
	}
	return persistenceError(tx.Commit())
}
