package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type fullTableTask struct {
	Pair          DatabasePair
	Table         string
	Interval      time.Duration
	Phase         string
	LastError     string
	LastSuccessAt string
}

func (s *syncState) addFullTableTask(task fullTableTask) error {
	if task.Pair.Source == "" || task.Pair.Target == "" || task.Table == "" || task.Interval < time.Minute {
		return fmt.Errorf("source-db, target-db, table, and interval of at least 1m are required")
	}
	if strings.TrimSpace(task.Pair.Source) != task.Pair.Source || strings.TrimSpace(task.Pair.Target) != task.Pair.Target || strings.TrimSpace(task.Table) != task.Table {
		return fmt.Errorf("task identifiers must not have surrounding whitespace")
	}
	if s.Mode != "" && s.Mode != "hourly-window" {
		return fmt.Errorf("state file uses mode %q; managed tasks require a new state file", s.Mode)
	}
	if err := s.checkManagedHourlySettings(); err != nil {
		return err
	}
	var exists int
	err := s.db.QueryRow("SELECT 1 FROM hourly_task WHERE source_db=? AND target_db=? AND table_name=?", task.Pair.Source, task.Pair.Target, task.Table).Scan(&exists)
	if err == nil {
		return fmt.Errorf("table already has an hourly-window task")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return persistenceError(err)
	}
	s.Mode, s.TimeField = "hourly-window", ""
	if err := s.saveMetadata(); err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO full_table_task(source_db,target_db,table_name,interval_seconds,created_at)
		VALUES(?,?,?,?,?)`, task.Pair.Source, task.Pair.Target, task.Table, int64(task.Interval/time.Second), time.Now().UTC().Format(time.RFC3339Nano))
	return persistenceError(err)
}

func (s *syncState) listFullTableTasks() ([]fullTableTask, error) {
	rows, err := s.db.Query(`SELECT source_db,target_db,table_name,interval_seconds,phase,last_error,last_success_at
		FROM full_table_task ORDER BY source_db,target_db,table_name`)
	if err != nil {
		return nil, persistenceError(err)
	}
	defer rows.Close()
	var tasks []fullTableTask
	for rows.Next() {
		var task fullTableTask
		var seconds int64
		if err := rows.Scan(&task.Pair.Source, &task.Pair.Target, &task.Table, &seconds, &task.Phase, &task.LastError, &task.LastSuccessAt); err != nil {
			return nil, persistenceError(err)
		}
		task.Interval = time.Duration(seconds) * time.Second
		tasks = append(tasks, task)
	}
	return tasks, persistenceError(rows.Err())
}

func (s *syncState) updateFullTableTask(task fullTableTask, phase string, runErr error, completed bool) error {
	message := ""
	if runErr != nil {
		message = runErr.Error()
	}
	_, err := s.db.Exec(`UPDATE full_table_task SET phase=?,last_error=?,last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END
		WHERE source_db=? AND target_db=? AND table_name=?`, phase, message, completed,
		time.Now().UTC().Format(time.RFC3339Nano), task.Pair.Source, task.Pair.Target, task.Table)
	return persistenceError(err)
}

func fullTableDue(task fullTableTask, now time.Time) (bool, error) {
	if task.LastSuccessAt == "" {
		return true, nil
	}
	last, err := time.Parse(time.RFC3339Nano, task.LastSuccessAt)
	if err != nil {
		return false, fmt.Errorf("invalid last_success_at for %s.%s: %w", task.Pair.Source, task.Table, err)
	}
	return !now.Before(last.Add(task.Interval)), nil
}

func (s *syncer) syncFullTable(ctx context.Context, task fullTableTask, targetTables map[string]bool) error {
	pair, table := task.Pair, task.Table
	ddl, err := s.source.createTableSQL(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	if !createPrefix.MatchString(ddl) {
		return fmt.Errorf("source is not a supported OLAP table")
	}
	if !targetTables[table] {
		createSQL, err := targetDDL(ddl, pair.Target, table)
		if err != nil {
			return err
		}
		if err := s.target.exec(ctx, createSQL); err != nil {
			return fmt.Errorf("create target table: %w", err)
		}
		targetTables[table] = true
		s.logger.Printf("CREATE_TABLE database=%s table=%s", pair.Target, table)
	}
	cols, err := s.source.columns(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	targetCols, err := s.target.columns(ctx, pair.Target, table)
	if err != nil {
		return err
	}
	if err := validateTargetColumns(cols, targetCols); err != nil {
		return err
	}
	columns := make([]string, len(cols))
	for i, col := range cols {
		columns[i] = col.Name
	}
	root := s.store.partitionPrefix(pair.Source, pair.Target, table, "full_table")
	if err := s.store.clear(ctx, root); err != nil {
		return fmt.Errorf("clear previous full-table backup: %w", err)
	}
	probe, err := s.source.query(ctx, "SELECT 1 FROM "+qtable(pair.Source, table)+" LIMIT 1")
	if err != nil {
		return err
	}
	empty := len(probe) == 0
	uri := ""
	if !empty {
		prefix, err := backupGenerationPrefix(root)
		if err != nil {
			return err
		}
		uri = s.store.uri(prefix)
		s.logger.Printf("FULL_TABLE_BACKUP_START database=%s table=%s uri=%s", pair.Source, table, uri)
		if _, err := s.source.query(ctx, outfileTableWhereSQL(pair.Source, table, uri, columns, "", s.options.S3)); err != nil {
			return fmt.Errorf("full-table OUTFILE: %w", err)
		}
		objects, err := s.store.list(ctx, prefix)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return fmt.Errorf("full-table OUTFILE returned no files for nonempty source")
		}
		uri += "*.parquet"
	}
	if err := s.state.updateFullTableTask(task, "overwriting", nil, false); err != nil {
		return err
	}
	s.logger.Printf("FULL_TABLE_OVERWRITE_START database=%s table=%s empty=%t", pair.Target, table, empty)
	if err := s.target.exec(ctx, overwriteTableSQL(pair.Target, table, uri, columns, empty, s.options.S3)); err != nil {
		return fmt.Errorf("full-table overwrite: %w", err)
	}
	s.logger.Printf("FULL_TABLE_OVERWRITE_DONE database=%s table=%s", pair.Target, table)
	return nil
}
