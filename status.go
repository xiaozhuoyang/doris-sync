package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sort"
	"time"
)

type statusTable struct {
	SourceDatabase        string `json:"sourceDatabase"`
	TargetDatabase        string `json:"targetDatabase"`
	Table                 string `json:"table"`
	TotalPartitions       int    `json:"totalPartitions"`
	InventoryKnown        bool   `json:"inventoryKnown"`
	ObservedPartitions    int    `json:"observedPartitions"`
	CurrentPartitions     int    `json:"currentPartitions"`
	IncrementalMonitoring int    `json:"incrementalMonitoring"`
	IncrementalSyncing    int    `json:"incrementalSyncing"`
}

type statusPartition struct {
	SourceDatabase   string    `json:"sourceDatabase"`
	TargetDatabase   string    `json:"targetDatabase"`
	Table            string    `json:"table"`
	Partition        string    `json:"partition"`
	Phase            string    `json:"phase"`
	SourceIdentity   string    `json:"sourceIdentity"`
	ImportedIdentity string    `json:"importedIdentity,omitempty"`
	BackupReady      bool      `json:"backupReady"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type statusReport struct {
	Mode                         string            `json:"mode"`
	ManagedTasks                 bool              `json:"managedTasks,omitempty"`
	TimeField                    string            `json:"timeField,omitempty"`
	TimeZone                     string            `json:"timeZone,omitempty"`
	HourlyStart                  string            `json:"hourlyStart,omitempty"`
	HourlyWindows                []statusHourly    `json:"hourlyWindows,omitempty"`
	FullTableTasks               []statusFullTable `json:"fullTableTasks,omitempty"`
	KnownTotalPartitions         int               `json:"knownTotalPartitions"`
	CurrentPartitions            int               `json:"currentPartitions"`
	ProgressPercent              float64           `json:"progressPercent"`
	IncrementalMonitoringCount   int               `json:"incrementalMonitoringCount"`
	IncrementalSyncingCount      int               `json:"incrementalSyncingCount"`
	Tables                       []statusTable     `json:"tables"`
	ActivePartitions             []statusPartition `json:"activePartitions"`
	IncrementalSyncingPartitions []statusPartition `json:"incrementalSyncingPartitions"`
}

type statusHourly struct {
	Mode           string         `json:"mode,omitempty"`
	SourceDatabase string         `json:"sourceDatabase"`
	TargetDatabase string         `json:"targetDatabase"`
	Table          string         `json:"table"`
	TimeField      string         `json:"timeField,omitempty"`
	Start          string         `json:"start,omitempty"`
	TimeZone       string         `json:"timeZone,omitempty"`
	NextStart      string         `json:"nextStart"`
	PendingEnd     string         `json:"pendingEnd,omitempty"`
	Phase          string         `json:"phase"`
	LastSuccessAt  string         `json:"lastSuccessAt,omitempty"`
	LastError      string         `json:"lastError,omitempty"`
	DayComparison  *dayComparison `json:"dayComparison,omitempty"`
}

type statusFullTable struct {
	Mode           string `json:"mode"`
	SourceDatabase string `json:"sourceDatabase"`
	TargetDatabase string `json:"targetDatabase"`
	Table          string `json:"table"`
	Interval       string `json:"interval"`
	Phase          string `json:"phase"`
	LastSuccessAt  string `json:"lastSuccessAt,omitempty"`
	NextDueAt      string `json:"nextDueAt,omitempty"`
	LastError      string `json:"lastError,omitempty"`
}

func readStatus(path string) (statusReport, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return statusReport{}, err
	}
	dsn := (&url.URL{Scheme: "file", Path: abs, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return statusReport{}, err
	}
	defer db.Close()
	report := statusReport{Tables: []statusTable{}, ActivePartitions: []statusPartition{}, IncrementalSyncingPartitions: []statusPartition{}}
	metadata, err := db.Query("SELECT key, value FROM metadata WHERE key IN ('mode', 'time_field', 'time_zone', 'hourly_start', 'hourly_managed')")
	if err != nil {
		return report, err
	}
	for metadata.Next() {
		var key, value string
		if err := metadata.Scan(&key, &value); err != nil {
			metadata.Close()
			return report, err
		}
		switch key {
		case "mode":
			report.Mode = value
		case "time_field":
			report.TimeField = value
		case "time_zone":
			report.TimeZone = value
		case "hourly_start":
			report.HourlyStart = value
		case "hourly_managed":
			report.ManagedTasks = value == "1"
		}
	}
	if err := metadata.Err(); err != nil {
		metadata.Close()
		return report, err
	}
	metadata.Close()
	if report.ManagedTasks {
		rows, err := db.Query(`SELECT t.source_db,t.target_db,t.table_name,t.time_field,t.start_time,t.time_zone,
			t.last_error,t.last_success_at,s.state_json FROM hourly_task t LEFT JOIN hourly_state s
			ON t.source_db=s.source_db AND t.target_db=s.target_db AND t.table_name=s.table_name
			ORDER BY t.source_db,t.target_db,t.table_name`)
		if err != nil {
			return report, err
		}
		defer rows.Close()
		for rows.Next() {
			var item statusHourly
			var raw sql.NullString
			if err := rows.Scan(&item.SourceDatabase, &item.TargetDatabase, &item.Table, &item.TimeField, &item.Start, &item.TimeZone, &item.LastError, &item.LastSuccessAt, &raw); err != nil {
				return report, err
			}
			item.Mode, item.NextStart, item.Phase = "hourly-window", item.Start, "queued"
			if raw.Valid {
				var entry hourlyState
				if err := json.Unmarshal([]byte(raw.String), &entry); err != nil {
					return report, err
				}
				item.NextStart, item.Phase = entry.Next, "waiting"
				if entry.Pending != nil {
					item.PendingEnd, item.Phase = entry.Pending.To, "backing_up"
					if entry.Pending.BackupReady {
						item.Phase = "importing"
					}
				}
			}
			if item.LastError != "" {
				item.Phase = "error"
			}
			report.HourlyWindows = append(report.HourlyWindows, item)
		}
		if err := rows.Err(); err != nil {
			return report, err
		}
		rows.Close()
		var fullTableExists int
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='full_table_task'").Scan(&fullTableExists); err != nil {
			return report, err
		}
		if fullTableExists == 0 {
			return report, nil
		}
		fullRows, err := db.Query(`SELECT source_db,target_db,table_name,interval_seconds,phase,last_error,last_success_at
			FROM full_table_task ORDER BY source_db,target_db,table_name`)
		if err != nil {
			return report, err
		}
		defer fullRows.Close()
		for fullRows.Next() {
			var item statusFullTable
			var seconds int64
			if err := fullRows.Scan(&item.SourceDatabase, &item.TargetDatabase, &item.Table, &seconds, &item.Phase, &item.LastError, &item.LastSuccessAt); err != nil {
				return report, err
			}
			item.Mode, item.Interval = "full-table", (time.Duration(seconds) * time.Second).String()
			if item.LastSuccessAt != "" {
				last, err := time.Parse(time.RFC3339Nano, item.LastSuccessAt)
				if err != nil {
					return report, err
				}
				item.NextDueAt = last.Add(time.Duration(seconds) * time.Second).Format(time.RFC3339Nano)
			}
			report.FullTableTasks = append(report.FullTableTasks, item)
		}
		return report, fullRows.Err()
	}
	if report.Mode == "hourly-window" {
		rows, err := db.Query("SELECT source_db, target_db, table_name, state_json FROM hourly_state ORDER BY source_db, target_db, table_name")
		if err != nil {
			return report, err
		}
		defer rows.Close()
		for rows.Next() {
			var source, target, table, raw string
			if err := rows.Scan(&source, &target, &table, &raw); err != nil {
				return report, err
			}
			var entry hourlyState
			if err := json.Unmarshal([]byte(raw), &entry); err != nil {
				return report, err
			}
			item := statusHourly{SourceDatabase: source, TargetDatabase: target, Table: table, NextStart: entry.Next, Phase: "waiting"}
			if entry.Pending != nil {
				item.PendingEnd, item.Phase = entry.Pending.To, "backing_up"
				if entry.Pending.BackupReady {
					item.Phase = "importing"
				}
			}
			report.HourlyWindows = append(report.HourlyWindows, item)
		}
		return report, rows.Err()
	}

	tables := map[string]*statusTable{}
	var inventoryExists int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='table_inventory'").Scan(&inventoryExists); err != nil {
		return report, err
	}
	if inventoryExists != 0 {
		rows, err := db.Query("SELECT source_db, target_db, table_name, partition_count FROM table_inventory")
		if err != nil {
			return report, fmt.Errorf("read table inventory: %w", err)
		}
		for rows.Next() {
			var source, target, table string
			var total int
			if err := rows.Scan(&source, &target, &table, &total); err != nil {
				rows.Close()
				return report, err
			}
			tables[stateKey(source, target, table, "")] = &statusTable{SourceDatabase: source, TargetDatabase: target, Table: table, TotalPartitions: total, InventoryKnown: true}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return report, err
		}
		rows.Close()
	}

	rows, err := db.Query("SELECT source_db, target_db, table_name, partition_name, state_json FROM partition_state")
	if err != nil {
		return report, err
	}
	for rows.Next() {
		var source, target, table, name, raw string
		if err := rows.Scan(&source, &target, &table, &name, &raw); err != nil {
			rows.Close()
			return report, err
		}
		var state partitionState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			rows.Close()
			return report, fmt.Errorf("partition %s: %w", name, err)
		}
		key := stateKey(source, target, table, "")
		t := tables[key]
		if t == nil {
			t = &statusTable{SourceDatabase: source, TargetDatabase: target, Table: table}
			tables[key] = t
		}
		t.ObservedPartitions++
		part := statusPartition{SourceDatabase: source, TargetDatabase: target, Table: table, Partition: name, SourceIdentity: state.SourceIdentity, ImportedIdentity: state.ImportedIdentity, BackupReady: state.BackupReady, UpdatedAt: state.UpdatedAt}
		switch {
		case state.ImportedIdentity != "" && state.ImportedIdentity == state.SourceIdentity && state.BackupPrefix != "" && !state.ImportInProgress && state.PendingDelta == nil:
			part.Phase = "incremental_monitoring"
			t.CurrentPartitions++
			t.IncrementalMonitoring++
		case state.ImportedIdentity != "":
			part.Phase = "incremental_syncing"
			t.IncrementalSyncing++
			report.IncrementalSyncingPartitions = append(report.IncrementalSyncingPartitions, part)
		default:
			part.Phase = "initial_backup"
			if state.BackupReady {
				part.Phase = "initial_import"
			}
		}
		if part.Phase != "incremental_monitoring" {
			report.ActivePartitions = append(report.ActivePartitions, part)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report, err
	}
	rows.Close()

	for _, table := range tables {
		report.Tables = append(report.Tables, *table)
		if table.InventoryKnown {
			report.KnownTotalPartitions += table.TotalPartitions
		}
		report.CurrentPartitions += table.CurrentPartitions
		report.IncrementalMonitoringCount += table.IncrementalMonitoring
		report.IncrementalSyncingCount += table.IncrementalSyncing
	}
	if report.KnownTotalPartitions > 0 {
		report.ProgressPercent = float64(report.CurrentPartitions) * 100 / float64(report.KnownTotalPartitions)
	}
	sort.Slice(report.Tables, func(i, j int) bool {
		a, b := report.Tables[i], report.Tables[j]
		return stateKey(a.SourceDatabase, a.TargetDatabase, a.Table, "") < stateKey(b.SourceDatabase, b.TargetDatabase, b.Table, "")
	})
	sort.Slice(report.ActivePartitions, func(i, j int) bool {
		a, b := report.ActivePartitions[i], report.ActivePartitions[j]
		return stateKey(a.SourceDatabase, a.TargetDatabase, a.Table, a.Partition) < stateKey(b.SourceDatabase, b.TargetDatabase, b.Table, b.Partition)
	})
	sort.Slice(report.IncrementalSyncingPartitions, func(i, j int) bool {
		a, b := report.IncrementalSyncingPartitions[i], report.IncrementalSyncingPartitions[j]
		return stateKey(a.SourceDatabase, a.TargetDatabase, a.Table, a.Partition) < stateKey(b.SourceDatabase, b.TargetDatabase, b.Table, b.Partition)
	})
	return report, nil
}

func printStatus(path string, out io.Writer) error {
	report, err := readStatus(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
