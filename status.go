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
	TimeField                    string            `json:"timeField,omitempty"`
	KnownTotalPartitions         int               `json:"knownTotalPartitions"`
	CurrentPartitions            int               `json:"currentPartitions"`
	ProgressPercent              float64           `json:"progressPercent"`
	IncrementalMonitoringCount   int               `json:"incrementalMonitoringCount"`
	IncrementalSyncingCount      int               `json:"incrementalSyncingCount"`
	Tables                       []statusTable     `json:"tables"`
	ActivePartitions             []statusPartition `json:"activePartitions"`
	IncrementalSyncingPartitions []statusPartition `json:"incrementalSyncingPartitions"`
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
	metadata, err := db.Query("SELECT key, value FROM metadata WHERE key IN ('mode', 'time_field')")
	if err != nil {
		return report, err
	}
	for metadata.Next() {
		var key, value string
		if err := metadata.Scan(&key, &value); err != nil {
			metadata.Close()
			return report, err
		}
		if key == "mode" {
			report.Mode = value
		} else {
			report.TimeField = value
		}
	}
	if err := metadata.Err(); err != nil {
		metadata.Close()
		return report, err
	}
	metadata.Close()

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
