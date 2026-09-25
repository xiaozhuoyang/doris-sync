package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var errStatePersistence = errors.New("state persistence failed")

func persistenceError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errStatePersistence, err)
}

type partitionState struct {
	SourceIdentity   string      `json:"sourceIdentity"`
	BackupIdentity   string      `json:"backupIdentity,omitempty"`
	ImportedIdentity string      `json:"importedIdentity,omitempty"`
	Objects          []string    `json:"objects,omitempty"`
	BackupReady      bool        `json:"backupReady"`
	ImportInProgress bool        `json:"importInProgress"`
	Watermark        string      `json:"watermark,omitempty"`
	WatermarkReady   bool        `json:"watermarkReady,omitempty"`
	PendingDelta     *deltaState `json:"pendingDelta,omitempty"`
	UpdatedAt        time.Time   `json:"updatedAt"`
}

type deltaState struct {
	From           string    `json:"from"`
	To             string    `json:"to"`
	SourceIdentity string    `json:"sourceIdentity"`
	Prefix         string    `json:"prefix"`
	Objects        []string  `json:"objects,omitempty"`
	BackupReady    bool      `json:"backupReady"`
	ImportStarted  bool      `json:"importStarted"`
	Label          string    `json:"label"`
	Attempt        int       `json:"attempt"`
	CreatedAt      time.Time `json:"createdAt"`
}

type syncState struct {
	Tables     map[string]string
	Partitions map[string]*partitionState
	Mode       string
	TimeField  string
	db         *sql.DB
}

func openState(path string) (*syncState, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	fail := func(err error) (*syncState, error) { db.Close(); return nil, err }
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
		"CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS table_state (source_db TEXT NOT NULL, target_db TEXT NOT NULL, table_name TEXT NOT NULL, schema_hash TEXT NOT NULL, PRIMARY KEY(source_db, target_db, table_name))",
		"CREATE TABLE IF NOT EXISTS partition_state (source_db TEXT NOT NULL, target_db TEXT NOT NULL, table_name TEXT NOT NULL, partition_name TEXT NOT NULL, source_identity TEXT NOT NULL, backup_identity TEXT NOT NULL, imported_identity TEXT NOT NULL, watermark TEXT NOT NULL, backup_ready INTEGER NOT NULL, import_in_progress INTEGER NOT NULL, updated_at TEXT NOT NULL, state_json TEXT NOT NULL, PRIMARY KEY(source_db, target_db, table_name, partition_name))",
	} {
		if _, err := db.Exec(statement); err != nil {
			return fail(err)
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fail(err)
	}
	s := &syncState{Tables: map[string]string{}, Partitions: map[string]*partitionState{}, db: db}
	rows, err := db.Query("SELECT source_db, target_db, table_name, schema_hash FROM table_state")
	if err != nil {
		return fail(err)
	}
	for rows.Next() {
		var sourceDB, targetDB, table, hash string
		if err := rows.Scan(&sourceDB, &targetDB, &table, &hash); err != nil {
			rows.Close()
			return fail(err)
		}
		s.Tables[stateKey(sourceDB, targetDB, table, "")] = hash
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fail(err)
	}
	rows.Close()
	rows, err = db.Query("SELECT source_db, target_db, table_name, partition_name, state_json FROM partition_state")
	if err != nil {
		return fail(err)
	}
	for rows.Next() {
		var sourceDB, targetDB, table, part, raw string
		if err := rows.Scan(&sourceDB, &targetDB, &table, &part, &raw); err != nil {
			rows.Close()
			return fail(err)
		}
		key := stateKey(sourceDB, targetDB, table, part)
		var entry partitionState
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			rows.Close()
			return fail(fmt.Errorf("partition state %q: %w", key, err))
		}
		s.Partitions[key] = &entry
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fail(err)
	}
	rows.Close()
	rows, err = db.Query("SELECT key, value FROM metadata")
	if err != nil {
		return fail(err)
	}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			rows.Close()
			return fail(err)
		}
		switch key {
		case "mode":
			s.Mode = value
		case "time_field":
			s.TimeField = value
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fail(err)
	}
	rows.Close()
	return s, nil
}

func (s *syncState) close() error { return s.db.Close() }

func (s *syncState) saveMetadata() error {
	tx, err := s.db.Begin()
	if err != nil {
		return persistenceError(err)
	}
	defer tx.Rollback()
	for key, value := range map[string]string{"mode": s.Mode, "time_field": s.TimeField} {
		if _, err := tx.Exec("INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value); err != nil {
			return persistenceError(err)
		}
	}
	return persistenceError(tx.Commit())
}

func (s *syncState) saveTable(key string) error {
	parts, err := splitStateKey(key)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT INTO table_state(source_db, target_db, table_name, schema_hash) VALUES(?, ?, ?, ?) ON CONFLICT(source_db, target_db, table_name) DO UPDATE SET schema_hash=excluded.schema_hash", parts[0], parts[1], parts[2], s.Tables[key])
	return persistenceError(err)
}

func (s *syncState) savePartition(key string) error {
	parts, err := splitStateKey(key)
	if err != nil {
		return err
	}
	entry := s.Partitions[key]
	if entry == nil {
		return fmt.Errorf("missing partition state for %q", key)
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO partition_state(
		source_db, target_db, table_name, partition_name,
		source_identity, backup_identity, imported_identity, watermark,
		backup_ready, import_in_progress, updated_at, state_json
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(source_db, target_db, table_name, partition_name) DO UPDATE SET
		source_identity=excluded.source_identity,
		backup_identity=excluded.backup_identity,
		imported_identity=excluded.imported_identity,
		watermark=excluded.watermark,
		backup_ready=excluded.backup_ready,
		import_in_progress=excluded.import_in_progress,
		updated_at=excluded.updated_at,
		state_json=excluded.state_json`,
		parts[0], parts[1], parts[2], parts[3],
		entry.SourceIdentity, entry.BackupIdentity, entry.ImportedIdentity, entry.Watermark,
		entry.BackupReady, entry.ImportInProgress, entry.UpdatedAt.Format(time.RFC3339Nano), string(raw))
	return persistenceError(err)
}

func stateKey(sourceDB, targetDB, table, partition string) string {
	return strings.Join([]string{sourceDB, targetDB, table, partition}, "\x00")
}

func splitStateKey(key string) ([]string, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid state key %q", key)
	}
	return parts, nil
}

func sourceIdentity(p partition) string { return p.ID + ":" + p.Version }
