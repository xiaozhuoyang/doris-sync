package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func validateTimeField(columns []column, name string) error {
	for _, col := range columns {
		if strings.EqualFold(col.Name, name) {
			kind := strings.ToUpper(col.Type)
			if strings.HasPrefix(kind, "DATE") {
				return nil
			}
			return fmt.Errorf("--time-field %q must be DATE or DATETIME, got %s", name, col.Type)
		}
	}
	return fmt.Errorf("--time-field %q is absent from source table", name)
}

func parseWatermark(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time watermark %q", value)
}

func (s *syncer) savePartitionState(pair DatabasePair, table string, part partition) error {
	return s.state.savePartition(stateKey(pair.Source, pair.Target, table, part.Name))
}

func (s *syncer) maxTime(ctx context.Context, pair DatabasePair, table string, part partition) (string, error) {
	statement := "SELECT MAX(" + ident(s.state.TimeField) + ") AS max_time FROM " + qtable(pair.Source, table) + " PARTITION(" + ident(part.Name) + ")"
	rows, err := s.source.query(ctx, statement)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("MAX(time) returned %d rows", len(rows))
	}
	return rows[0]["max_time"], nil
}

func (s *syncer) seedWatermark(ctx context.Context, pair DatabasePair, table string, part partition, entry *partitionState) error {
	value, err := s.maxTime(ctx, pair, table, part)
	if err != nil {
		return err
	}
	if value != "" {
		if _, err := parseWatermark(value); err != nil {
			return err
		}
	}
	parts, err := s.source.partitions(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	current, found := findPartition(parts, part.Name)
	if !found || sourceIdentity(current) != sourceIdentity(part) {
		return fmt.Errorf("source changed while recording watermark; retry")
	}
	entry.Watermark, entry.WatermarkReady, entry.PendingDelta = value, true, nil
	entry.UpdatedAt = time.Now()
	return s.savePartitionState(pair, table, part)
}

func (s *syncer) syncPartitionByMode(ctx context.Context, pair DatabasePair, table string, part partition, columns []string, ddl string, summary *stats) error {
	if s.state.Mode == "overwrite" {
		return s.syncPartition(ctx, pair, table, part, columns, ddl, summary)
	}
	key := stateKey(pair.Source, pair.Target, table, part.Name)
	entry := s.state.Partitions[key]
	if entry == nil || !entry.BackupReady || entry.ImportInProgress || entry.ImportedIdentity == "" || !entry.WatermarkReady {
		if err := s.syncPartition(ctx, pair, table, part, columns, ddl, summary); err != nil {
			return err
		}
		return s.seedWatermark(ctx, pair, table, part, s.state.Partitions[key])
	}
	if entry.PendingDelta != nil {
		return s.resumeDelta(ctx, pair, table, part, columns, entry, summary)
	}
	if entry.ImportedIdentity == sourceIdentity(part) {
		return s.syncPartition(ctx, pair, table, part, columns, ddl, summary)
	}
	if !strings.HasPrefix(entry.ImportedIdentity, part.ID+":") {
		s.logger.Printf("FULL_REPLACE database=%s table=%s partition=%s reason=partition_id_changed", pair.Source, table, part.Name)
		if err := s.syncPartition(ctx, pair, table, part, columns, ddl, summary); err != nil {
			return err
		}
		return s.seedWatermark(ctx, pair, table, part, entry)
	}
	targetParts, err := s.target.partitions(ctx, pair.Target, table)
	if err != nil {
		return err
	}
	if _, found, err := matchTargetPartition(part, targetParts); err != nil {
		return err
	} else if !found {
		s.logger.Printf("TARGET_PARTITION_MISSING database=%s table=%s partition=%s action=full_restore", pair.Target, table, part.Name)
		if err := s.syncPartition(ctx, pair, table, part, columns, ddl, summary); err != nil {
			return err
		}
		return s.seedWatermark(ctx, pair, table, part, entry)
	}
	newMax, err := s.maxTime(ctx, pair, table, part)
	if err != nil {
		return err
	}
	if entry.Watermark != "" && newMax != "" {
		from, err := parseWatermark(entry.Watermark)
		if err != nil {
			return err
		}
		to, err := parseWatermark(newMax)
		if err != nil {
			return err
		}
		if to.After(from) {
			return s.startDelta(ctx, pair, table, part, columns, entry, newMax, summary)
		}
	}
	s.logger.Printf("FULL_REPLACE database=%s table=%s partition=%s reason=version_changed_without_new_max_time", pair.Source, table, part.Name)
	if err := s.syncPartition(ctx, pair, table, part, columns, ddl, summary); err != nil {
		return err
	}
	return s.seedWatermark(ctx, pair, table, part, entry)
}

func deltaLabel(key, from, to, identity string, attempt int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%d", key, from, to, identity, attempt)))
	return "ps_" + hex.EncodeToString(sum[:12])
}

func (s *syncer) startDelta(ctx context.Context, pair DatabasePair, table string, part partition, columns []string, entry *partitionState, to string, summary *stats) error {
	key := stateKey(pair.Source, pair.Target, table, part.Name)
	label := deltaLabel(key, entry.Watermark, to, sourceIdentity(part), 0)
	entry.PendingDelta = &deltaState{From: entry.Watermark, To: to, SourceIdentity: sourceIdentity(part), Prefix: s.store.partitionPrefix(pair.Source, pair.Target, table, part.Name) + "incremental/" + label + "/", Label: label, CreatedAt: time.Now()}
	if err := s.savePartitionState(pair, table, part); err != nil {
		return err
	}
	return s.resumeDelta(ctx, pair, table, part, columns, entry, summary)
}

func (s *syncer) resumeDelta(ctx context.Context, pair DatabasePair, table string, part partition, columns []string, entry *partitionState, summary *stats) error {
	delta := entry.PendingDelta
	if delta == nil {
		return fmt.Errorf("missing pending delta")
	}
	if !delta.BackupReady {
		if err := s.store.clear(ctx, delta.Prefix); err != nil {
			return err
		}
		where := " WHERE " + ident(s.state.TimeField) + " > " + sqlLiteral(delta.From) + " AND " + ident(s.state.TimeField) + " <= " + sqlLiteral(delta.To)
		s.logger.Printf("DELTA_BACKUP_START database=%s table=%s partition=%s from=%s to=%s", pair.Source, table, part.Name, delta.From, delta.To)
		if _, err := s.source.query(ctx, outfileWhereSQL(pair.Source, table, part.Name, s.store.uri(delta.Prefix), columns, where, s.options.S3)); err != nil {
			return fmt.Errorf("delta OUTFILE: %w", err)
		}
		objects, err := s.store.list(ctx, delta.Prefix)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return fmt.Errorf("delta OUTFILE returned no objects for advanced watermark")
		}
		parts, err := s.source.partitions(ctx, pair.Source, table)
		if err != nil {
			return err
		}
		current, found := findPartition(parts, part.Name)
		if !found || sourceIdentity(current) != delta.SourceIdentity {
			entry.PendingDelta = nil
			if err := s.savePartitionState(pair, table, part); err != nil {
				return err
			}
			return fmt.Errorf("source version changed during delta backup; retry next cycle")
		}
		delta.Objects, delta.BackupReady = objects, true
		if err := s.savePartitionState(pair, table, part); err != nil {
			return err
		}
		summary.backedUp++
	}
	actual, err := s.store.list(ctx, delta.Prefix)
	if err != nil {
		return err
	}
	if !equalStrings(actual, delta.Objects) {
		return fmt.Errorf("delta backup objects changed; refusing import")
	}
	if delta.ImportStarted {
		status, err := s.loadStatus(ctx, pair.Target, delta.Label)
		if err != nil {
			return err
		}
		switch strings.ToUpper(status) {
		case "FINISHED":
			return s.finishDelta(entry, delta, pair, table, part, summary)
		case "CANCELLED":
			delta.Attempt++
			delta.Label = deltaLabel(stateKey(pair.Source, pair.Target, table, part.Name), delta.From, delta.To, delta.SourceIdentity, delta.Attempt)
			delta.ImportStarted = false
			if err := s.savePartitionState(pair, table, part); err != nil {
				return err
			}
		case "PENDING", "ETL", "LOADING", "COMMITTED":
			return fmt.Errorf("delta import label %s is still %s", delta.Label, status)
		default:
			return fmt.Errorf("delta import label %s has no conclusive status (%q); manual verification required", delta.Label, status)
		}
	}
	if !delta.ImportStarted {
		parts, err := s.source.partitions(ctx, pair.Source, table)
		if err != nil {
			return err
		}
		current, found := findPartition(parts, part.Name)
		if !found || sourceIdentity(current) != delta.SourceIdentity {
			entry.PendingDelta = nil
			if err := s.savePartitionState(pair, table, part); err != nil {
				return err
			}
			return fmt.Errorf("source version changed before delta import; retry next cycle")
		}
	}
	delta.ImportStarted = true
	if err := s.savePartitionState(pair, table, part); err != nil {
		return err
	}
	uri := s.store.uri(delta.Prefix) + "*.parquet"
	s.logger.Printf("DELTA_IMPORT_START database=%s table=%s partition=%s label=%s files=%d", pair.Target, table, part.Name, delta.Label, len(delta.Objects))
	if err := s.target.exec(ctx, importLabeledSQL(pair.Target, table, uri, columns, delta.Label, s.options.S3)); err != nil {
		return fmt.Errorf("delta import label %s: %w", delta.Label, err)
	}
	status, err := s.loadStatus(ctx, pair.Target, delta.Label)
	if err != nil {
		return err
	}
	if strings.EqualFold(status, "FINISHED") {
		return s.finishDelta(entry, delta, pair, table, part, summary)
	}
	return fmt.Errorf("delta import label %s returned but load status is %s; will check again", delta.Label, status)
}

func (s *syncer) loadStatus(ctx context.Context, db, label string) (string, error) {
	rows, err := s.target.query(ctx, "SHOW LOAD FROM "+ident(db)+" WHERE LABEL = "+sqlLiteral(label))
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("SHOW LOAD label %s returned %d rows; manual verification required", label, len(rows))
	}
	return rows[0]["state"], nil
}

func (s *syncer) finishDelta(entry *partitionState, delta *deltaState, pair DatabasePair, table string, part partition, summary *stats) error {
	entry.ImportedIdentity, entry.Watermark, entry.WatermarkReady = delta.SourceIdentity, delta.To, true
	entry.PendingDelta = nil
	entry.UpdatedAt = time.Now()
	if err := s.savePartitionState(pair, table, part); err != nil {
		return err
	}
	summary.imported++
	s.logger.Printf("DELTA_IMPORT_DONE database=%s table=%s partition=%s watermark=%s label=%s", pair.Target, table, part.Name, entry.Watermark, delta.Label)
	return nil
}
