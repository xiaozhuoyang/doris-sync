package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type syncer struct {
	options          Options
	source           *database
	target           *database
	store            *objectStore
	state            *syncState
	logger           *log.Logger
	nextVersionCheck time.Time
}

type stats struct{ tables, skipped, backedUp, imported, unchanged, failed int }

func main() {
	configFile := flag.String("config", "partition-sync.json", "configuration file")
	once := flag.Bool("once", false, "run one synchronization cycle even when interval is configured")
	checkInterval := flag.String("check-interval", "", "visible version check interval (default 1h)")
	syncMode := flag.String("sync-mode", "overwrite", "incremental mode: overwrite or time-window")
	timeField := flag.String("time-field", "", "DATE/DATETIME column required for time-window mode")
	flag.Parse()
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	if err := run(*configFile, *once, *checkInterval, *syncMode, *timeField, logger); err != nil {
		logger.Printf("FATAL error=%q", err)
		os.Exit(1)
	}
}

func run(configFile string, once bool, checkInterval, syncMode, timeField string, logger *log.Logger) error {
	if syncMode != "overwrite" && syncMode != "time-window" {
		return fmt.Errorf("--sync-mode must be overwrite or time-window")
	}
	if syncMode == "time-window" && strings.TrimSpace(timeField) == "" {
		return fmt.Errorf("--time-field is required in time-window mode")
	}
	if syncMode == "overwrite" && timeField != "" {
		return fmt.Errorf("--time-field is only valid in time-window mode")
	}
	o, err := loadOptions(configFile)
	if err != nil {
		return err
	}
	if checkInterval != "" {
		d, err := time.ParseDuration(checkInterval)
		if err != nil || d < time.Minute {
			return fmt.Errorf("--check-interval must be at least 1m")
		}
		o.Interval = checkInterval
	}
	if once {
		o.Interval = ""
	}
	if err := os.MkdirAll(filepath.Dir(o.StateFile), 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(o.StateFile+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("state file is already in use: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	source, err := openDatabase(ctx, o.Source)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer source.close()
	target, err := openDatabase(ctx, o.Target)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer target.close()
	store, err := openObjectStore(ctx, o.S3)
	if err != nil {
		return err
	}
	state, err := openState(o.StateFile)
	if err != nil {
		return err
	}
	defer state.close()
	if state.Mode != "" && state.Mode != syncMode {
		return fmt.Errorf("state file uses mode %q, cannot restart in %q mode; use a separate state file", state.Mode, syncMode)
	}
	if state.TimeField != "" && state.TimeField != timeField {
		return fmt.Errorf("state file uses time field %q, cannot restart with %q", state.TimeField, timeField)
	}
	state.Mode, state.TimeField = syncMode, timeField
	if err := state.saveMetadata(); err != nil {
		return err
	}
	s := &syncer{options: o, source: source, target: target, store: store, state: state, logger: logger}
	if o.Interval != "" {
		interval, _ := time.ParseDuration(o.Interval)
		s.nextVersionCheck = time.Now().Add(interval)
	}
	for {
		cycleErr := s.cycle(ctx)
		if o.Interval == "" || ctx.Err() != nil {
			return cycleErr
		}
		if cycleErr != nil {
			logger.Printf("CYCLE_ERROR error=%q", cycleErr)
			if errors.Is(cycleErr, errStatePersistence) {
				return cycleErr
			}
		}
		interval, _ := time.ParseDuration(o.Interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (s *syncer) cycle(ctx context.Context) error {
	var summary stats
	s.logger.Printf("CYCLE_START sync_mode=%s", s.state.Mode)
	for _, mapping := range s.options.Databases {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.target.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(mapping.Target)); err != nil {
			return err
		}
		sourceTables, err := s.source.tables(ctx, mapping.Source)
		if err != nil {
			return err
		}
		targetNames, err := s.target.tables(ctx, mapping.Target)
		if err != nil {
			return err
		}
		targetTables := map[string]bool{}
		for _, table := range targetNames {
			targetTables[table] = true
		}
		for _, table := range sourceTables {
			if !s.options.allowsTable(table) {
				continue
			}
			summary.tables++
			if err := s.syncTable(ctx, mapping, table, targetTables, &summary); err != nil {
				summary.failed++
				s.logger.Printf("TABLE_FAILED database=%s table=%s error=%q", mapping.Source, table, err)
				if errors.Is(err, errStatePersistence) {
					return err
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
	}
	s.logger.Printf("SUMMARY tables=%d skipped=%d backed_up=%d imported=%d unchanged=%d failed=%d", summary.tables, summary.skipped, summary.backedUp, summary.imported, summary.unchanged, summary.failed)
	if summary.failed > 0 {
		return fmt.Errorf("%d tables failed", summary.failed)
	}
	return nil
}

func (s *syncer) maybeScanCompleted(ctx context.Context) error {
	if s.nextVersionCheck.IsZero() || time.Now().Before(s.nextVersionCheck) {
		return nil
	}
	interval, _ := time.ParseDuration(s.options.Interval)
	s.nextVersionCheck = time.Now().Add(interval)
	var failures []error
	var checked, changed int
	for _, pair := range s.options.Databases {
		byTable := map[string]bool{}
		for key, entry := range s.state.Partitions {
			parts := strings.Split(key, "\x00")
			if len(parts) == 4 && parts[0] == pair.Source && parts[1] == pair.Target && entry != nil && entry.ImportedIdentity != "" && s.options.allowsTable(parts[2]) {
				byTable[parts[2]] = true
			}
		}
		tables := make([]string, 0, len(byTable))
		for table := range byTable {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		for _, table := range tables {
			if err := ctx.Err(); err != nil {
				return err
			}
			parts, err := s.source.partitions(ctx, pair.Source, table)
			if err != nil {
				failures = append(failures, fmt.Errorf("%s.%s: %w", pair.Source, table, err))
				continue
			}
			var candidates []partition
			for _, part := range parts {
				entry := s.state.Partitions[stateKey(pair.Source, pair.Target, table, part.Name)]
				if entry != nil && entry.ImportedIdentity != "" {
					checked++
					if entry.ImportedIdentity != sourceIdentity(part) || entry.ImportInProgress || entry.PendingDelta != nil || (s.state.Mode == "time-window" && !entry.WatermarkReady) {
						candidates = append(candidates, part)
					}
				}
			}
			if len(candidates) == 0 {
				continue
			}
			ddl, err := s.source.createTableSQL(ctx, pair.Source, table)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			cols, err := s.source.columns(ctx, pair.Source, table)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if s.state.Mode == "time-window" {
				if err := validateTimeField(cols, s.state.TimeField); err != nil {
					failures = append(failures, fmt.Errorf("%s.%s: %w", pair.Source, table, err))
					continue
				}
			}
			names := make([]string, len(cols))
			for i, col := range cols {
				names[i] = col.Name
			}
			for _, part := range candidates {
				if err := s.syncPartitionByMode(ctx, pair, table, part, names, ddl, &stats{}); err != nil {
					failures = append(failures, fmt.Errorf("%s.%s.%s: %w", pair.Source, table, part.Name, err))
				} else {
					changed++
				}
			}
		}
	}
	s.logger.Printf("VERSION_CHECK_DONE checked=%d changed=%d failed=%d", checked, changed, len(failures))
	return errors.Join(failures...)
}

func (s *syncer) syncTable(ctx context.Context, pair DatabasePair, table string, targetTables map[string]bool, summary *stats) error {
	ddl, err := s.source.createTableSQL(ctx, pair.Source, table)
	if isAsyncView(err) {
		summary.skipped++
		s.logger.Printf("SKIP_TABLE database=%s table=%s reason=async_materialized_view", pair.Source, table)
		return nil
	}
	if err != nil {
		return err
	}
	if !createPrefix.MatchString(ddl) {
		summary.skipped++
		s.logger.Printf("SKIP_TABLE database=%s table=%s reason=non_olap_table", pair.Source, table)
		return nil
	}
	columns, err := s.source.columns(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	if s.state.Mode == "time-window" {
		if err := validateTimeField(columns, s.state.TimeField); err != nil {
			return err
		}
	}
	parts, err := s.source.partitions(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	columnJSON, _ := json.Marshal(columns)
	hash := sha256.Sum256(columnJSON)
	schemaHash := hex.EncodeToString(hash[:])
	tableKey := stateKey(pair.Source, pair.Target, table, "")
	if previous := s.state.Tables[tableKey]; previous != "" && previous != schemaHash {
		return fmt.Errorf("source schema changed; migrate target table before continuing")
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
	} else {
		targetColumns, err := s.target.columns(ctx, pair.Target, table)
		if err != nil {
			return err
		}
		if !sameColumns(columns, targetColumns) {
			return fmt.Errorf("target schema columns differ from source")
		}
	}
	s.state.Tables[tableKey] = schemaHash
	if err := s.state.saveTable(tableKey); err != nil {
		return err
	}
	if len(parts) == 0 {
		s.logger.Printf("EMPTY_TABLE database=%s table=%s", pair.Source, table)
		return nil
	}
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	var partitionErrors []error
	for i, part := range parts {
		if err := ctx.Err(); err != nil {
			return err
		}
		previousCheck := s.nextVersionCheck
		if err := s.maybeScanCompleted(ctx); err != nil {
			s.logger.Printf("VERSION_CHECK_FAILED error=%q", err)
			if errors.Is(err, errStatePersistence) {
				return err
			}
		}
		if !previousCheck.Equal(s.nextVersionCheck) {
			latestParts, err := s.source.partitions(ctx, pair.Source, table)
			if err != nil {
				return err
			}
			current, found := findPartition(latestParts, part.Name)
			if !found {
				return fmt.Errorf("source partition %s disappeared during version check", part.Name)
			}
			part = current
		}
		if err := s.syncPartitionByMode(ctx, pair, table, part, columnNames, ddl, summary); err != nil {
			if errors.Is(err, errStatePersistence) {
				return err
			}
			partitionErrors = append(partitionErrors, fmt.Errorf("partition %s: %w", part.Name, err))
			s.logger.Printf("PARTITION_FAILED database=%s table=%s partition=%s error=%q", pair.Source, table, part.Name, err)
			continue
		}
		s.logger.Printf("PROGRESS database=%s table=%s partitions=%d/%d", pair.Source, table, i+1, len(parts))
	}
	return errors.Join(partitionErrors...)
}

func sameColumns(a, b []column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || !strings.EqualFold(a[i].Type, b[i].Type) {
			return false
		}
	}
	return true
}

func (s *syncer) syncPartition(ctx context.Context, pair DatabasePair, table string, part partition, columns []string, ddl string, summary *stats) error {
	key := stateKey(pair.Source, pair.Target, table, part.Name)
	identity := sourceIdentity(part)
	entry := s.state.Partitions[key]
	if entry == nil {
		entry = &partitionState{}
		s.state.Partitions[key] = entry
	}
	if entry.BackupReady && entry.ImportedIdentity == identity && !entry.ImportInProgress {
		if len(entry.Objects) == 0 {
			summary.unchanged++
			return nil
		}
		targetParts, err := s.target.partitions(ctx, pair.Target, table)
		if err != nil {
			return err
		}
		if _, found, err := matchTargetPartition(part, targetParts); err != nil {
			return err
		} else if found {
			summary.unchanged++
			return nil
		}
		s.logger.Printf("TARGET_PARTITION_MISSING database=%s table=%s partition=%s action=reimport", pair.Target, table, part.Name)
		entry.ImportedIdentity = ""
		entry.UpdatedAt = time.Now()
		if err := s.state.savePartition(key); err != nil {
			return err
		}
	}
	prefix := s.store.partitionPrefix(pair.Source, pair.Target, table, part.Name)
	if entry.BackupIdentity != identity || !entry.BackupReady {
		entry.SourceIdentity = identity
		entry.BackupReady = false
		entry.BackupIdentity = ""
		entry.Objects = nil
		entry.UpdatedAt = time.Now()
		if err := s.state.savePartition(key); err != nil {
			return err
		}
		if err := s.store.clear(ctx, prefix); err != nil {
			return fmt.Errorf("clear backup prefix: %w", err)
		}
		s.logger.Printf("BACKUP_START database=%s table=%s partition=%s visible_version=%s uri=%s", pair.Source, table, part.Name, part.Version, s.store.uri(prefix))
		if _, err := s.source.query(ctx, outfileSQL(pair.Source, table, part.Name, s.store.uri(prefix), columns, s.options.S3)); err != nil {
			return fmt.Errorf("OUTFILE: %w", err)
		}
		objects, err := s.store.list(ctx, prefix)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			rows, err := s.source.query(ctx, "SELECT 1 FROM "+qtable(pair.Source, table)+" PARTITION("+ident(part.Name)+") LIMIT 1")
			if err != nil {
				return err
			}
			if len(rows) > 0 {
				return fmt.Errorf("OUTFILE produced no objects for nonempty source partition")
			}
		}
		latest, err := s.source.partitions(ctx, pair.Source, table)
		if err != nil {
			return err
		}
		if current, found := findPartition(latest, part.Name); !found || sourceIdentity(current) != identity {
			return fmt.Errorf("source visible version changed during backup; retry next cycle")
		}
		entry.BackupIdentity = identity
		entry.Objects = objects
		entry.BackupReady = true
		entry.UpdatedAt = time.Now()
		if err := s.state.savePartition(key); err != nil {
			return err
		}
		summary.backedUp++
		s.logger.Printf("BACKUP_DONE database=%s table=%s partition=%s visible_version=%s files=%d", pair.Source, table, part.Name, part.Version, len(objects))
	}
	currentObjects, err := s.store.list(ctx, prefix)
	if err != nil {
		return err
	}
	if !equalStrings(currentObjects, entry.Objects) {
		return fmt.Errorf("backup objects changed since export; refusing import")
	}
	currentSource, err := s.source.partitions(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	if current, found := findPartition(currentSource, part.Name); !found || sourceIdentity(current) != identity {
		return fmt.Errorf("source visible version changed before import; retry next cycle")
	}
	targetParts, err := s.target.partitions(ctx, pair.Target, table)
	if err != nil {
		return err
	}
	targetPart, exists, err := matchTargetPartition(part, targetParts)
	if err != nil {
		return err
	}
	if !exists && !isAutoPartitionDDL(ddl) {
		return fmt.Errorf("target partition %s missing and table does not use AUTO PARTITION", part.Name)
	}
	entry.ImportInProgress = true
	entry.UpdatedAt = time.Now()
	if err := s.state.savePartition(key); err != nil {
		return err
	}
	if exists {
		s.logger.Printf("OVERWRITE_START database=%s table=%s source_partition=%s target_partition=%s files=%d", pair.Target, table, part.Name, targetPart.Name, len(entry.Objects))
		statement := overwriteSQL(pair.Target, table, targetPart.Name, s.store.uri(prefix)+"*.parquet", columns, len(entry.Objects) == 0, s.options.S3)
		if err := s.target.exec(ctx, statement); err != nil {
			return fmt.Errorf("overwrite target partition %s: %w", targetPart.Name, err)
		}
	} else if len(entry.Objects) > 0 {
		if err := s.target.exec(ctx, importSQL(pair.Target, table, s.store.uri(prefix)+"*.parquet", columns, s.options.S3)); err != nil {
			return fmt.Errorf("initial import partition %s: %w", part.Name, err)
		}
	}
	if len(entry.Objects) > 0 {
		updatedTargetParts, err := s.target.partitions(ctx, pair.Target, table)
		if err != nil {
			return err
		}
		if _, found, err := matchTargetPartition(part, updatedTargetParts); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("target AUTO PARTITION did not create a partition matching source range for %s", part.Name)
		}
	}
	latest, err := s.source.partitions(ctx, pair.Source, table)
	if err != nil {
		return err
	}
	if current, found := findPartition(latest, part.Name); !found || sourceIdentity(current) != identity {
		return fmt.Errorf("source visible version changed during import; retry next cycle")
	}
	entry.ImportedIdentity = identity
	entry.ImportInProgress = false
	entry.UpdatedAt = time.Now()
	if err := s.state.savePartition(key); err != nil {
		return err
	}
	summary.imported++
	s.logger.Printf("IMPORT_DONE database=%s table=%s partition=%s visible_version=%s files=%d", pair.Target, table, part.Name, part.Version, len(entry.Objects))
	return nil
}

var autoRangeDDL = regexp.MustCompile(`(?is)\bPARTITION\s+BY\s+RANGE\s*\(\s*date_trunc\s*\(`)

func isAutoPartitionDDL(ddl string) bool {
	upper := strings.ToUpper(ddl)
	return strings.Contains(upper, "AUTO PARTITION BY LIST") || autoRangeDDL.MatchString(ddl)
}

func findPartition(parts []partition, name string) (partition, bool) {
	for _, part := range parts {
		if part.Name == name {
			return part, true
		}
	}
	return partition{}, false
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
