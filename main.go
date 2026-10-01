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
	seenPartitions   map[string]bool
}

type stats struct{ tables, skipped, backedUp, imported, unchanged, failed int }

func main() {
	if len(os.Args) > 1 && os.Args[1] == "task" {
		if err := runTaskCommand(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "task:", err)
			os.Exit(1)
		}
		return
	}
	configFile := flag.String("config", "partition-sync.json", "configuration file")
	status := flag.Bool("status", false, "print the current SQLite checkpoint as JSON without connecting to SelectDB")
	stateFile := flag.String("state-file", "", "override SQLite state file in config, or use with --status")
	s3Prefix := flag.String("s3-prefix", "", "dedicated OSS/S3 prefix for --force-overwrite")
	once := flag.Bool("once", false, "run one synchronization cycle even when interval is configured")
	checkInterval := flag.String("check-interval", "", "sync scan interval (default 1h)")
	syncMode := flag.String("sync-mode", "overwrite", "sync mode: overwrite, time-window, or hourly-window")
	timeField := flag.String("time-field", "", "DATE/DATETIME column required for time-window and hourly-window modes")
	windowStart := flag.String("window-start", "", "first hourly window start, YYYY-MM-DD HH:00:00; overrides hourlyStart in config")
	partitions := flag.String("partitions", "", "comma-separated exact source partition names; overrides config partition filters")
	table := flag.String("table", "", "exact table name for --force-overwrite")
	forceOverwrite := flag.Bool("force-overwrite", false, "re-export and overwrite selected partitions even when versions match; requires --once")
	overwritePlanFile := flag.String("overwrite-plan", "", "JSON file listing tables and partitions for a one-time forced overwrite")
	flag.Parse()
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	if *status {
		path := *stateFile
		if path == "" {
			path = "partition-sync-state.db"
		}
		if err := printStatus(path, os.Stdout); err != nil {
			logger.Printf("FATAL error=%q", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*configFile, *once, *checkInterval, *syncMode, *timeField, *partitions, *windowStart, *stateFile, *s3Prefix, *table, *overwritePlanFile, *forceOverwrite, logger); err != nil {
		logger.Printf("FATAL error=%q", err)
		os.Exit(1)
	}
}

func run(configFile string, once bool, checkInterval, syncMode, timeField, partitions, windowStart, stateFile, s3Prefix, table, overwritePlanFile string, forceOverwrite bool, logger *log.Logger) error {
	if syncMode != "overwrite" && syncMode != "time-window" && syncMode != "hourly-window" {
		return fmt.Errorf("--sync-mode must be overwrite, time-window, or hourly-window")
	}
	o, err := loadOptions(configFile)
	if err != nil {
		return err
	}
	var plan *overwritePlan
	if overwritePlanFile != "" {
		if !forceOverwrite {
			return fmt.Errorf("--overwrite-plan requires --force-overwrite")
		}
		if table != "" || partitions != "" || len(o.Partitions) > 0 || o.IncludePartitions != "" || o.IncludeTables != "" || o.ExcludeTables != "" {
			return fmt.Errorf("--overwrite-plan cannot be combined with table or partition filters")
		}
		plan, err = loadOverwritePlan(overwritePlanFile, o.Databases)
		if err != nil {
			return err
		}
	}
	if forceOverwrite {
		if stateFile == "" || s3Prefix == "" {
			return fmt.Errorf("--force-overwrite requires --state-file and --s3-prefix dedicated to this run")
		}
		configuredState, err := filepath.Abs(o.StateFile)
		if err != nil {
			return err
		}
		selectedState, err := filepath.Abs(stateFile)
		if err != nil {
			return err
		}
		configuredPrefix := strings.Trim(o.S3.Prefix, "/")
		selectedPrefix := strings.Trim(s3Prefix, "/")
		if configuredState == selectedState || configuredPrefix == selectedPrefix || strings.HasPrefix(configuredPrefix, selectedPrefix+"/") || strings.HasPrefix(selectedPrefix, configuredPrefix+"/") {
			return fmt.Errorf("--force-overwrite state file and S3 prefix must differ from the service configuration")
		}
		if selectedPrefix == "" {
			return fmt.Errorf("--s3-prefix must be a nonempty dedicated directory")
		}
		o.ManagedTasks = false
		o.HourlyStart = ""
		o.S3.Prefix = s3Prefix
	}
	if stateFile != "" {
		o.StateFile = stateFile
	}
	if syncMode == "time-window" && strings.TrimSpace(timeField) == "" {
		return fmt.Errorf("--time-field is required in time-window mode")
	}
	if syncMode == "overwrite" && timeField != "" {
		return fmt.Errorf("--time-field is only valid in time-window or hourly-window mode")
	}
	if o.ManagedTasks {
		if syncMode != "hourly-window" || timeField != "" || windowStart != "" || o.HourlyStart != "" {
			return fmt.Errorf("managedTasks requires hourly-window mode with per-task time fields and starts")
		}
		if o.IncludeTables != "" || o.ExcludeTables != "" {
			return fmt.Errorf("table filters are not used with managedTasks")
		}
		if len(o.Partitions) > 0 || o.IncludePartitions != "" || partitions != "" {
			return fmt.Errorf("partition filters are not supported in hourly-window mode")
		}
	} else if syncMode == "hourly-window" && strings.TrimSpace(timeField) == "" {
		return fmt.Errorf("--time-field is required in static hourly-window mode")
	}
	if windowStart != "" {
		o.HourlyStart = windowStart
	}
	if syncMode == "hourly-window" && !o.ManagedTasks {
		if _, err := parseHourlyStart(o.HourlyStart, o.TimeZone); err != nil {
			return err
		}
		if len(o.Partitions) > 0 || o.IncludePartitions != "" {
			return fmt.Errorf("partition filters are not supported in hourly-window mode")
		}
	} else if !o.ManagedTasks && (windowStart != "" || o.HourlyStart != "") {
		return fmt.Errorf("hourlyStart and --window-start require hourly-window mode")
	}
	if partitions != "" {
		names, err := parsePartitionNames(partitions)
		if err != nil {
			return err
		}
		o.Partitions = names
		o.IncludePartitions = ""
	}
	if forceOverwrite {
		if plan != nil {
			err = validateForcedOverwriteMode(once, syncMode)
		} else {
			err = validateForcedOverwrite(once, syncMode, table, o)
		}
		if err != nil {
			return err
		}
		o.ForceOverwrite = true
		if plan == nil {
			o.IncludeTables = "^" + regexp.QuoteMeta(table) + "$"
		}
		o.ExcludeTables = ""
	} else if table != "" || s3Prefix != "" || overwritePlanFile != "" {
		return fmt.Errorf("--table, --s3-prefix and --overwrite-plan require --force-overwrite")
	}
	metadataTimeout, _ := time.ParseDuration(o.MetadataTimeout)
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
	source, err := openDatabase(ctx, o.Source, metadataTimeout)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer source.close()
	target, err := openDatabase(ctx, o.Target, metadataTimeout)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer target.close()
	if err := checkConnection(ctx, source); err != nil {
		return fmt.Errorf("source SELECT 1 failed: %w", err)
	}
	if err := checkConnection(ctx, target); err != nil {
		return fmt.Errorf("target SELECT 1 failed: %w", err)
	}
	logger.Printf("CONNECTION_CHECK source=ok target=ok")
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
	if syncMode == "hourly-window" {
		var err error
		if o.ManagedTasks {
			err = state.checkManagedHourlySettings()
		} else {
			err = state.checkHourlySettings(o.HourlyStart, o.TimeZone)
		}
		if err != nil {
			return err
		}
	}
	state.Mode, state.TimeField = syncMode, timeField
	if err := state.saveMetadata(); err != nil {
		return err
	}
	s := &syncer{options: o, source: source, target: target, store: store, state: state, logger: logger, seenPartitions: make(map[string]bool)}
	if plan != nil {
		return s.cycleOverwritePlan(ctx, *plan)
	}
	if o.Interval != "" {
		interval, _ := time.ParseDuration(o.Interval)
		s.nextVersionCheck = time.Now().Add(interval)
	}
	for {
		var cycleErr error
		if syncMode == "hourly-window" {
			cycleErr = s.cycleHourly(ctx)
		} else {
			cycleErr = s.cycle(ctx)
		}
		if ctx.Err() != nil {
			return nil
		}
		if o.Interval == "" {
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
			return nil
		case <-time.After(interval):
		}
	}
}

func validateForcedOverwrite(once bool, mode, table string, o Options) error {
	if err := validateForcedOverwriteMode(once, mode); err != nil {
		return err
	}
	if table == "" || strings.TrimSpace(table) != table {
		return fmt.Errorf("--force-overwrite requires an exact --table name")
	}
	if len(o.Partitions) == 0 || o.IncludePartitions != "" {
		return fmt.Errorf("--force-overwrite requires exact --partitions names")
	}
	if len(o.Databases) != 1 {
		return fmt.Errorf("--force-overwrite requires exactly one source/target database mapping")
	}
	return nil
}

func validateForcedOverwriteMode(once bool, mode string) error {
	if !once || mode != "overwrite" {
		return fmt.Errorf("--force-overwrite requires --sync-mode overwrite --once")
	}
	return nil
}

func (s *syncer) cycle(ctx context.Context) error {
	var summary stats
	type tablePlan struct {
		mapping      DatabasePair
		targetTables map[string]bool
		tables       []string
	}
	var plans []tablePlan
	clear(s.seenPartitions)
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
		readyTables := make([]string, 0, len(sourceTables))
		for _, table := range sourceTables {
			if !s.options.allowsTable(table) {
				continue
			}
			summary.tables++
			ready, err := s.ensureTargetTable(ctx, mapping, table, targetTables, &summary)
			if err != nil {
				summary.failed++
				s.logger.Printf("TABLE_FAILED database=%s table=%s error=%q", mapping.Source, table, err)
				if s.options.ForceOverwrite {
					return fmt.Errorf("preflight %s.%s: %w", mapping.Source, table, err)
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			if ready {
				readyTables = append(readyTables, table)
			}
		}
		plans = append(plans, tablePlan{mapping: mapping, targetTables: targetTables, tables: readyTables})
	}
	var missing []string
	for _, name := range s.options.Partitions {
		if !s.seenPartitions[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("specified source partitions not found in selected tables: %s", strings.Join(missing, ", "))
	}
	for _, plan := range plans {
		for _, table := range plan.tables {
			if err := s.syncTable(ctx, plan.mapping, table, plan.targetTables, &summary); err != nil {
				summary.failed++
				s.logger.Printf("TABLE_FAILED database=%s table=%s error=%q", plan.mapping.Source, table, err)
				if s.options.ForceOverwrite {
					return fmt.Errorf("forced overwrite %s.%s: %w", plan.mapping.Source, table, err)
				}
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

func (s *syncer) ensureTargetTable(ctx context.Context, pair DatabasePair, table string, targetTables map[string]bool, summary *stats) (bool, error) {
	ddl, err := s.source.createTableSQL(ctx, pair.Source, table)
	if isAsyncView(err) {
		summary.skipped++
		s.logger.Printf("SKIP_TABLE database=%s table=%s reason=async_materialized_view", pair.Source, table)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !createPrefix.MatchString(ddl) {
		summary.skipped++
		s.logger.Printf("SKIP_TABLE database=%s table=%s reason=non_olap_table", pair.Source, table)
		return false, nil
	}
	if s.options.ForceOverwrite && !targetTables[table] {
		return false, fmt.Errorf("target table %s.%s does not exist; forced overwrite will not create it", pair.Target, table)
	}
	if !targetTables[table] {
		createSQL, err := targetDDL(ddl, pair.Target, table)
		if err != nil {
			return false, err
		}
		if err := s.target.exec(ctx, createSQL); err != nil {
			return false, fmt.Errorf("create target table: %w", err)
		}
		targetTables[table] = true
		s.logger.Printf("CREATE_TABLE database=%s table=%s", pair.Target, table)
	}
	sourceColumns, err := s.source.columns(ctx, pair.Source, table)
	if err != nil {
		return false, err
	}
	targetColumns, err := s.target.columns(ctx, pair.Target, table)
	if err != nil {
		return false, err
	}
	if err := validateTargetColumns(sourceColumns, targetColumns); err != nil {
		return false, fmt.Errorf("%s.%s: %w", pair.Target, table, err)
	}
	parts, err := s.source.partitions(ctx, pair.Source, table)
	if err != nil {
		return false, err
	}
	if s.options.ForceOverwrite {
		targetDDL, err := s.target.createTableSQL(ctx, pair.Target, table)
		if err != nil {
			return false, err
		}
		allowsAutoPartition := isAutoPartitionDDL(targetDDL)
		targetParts, err := s.target.partitions(ctx, pair.Target, table)
		if err != nil {
			return false, err
		}
		for _, part := range parts {
			if !s.options.allowsPartition(part.Name) {
				continue
			}
			if _, found, err := matchTargetPartition(part, targetParts); err != nil {
				return false, err
			} else if !found && !allowsAutoPartition {
				return false, fmt.Errorf("target partition matching source %s is missing and target table is not AUTO PARTITION", part.Name)
			}
		}
	}
	for _, part := range parts {
		s.seenPartitions[part.Name] = true
	}
	if err := s.state.saveInventory(stateKey(pair.Source, pair.Target, table, ""), len(parts)); err != nil {
		return false, err
	}
	return true, nil
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
			if len(parts) == 4 && parts[0] == pair.Source && parts[1] == pair.Target && entry != nil && entry.ImportedIdentity != "" && s.options.allowsTable(parts[2]) && s.options.allowsPartition(parts[3]) {
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
				if !s.options.allowsPartition(part.Name) {
					continue
				}
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
	filtered := parts[:0]
	for _, part := range parts {
		if s.options.allowsPartition(part.Name) {
			filtered = append(filtered, part)
		}
	}
	parts = filtered
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
		if err := validateTargetColumns(columns, targetColumns); err != nil {
			return err
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
			if s.options.ForceOverwrite {
				return fmt.Errorf("partition %s: %w", part.Name, err)
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

func validateTargetColumns(source, target []column) error {
	if len(source) != len(target) {
		return fmt.Errorf("target column count differs from source: source=%d target=%d; existing target table was not changed", len(source), len(target))
	}
	if !sameColumns(source, target) {
		return fmt.Errorf("target column names, order, or types differ from source; existing target table was not changed")
	}
	return nil
}

func (s *syncer) syncPartition(ctx context.Context, pair DatabasePair, table string, part partition, columns []string, ddl string, summary *stats) error {
	key := stateKey(pair.Source, pair.Target, table, part.Name)
	identity := sourceIdentity(part)
	entry := s.state.Partitions[key]
	if entry == nil {
		entry = &partitionState{}
		s.state.Partitions[key] = entry
	}
	if !s.options.ForceOverwrite && entry.BackupReady && entry.BackupPrefix != "" && entry.ImportedIdentity == identity && !entry.ImportInProgress {
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
	partitionPrefix := s.store.partitionPrefix(pair.Source, pair.Target, table, part.Name)
	prefix := entry.BackupPrefix
	if s.options.ForceOverwrite || entry.BackupIdentity != identity || !entry.BackupReady || prefix == "" {
		var err error
		prefix, err = backupGenerationPrefix(partitionPrefix)
		if err != nil {
			return err
		}
		entry.SourceIdentity = identity
		entry.BackupReady = false
		entry.BackupIdentity = ""
		entry.BackupPrefix = prefix
		entry.Objects = nil
		entry.ImportInProgress = false
		entry.UpdatedAt = time.Now()
		if err := s.state.savePartition(key); err != nil {
			return err
		}
		if err := s.store.clear(ctx, partitionPrefix); err != nil {
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
	if !exists {
		targetDDL, err := s.target.createTableSQL(ctx, pair.Target, table)
		if err != nil {
			return err
		}
		if !isAutoPartitionDDL(targetDDL) {
			return fmt.Errorf("target partition %s missing and target table does not use AUTO PARTITION", part.Name)
		}
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
		s.logger.Printf("IMPORT_START database=%s table=%s partition=%s files=%d", pair.Target, table, part.Name, len(entry.Objects))
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
