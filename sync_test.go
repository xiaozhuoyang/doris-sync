package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTargetDDL(t *testing.T) {
	ddl := "CREATE TABLE `source_db`.`orders` (\n `id` bigint,\n `dt` date\n) ENGINE=OLAP\nPARTITION BY RANGE(`dt`)\n(\n PARTITION `p202601` VALUES [('2026-01-01'), ('2026-02-01')),\n PARTITION `p202602` VALUES [('2026-02-01'), ('2026-03-01'))\n)\nDISTRIBUTED BY HASH(`id`) BUCKETS 8"
	target, err := targetDDL(ddl, "target_db", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(target, "CREATE TABLE `target_db`.`orders`") {
		t.Fatalf("unexpected target DDL: %s", target)
	}
}

func TestTargetDDLRemovesAutoPartitionList(t *testing.T) {
	ddl := "CREATE TABLE `events` (`dt` datetime(6), `value` text) ENGINE=OLAP\n" +
		"AUTO PARTITION BY RANGE (date_trunc(`dt`, 'hour'))\n" +
		"(PARTITION p1 VALUES [('2026-09-01 00:00:00'), ('2026-09-01 01:00:00')),\n" +
		" PARTITION p2 VALUES [('2026-09-01 01:00:00'), ('2026-09-01 02:00:00')))\n" +
		"DISTRIBUTED BY RANDOM BUCKETS 16"
	target, err := targetDDL(ddl, "minimax_target", "events")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(target, "AUTO PARTITION BY RANGE (date_trunc(`dt`, 'hour'))\n()") ||
		strings.Contains(target, "PARTITION p1") || strings.Contains(target, "PARTITION p2") || !strings.Contains(target, "DISTRIBUTED BY RANDOM BUCKETS 16") {
		t.Fatal(target)
	}
}

func TestS3PathAndSQL(t *testing.T) {
	cfg := S3Options{Bucket: "b", Prefix: "backup/run01", Region: "cn-shanghai", Endpoint: "oss-cn-shanghai-internal.aliyuncs.com", ClientEndpoint: "oss-cn-shanghai.aliyuncs.com", AuthMode: "static", AccessKey: "ak", SecretKey: "sk", MaxFileSize: "1024MB"}
	store := &objectStore{options: cfg}
	prefix := store.partitionPrefix("source_db", "target_db", "tbl", "p202601")
	if prefix != "backup/run01/source_db/target_db/tbl/p202601/" {
		t.Fatal(prefix)
	}
	backup := outfileSQL("db", "tbl", "p202601", store.uri(prefix), []string{"id", "dt"}, cfg)
	if !strings.Contains(backup, "PARTITION(`p202601`)") || !strings.Contains(backup, `"s3.secret_key" = "sk"`) || !strings.Contains(backup, `"s3.endpoint" = "oss-cn-shanghai-internal.aliyuncs.com"`) || strings.Contains(backup, "clientEndpoint") {
		t.Fatal(backup)
	}
	importStatement := importSQL("target", "tbl", store.uri(prefix+"part.parquet"), []string{"id", "dt"}, cfg)
	if !strings.Contains(importStatement, "INSERT INTO `target`.`tbl`") || !strings.Contains(importStatement, `"format" = "parquet"`) {
		t.Fatal(importStatement)
	}
}

func TestBackupGenerationPrefix(t *testing.T) {
	base := "backup/src/dst/table/partition/"
	first, err := backupGenerationPrefix(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := backupGenerationPrefix(base)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, base+"full/") || !strings.HasSuffix(first, "/") {
		t.Fatalf("invalid generation prefixes: %q %q", first, second)
	}
}

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	key := stateKey("source_db", "target_db", "tbl", "p1")
	otherKey := stateKey("source_db", "another_target", "tbl", "p1")
	state.Partitions[key] = &partitionState{SourceIdentity: "123:5", BackupIdentity: "123:5", BackupPrefix: "backup/db/tbl/p1/full/generation/", ImportedIdentity: "123:5", BackupReady: true, Objects: []string{"backup/db/tbl/p1/full/generation/a.parquet"}}
	state.Partitions[otherKey] = &partitionState{SourceIdentity: "123:6", ImportedIdentity: "123:6", BackupReady: true}
	state.Tables[stateKey("source_db", "target_db", "tbl", "")] = "schema-hash"
	if err := state.saveTable(stateKey("source_db", "target_db", "tbl", "")); err != nil {
		t.Fatal(err)
	}
	if err := state.savePartition(key); err != nil {
		t.Fatal(err)
	}
	if err := state.savePartition(otherKey); err != nil {
		t.Fatal(err)
	}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}
	loaded, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.close()
	if loaded.Partitions[key].ImportedIdentity != "123:5" {
		t.Fatal("state did not persist")
	}
	if loaded.Partitions[key].BackupPrefix != "backup/db/tbl/p1/full/generation/" {
		t.Fatal("backup generation did not persist")
	}
	if loaded.Tables[stateKey("source_db", "target_db", "tbl", "")] != "schema-hash" {
		t.Fatal("table state did not persist")
	}
	if loaded.Partitions[otherKey].ImportedIdentity != "123:6" {
		t.Fatal("target mappings must have separate state")
	}
	var sourceDB, targetDB, importedVersion string
	if err := loaded.db.QueryRow("SELECT source_db, target_db, imported_identity FROM partition_state WHERE table_name = ? AND partition_name = ? AND target_db = ?", "tbl", "p1", "another_target").Scan(&sourceDB, &targetDB, &importedVersion); err != nil {
		t.Fatal(err)
	}
	if sourceDB != "source_db" || targetDB != "another_target" || importedVersion != "123:6" {
		t.Fatalf("unexpected database mapping/version: %s -> %s version=%s", sourceDB, targetDB, importedVersion)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions: %v", stat.Mode().Perm())
	}
}

func TestStatusWhileStateOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.db")
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	state.Mode = "overwrite"
	if err := state.saveMetadata(); err != nil {
		t.Fatal(err)
	}
	if err := state.saveInventory(stateKey("src", "dst", "events", ""), 3); err != nil {
		t.Fatal(err)
	}
	states := map[string]*partitionState{
		"p1": {SourceIdentity: "1:2", ImportedIdentity: "1:2", BackupPrefix: "backup/src/dst/events/p1/full/a/", BackupReady: true},
		"p2": {SourceIdentity: "2:3", ImportedIdentity: "2:2", BackupPrefix: "backup/src/dst/events/p2/full/b/", BackupReady: true, ImportInProgress: true},
		"p3": {SourceIdentity: "3:2", BackupPrefix: "backup/src/dst/events/p3/full/c/"},
	}
	for name, entry := range states {
		key := stateKey("src", "dst", "events", name)
		state.Partitions[key] = entry
		if err := state.savePartition(key); err != nil {
			t.Fatal(err)
		}
	}
	report, err := readStatus(path)
	if err != nil {
		t.Fatal(err)
	}
	if report.KnownTotalPartitions != 3 || report.CurrentPartitions != 1 || report.IncrementalMonitoringCount != 1 || report.IncrementalSyncingCount != 1 || len(report.IncrementalSyncingPartitions) != 1 || report.IncrementalSyncingPartitions[0].Partition != "p2" {
		t.Fatalf("unexpected status: %+v", report)
	}
	if len(report.ActivePartitions) != 2 || report.Tables[0].TotalPartitions != 3 {
		t.Fatalf("unexpected active/table status: %+v", report)
	}
}

func TestStateWriteFailureIsFatal(t *testing.T) {
	state, err := openState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	key := stateKey("src", "dst", "events", "p1")
	state.Partitions[key] = &partitionState{SourceIdentity: "1:2"}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}
	if err := state.savePartition(key); !errors.Is(err, errStatePersistence) {
		t.Fatalf("expected fatal persistence error, got %v", err)
	}
}

func TestMatchTargetPartitionByRange(t *testing.T) {
	source := partition{Name: "p20260925080000", Range: "[types: [DATETIMEV2]; keys: [2026-09-25 08:00:00]; ..types: [DATETIMEV2]; keys: [2026-09-25 09:00:00]; )"}
	target := partition{Name: "historic_08", Range: "[types: [DATETIMEV2]; keys: [2026-09-25 08:00:00]; ..types: [DATETIMEV2]; keys: [2026-09-25 09:00:00]; )"}
	matched, ok, err := matchTargetPartition(source, []partition{target})
	if err != nil || !ok || matched.Name != "historic_08" {
		t.Fatalf("match=%+v found=%v err=%v", matched, ok, err)
	}
	wrongRange := partition{Name: source.Name, Range: "[types: [DATETIMEV2]; keys: [2026-09-25 10:00:00]; ..types: [DATETIMEV2]; keys: [2026-09-25 11:00:00]; )"}
	if _, _, err := matchTargetPartition(source, []partition{wrongRange}); err == nil {
		t.Fatal("same name with a different range must fail")
	}
}

func TestAutoPartitionDDL(t *testing.T) {
	for _, ddl := range []string{
		"AUTO PARTITION BY RANGE (date_trunc(`dt`, 'hour')) ()",
		"PARTITION BY RANGE (date_trunc(`dt`, 'hour')) ()",
		"AUTO PARTITION BY LIST (`tenant`) ()",
	} {
		if !isAutoPartitionDDL(ddl) {
			t.Fatalf("not detected: %s", ddl)
		}
	}
}

func TestOverwriteSQL(t *testing.T) {
	cfg := S3Options{Region: "cn-shanghai"}
	stmt := overwriteSQL("dst", "events", "actual_target_p1", "s3://b/p/*.parquet", []string{"id", "dt"}, false, cfg)
	if !strings.Contains(stmt, "INSERT OVERWRITE TABLE `dst`.`events` PARTITION(`actual_target_p1`)") || !strings.Contains(stmt, `"uri" = "s3://b/p/*.parquet"`) {
		t.Fatal(stmt)
	}
	empty := overwriteSQL("dst", "events", "p1", "", []string{"id", "dt"}, true, cfg)
	if !strings.Contains(empty, "FROM `dst`.`events` WHERE 1 = 0") || strings.Contains(empty, "s3(") {
		t.Fatal(empty)
	}
}

func TestTimeWindowSQL(t *testing.T) {
	cfg := S3Options{Region: "cn-shanghai", MaxFileSize: "1024MB"}
	where := " WHERE `event_time` > '2026-09-24 10:00:00.000000' AND `event_time` <= '2026-09-24 10:10:00.000000'"
	stmt := outfileWhereSQL("src", "events", "p1", "s3://bucket/full/p1/incremental/x/", []string{"event_time", "id"}, where, cfg)
	if !strings.Contains(stmt, "PARTITION(`p1`)"+where+" INTO OUTFILE") {
		t.Fatal(stmt)
	}
	insert := importLabeledSQL("dst", "events", "s3://bucket/full/p1/incremental/x/*.parquet", []string{"event_time", "id"}, "ps_123", cfg)
	if !strings.Contains(insert, "INSERT INTO `dst`.`events` WITH LABEL `ps_123` (`event_time`, `id`)") || !strings.Contains(insert, "*.parquet") {
		t.Fatal(insert)
	}
	if deltaLabel("key", "a", "b", "v", 0) == deltaLabel("key", "a", "b", "v", 1) {
		t.Fatal("retry labels must differ")
	}
}

func TestTimeFieldAndWatermark(t *testing.T) {
	if err := validateTimeField([]column{{Name: "event_time", Type: "DATETIMEV2(6)"}}, "event_time"); err != nil {
		t.Fatal(err)
	}
	if err := validateTimeField([]column{{Name: "event_time", Type: "VARCHAR(64)"}}, "event_time"); err == nil {
		t.Fatal("text time field accepted")
	}
	older, err := parseWatermark("2026-09-24 10:00:00.123456")
	if err != nil {
		t.Fatal(err)
	}
	newer, err := parseWatermark("2026-09-24 10:00:00.123457")
	if err != nil || !newer.After(older) {
		t.Fatalf("microsecond comparison failed: %v", err)
	}
	if _, err := parseWatermark("bad time"); err == nil {
		t.Fatal("invalid watermark accepted")
	}
}

func TestModeStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	state, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	state.Mode, state.TimeField = "time-window", "event_time"
	key := stateKey("src", "dst", "events", "p1")
	state.Partitions[key] = &partitionState{ImportedIdentity: "1:2", Watermark: "2026-09-24 10:00:00", WatermarkReady: true, PendingDelta: &deltaState{From: "2026-09-24 10:00:00", To: "2026-09-24 10:10:00", Label: "ps_abc"}}
	if err := state.saveMetadata(); err != nil {
		t.Fatal(err)
	}
	if err := state.savePartition(key); err != nil {
		t.Fatal(err)
	}
	if err := state.close(); err != nil {
		t.Fatal(err)
	}
	loaded, err := openState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.close()
	entry := loaded.Partitions[key]
	if loaded.Mode != "time-window" || loaded.TimeField != "event_time" || !entry.WatermarkReady || entry.PendingDelta.Label != "ps_abc" {
		t.Fatal("mode state did not persist")
	}
}
