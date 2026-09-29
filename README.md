# Doris / SelectDB partition sync

Synchronizes table definitions and partition data from a source FE to a target FE through Parquet OUTFILE files in S3 or OSS. Async materialized views and other non-table objects are skipped.

## Build

```bash
mkdir -p dist
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o dist/doris-partition-sync-linux-amd64 .
```

## Linux installation package

Build a self-contained Linux amd64 package on macOS or Linux:

```bash
sh deploy/build-package.sh v1.0.8
```

The resulting `dist/doris-partition-sync-v1.0.8-linux-amd64.tar.gz` contains the static executable, example config, environment template, README, and a systemd installer. SQLite is linked into the executable by the pure-Go driver; neither CGO nor a system `sqlite3` package is required to run it. The optional `sqlite3` CLI is only useful for manual state inspection.

On the Linux host, extract the package, prepare real config and credentials, then install and start the service:

```bash
tar -xzf doris-partition-sync-v1.0.8-linux-amd64.tar.gz
cd doris-partition-sync-v1.0.8-linux-amd64
cp partition-sync.example.json partition-sync.json
cp partition-sync.env.example partition-sync.env
# Edit partition-sync.json and partition-sync.env for your source, target, bucket, and credentials.
sudo ./install.sh --config ./partition-sync.json --env ./partition-sync.env
```

The installer creates a dedicated unprivileged user, installs the program under `/opt/doris-partition-sync`, config and credentials under `/etc/doris-partition-sync`, and enables `doris-partition-sync.service`. The default relative `stateFile` is stored in `/var/lib/doris-partition-sync/partition-sync-state.db` because this is the service working directory. Use `journalctl -u doris-partition-sync -f` for logs, `systemctl stop doris-partition-sync` to stop it, and run the same installer again after changing the package or config. A custom absolute `stateFile` path must be writable by the service user. Do not run two service instances against one state database.

## Configure and run

Edit `partition-sync.example.json` for both FEs, database mappings, and a dedicated bucket prefix. Passwords and static S3 keys support `${ENV_NAME}` references. The source and target databases may differ; table names stay the same. On first use, set `includeTables` to an exact table regex, for example `^orders$`, and run a single cycle:

`databases` explicitly maps each source database to a target database, including a different name:

```json
"databases": [
  {"source": "source_db_a", "target": "target_db_a"},
  {"source": "source_db_b", "target": "archive_db_b"}
]
```

```bash
export SOURCE_PASSWORD='...'
export TARGET_PASSWORD='...'
export S3_ACCESS_KEY='...'
export S3_SECRET_KEY='...'
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode overwrite --once
```

For continuous checks, omit `--once`. Three modes are available:

```bash
# Default: re-export and atomically replace each changed partition.
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode overwrite --check-interval 1h

# Append-only data: export and import only rows after the saved time watermark.
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode time-window --time-field event_time --check-interval 1h

# Contiguous one-hour windows starting at an explicit wall-clock hour.
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode hourly-window \
  --time-field event_time --window-start '2026-09-29 08:00:00' --check-interval 1m
```

For `hourly-window`, set `timeZone` (for example `Asia/Shanghai`) and `hourlyStart` in the config, or pass `--window-start` to override the configured start. The start must be an exact hour in that time zone and remains fixed in SQLite after the first run. For systemd, set `SYNC_MODE=hourly-window` and `TIME_FIELD=event_time` in `partition-sync.env`, and set `hourlyStart`, `timeZone`, and `interval` in `partition-sync.json`. Use a new SQLite state file and dedicated bucket prefix when switching modes. Partition filters are not supported in this mode.

### Managed hourly tasks

To keep one service running and add tables without restarting it, configure the source/target FE connections, database mappings, S3 bucket, and a dedicated SQLite state file as above. Set `"managedTasks": true`, `"interval": "1m"`, and leave `hourlyStart`, `timeField`, and `includeTables` unset in the JSON config. Set `SYNC_MODE=hourly-window` in the installed `partition-sync.env` and leave `TIME_FIELD` unset. Then install/start the service with `install.sh`, or start it manually without `--once`:

```bash
./doris-partition-sync --config ./partition-sync.json --sync-mode hourly-window
```

Add a table while the service is running. The source/target database pair must appear in `databases` in the service config; each table chooses its own DATETIME field and exact-hour start. The service discovers new tasks on its next scan. Run these commands as the service user so the SQLite file remains writable:

```bash
/opt/doris-partition-sync/doris-partition-sync task add \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db source_db --target-db target_db --table orders \
  --time-field event_time --start '2026-09-29 08:00:00' --time-zone Asia/Shanghai

/opt/doris-partition-sync/doris-partition-sync task status \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db source_db --target-db target_db --table orders

/opt/doris-partition-sync/doris-partition-sync task list \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db
```

`task status` returns the saved `nextStart` (the first uncommitted hour), current `phase`, pending window, last completed window time, and last error. A new task starts as `queued`; the service then processes each eligible hour in order. To observe service logs, run `journalctl -u doris-partition-sync -f`. Keep the state file and bucket prefix when restarting so committed hours are not replayed. Managed and static hourly modes require separate state files.

For a small table without a useful time column or partitions, add a recurring whole-table overwrite task to the same managed service:

```bash
/opt/doris-partition-sync/doris-partition-sync task add --mode full-table --interval 1h \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db source_db --target-db target_db --table small_lookup

/opt/doris-partition-sync/doris-partition-sync task status \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db source_db --target-db target_db --table small_lookup
```

This task runs immediately after registration, then at least one hour after each successful overwrite (the service checks on its configured `interval`). It exports the entire source table to its own OSS prefix, verifies backup objects, and atomically replaces the whole target table with `INSERT OVERWRITE TABLE`. An empty source atomically empties the target. Failures leave the previous target contents intact and retry on the next service scan. `task status` shows `lastSuccessAt`, `nextDueAt`, `phase`, and `lastError`; `task list` includes both task types. Add `--config ./partition-sync.json` to `task status` for a live whole-table `COUNT(*)` on each side; the response includes `comparison.sourceRows`, `comparison.targetRows`, and `comparison.difference` (target minus source). This scan can be expensive on large tables. The previous full-table backup prefix is cleared before the next attempt, so this is synchronization rather than an archive of every hourly snapshot. Do not use this mode for tables requiring partition-only replacement or for tables too large to export and overwrite hourly. `--date` row comparison is available only for hourly-window tasks with a time field.

To compare source and target row counts for every hour of a particular date, add `--date` and `--config` to `task status`:

```bash
./doris-partition-sync task status --state-file ./partition-sync-state.db \
  --config ./partition-sync.json --source-db source_db --target-db target_db \
  --table orders --date 2026-09-29
```

The `dayComparison` JSON includes all 24 hours, zeros for empty hours, daily totals, and `difference = targetRows - sourceRows`. It runs one grouped count query against each database, using the task's time column and time zone. This is a live full-day count and may be expensive on large tables. Hours at or after `nextStart` have not necessarily been synchronized; a nonzero difference there is not by itself a failure. For an installed service, run the command as the service user with access to its SQLite file and provide the password environment variables from `partition-sync.env`.

To choose compute clusters independently for export and import, set `cluster` on each connection in `partition-sync.json`:

```json
"source": {"host": "source-fe", "port": 9030, "user": "root", "password": "${SOURCE_PASSWORD}", "cluster": "export_cluster"},
"target": {"host": "target-fe", "port": 9030, "user": "root", "password": "${TARGET_PASSWORD}", "cluster": "import_cluster"}
```

The tool executes `USE @cluster` on each connection before metadata, OUTFILE, import, or live status queries. Omit `cluster` to use the FE's default cluster. Cluster selection is per source/target connection, not per table task; restart the service after changing its config.

At 10:00 local time, the newest eligible window is `[08:00, 09:00)`. The tool processes all eligible hours from its saved `nextStart` in order; it never skips backlog to jump to the newest hour. Each window exports rows satisfying `time_field >= start AND time_field < end` across the table, then performs a labeled import. Empty windows are checkpointed too. The next start is persisted only after successful import verification, and an interrupted import is reconciled through `SHOW LOAD` before advancing. The field must be `DATETIME`; use a fixed-offset time zone such as `UTC` or `Asia/Shanghai`. The target must not already contain rows in the chosen initial time range, or append imports will duplicate them.

The interval defaults to `1h`, can be set in the config as `interval`, and can be overridden with `--check-interval`. Every partition joins version checks as soon as its first import succeeds; other partitions can still be doing their initial sync. `--once` runs one complete scan. `stateFile` is a SQLite database (default `partition-sync-state.db`) and must be on persistent local storage. Only one process may use it at a time. Mode and time field are recorded in SQLite and cannot be changed on restart; use a separate state database and dedicated bucket prefix to start a different mode. `source.cluster` and `target.cluster` can select different SelectDB compute clusters; `session` contains `SET` assignments such as `query_timeout=7200`.

Metadata reads (`SHOW TABLES`, `SHOW CREATE TABLE`, `DESC`, `SHOW PARTITIONS`) have a configurable `metadataTimeout`, defaulting to `10m`, and are retried up to three times on timeout. Long-running OUTFILE and import statements retain their separate connection timeout. For a one-partition test, set `includeTables` and `includePartitions` to anchored regular expressions; clear both filters for whole-database sync.

To select exact source partition names, set `"partitions": ["p20260901000000", "p20260901010000"]` in the config, or override the config selection at startup:

```bash
./doris-partition-sync --config ./partition-sync.json --sync-mode overwrite --once \
  --partitions p20260901000000,p20260901010000
```

`--partitions` replaces either config partition filter for that run. In the config, `partitions` and the existing `includePartitions` regex are mutually exclusive. Both initial sync and subsequent visible-version checks honor the selection, while partitions still run oldest first. Exact names not found in any selected source table cause an error instead of a silent no-op. `includeTables` can narrow the selection to one table. Remove the partition filter for whole-database sync. The status command's progress denominator remains the complete partition inventory, not only the selected subset.

Read a live SQLite checkpoint without connecting to SelectDB or stopping the sync process:

```bash
./dist/doris-partition-sync-linux-amd64 --status --state-file /var/lib/doris-partition-sync/partition-sync-state.db
./dist/doris-partition-sync-linux-amd64 --status --state-file /var/lib/doris-partition-sync/partition-sync-state.db | jq '{progressPercent, tables, incrementalSyncingPartitions}'
```

`currentPartitions` counts partitions whose recorded source and imported versions match. `incrementalMonitoring` counts imported partitions waiting for version changes; `incrementalSyncingPartitions` lists imported partitions with a pending version change or import checkpoint. `activePartitions` also includes first-time backup/import checkpoints. The report reflects persisted checkpoints rather than an instantaneous source version check. `knownTotalPartitions` comes from the latest table inventory and is populated when each table is scanned; if a table lacks inventory, `inventoryKnown` is false and the reported percentage is incomplete.

For AWS IAM, set `authMode` to `iam`, remove `accessKey` and `secretKey`, set `region` and optionally `roleArn`. The local process uses the AWS default credential chain or assumes `roleArn`; the FE uses `s3.role_arn` when it is configured. The FE/BE nodes must be authorized to read and write the bucket independently of the tool process.

If the SelectDB nodes use a private OSS/S3 endpoint but the sync process runs outside that network, set `s3.endpoint` to the private endpoint and `s3.clientEndpoint` to the public endpoint. OUTFILE and S3 TVF use `endpoint`; local object listing and cleanup use `clientEndpoint` (or `endpoint` when omitted).

In `hourly-window` mode, `--status` reports `hourlyStart`, `timeZone`, and one `hourlyWindows` entry per table. `nextStart` is the first uncommitted hour; `pendingEnd` and `phase` show an interrupted or in-progress window.

## Workflow

1. `SHOW TABLES` and `SHOW CREATE TABLE` discover the source. Async materialized views are skipped. All missing target tables are created before any partition data is copied, including empty tables. `DESC` and `SHOW PARTITIONS` validate and read each table during data sync. For automatic partitioning, source partition instances are omitted from the target DDL so the target creates partitions as data arrives.
2. Partitions are sorted by range start (or name where no date range is available) and processed sequentially, oldest first.
3. Each partition uses `s3://bucket/prefix/source_db/target_db/table/partition/`; each full backup attempt writes only to its own `full/<random-id>/` child. A stale or canceled OUTFILE can therefore never contaminate the next import. SQLite stores the chosen child prefix, partition ID and `VisibleVersion`. Initial sync exports the full partition. The target partition is matched by the lower and upper bounds in `SHOW PARTITIONS.Range` when available, falling back to `PartitionName` if range bounds cannot be parsed.
4. In `overwrite` mode, a changed partition clears its backup prefix, reruns full OUTFILE, then atomically replaces only the matching target partition with one `INSERT OVERWRITE TABLE ... PARTITION(target_name)`. It does not truncate first. A missing target partition is restored using auto partitioning.
5. In `time-window` mode, initial full sync also records `MAX(time_field)` per partition. When its version changes, the tool exports `(previous_watermark, current_MAX]` to `.../partition/incremental/<batch-label>/` and imports the batch with a labeled INSERT. An interrupted import checks `SHOW LOAD` before retrying. If the maximum time does not advance, the tool falls back to full partition overwrite. A missing target partition is also fully restored.
6. State is written atomically after backup and import. A failed partition is logged, and the tool continues to other partitions. Unchanged partitions are skipped on later scans.

`BACKUP_START`, `BACKUP_DONE`, `IMPORT_START`, `OVERWRITE_START`, `IMPORT_DONE`, `DELTA_BACKUP_START`, `DELTA_IMPORT_START`, `DELTA_IMPORT_DONE`, `PARTITION_FAILED`, and `SUMMARY` are printed to stdout. SQLite stores mode, table schema hashes, partition versions, time watermarks where applicable, and object names; it does not contain passwords or access keys. Each table or partition checkpoint updates one database row. The SQLite database uses WAL journaling; back it up with SQLite's backup API or `sqlite3 partition-sync-state.db '.backup backup-state.db'`, not by copying only the live `.db` file.

Inspect saved versions directly:

```bash
sqlite3 partition-sync-state.db 'SELECT source_db, target_db, table_name, partition_name, backup_identity, imported_identity, watermark FROM partition_state ORDER BY source_db, target_db, table_name, partition_name LIMIT 20;'
```

## Constraints

- `hourly-window` guarantees contiguous processed time ranges, not capture of late inserts or updates to an already completed hour. Rows committed after that hour has been checkpointed require a separate replay/overwrite strategy; do not rewind the SQLite cursor against a populated Duplicate Key target. A window should have settled before it becomes eligible. A daylight-saving-time zone can have repeated local hours; the tool stops at such a boundary instead of silently duplicating a window.

- The target table must have compatible automatic partitioning. If a target partition cannot be matched by range or name after import, the tool stops for that partition to prevent duplicate writes.
- Column changes in an existing table require a schema migration before the next sync. The tool creates missing tables, but does not apply `ALTER TABLE` for existing tables.
- A dedicated bucket prefix is required. In `overwrite` mode, a changed partition prefix is cleared before re-export. In `time-window` mode, delta batches remain under the partition prefix; a full fallback replaces that backup and its delta history.
- Deleting the SQLite state database causes the tool to re-export and overwrite partitions. Keep its checkpoint when moving or restarting the process. The tool does not import legacy JSON state files.
- Validate OUTFILE, S3 TVF, and IAM permissions on one table before scaling to an entire database.
- The source partition must be stable between export and import. If its visible version changes during either step, the partition is retried on the next scan. Concurrent writes to the target partition during an overwrite are outside this tool's coordination.
- `time-window` mode assumes appended rows have a non-null, strictly increasing time value. It cannot detect older rows inserted at or before the saved watermark, nor updates/deletes of older rows when the maximum time also advances. Use `overwrite` mode for that workload. An ambiguous labeled import stops that partition for manual verification instead of risking duplicate rows.
