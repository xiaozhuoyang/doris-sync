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
sh deploy/build-package.sh v1.0.1
```

The resulting `dist/doris-partition-sync-v1.0.1-linux-amd64.tar.gz` contains the static executable, example config, environment template, README, and a systemd installer. SQLite is linked into the executable by the pure-Go driver; neither CGO nor a system `sqlite3` package is required to run it. The optional `sqlite3` CLI is only useful for manual state inspection.

On the Linux host, extract the package, prepare real config and credentials, then install and start the service:

```bash
tar -xzf doris-partition-sync-v1.0.1-linux-amd64.tar.gz
cd doris-partition-sync-v1.0.1-linux-amd64
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

For continuous checks, omit `--once`. Two modes are available:

```bash
# Default: re-export and atomically replace each changed partition.
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode overwrite --check-interval 1h

# Append-only data: export and import only rows after the saved time watermark.
./dist/doris-partition-sync-linux-amd64 --config ./partition-sync.json --sync-mode time-window --time-field event_time --check-interval 1h
```

The interval defaults to `1h`, can be set in the config as `interval`, and can be overridden with `--check-interval`. Every partition joins version checks as soon as its first import succeeds; other partitions can still be doing their initial sync. `--once` runs one complete scan. `stateFile` is a SQLite database (default `partition-sync-state.db`) and must be on persistent local storage. Only one process may use it at a time. Mode and time field are recorded in SQLite and cannot be changed on restart; use a separate state database and dedicated bucket prefix to start a different mode. `source.cluster` and `target.cluster` can select different SelectDB compute clusters; `session` contains `SET` assignments such as `query_timeout=7200`.

For AWS IAM, set `authMode` to `iam`, remove `accessKey` and `secretKey`, set `region` and optionally `roleArn`. The local process uses the AWS default credential chain or assumes `roleArn`; the FE uses `s3.role_arn` when it is configured. The FE/BE nodes must be authorized to read and write the bucket independently of the tool process.

If the SelectDB nodes use a private OSS/S3 endpoint but the sync process runs outside that network, set `s3.endpoint` to the private endpoint and `s3.clientEndpoint` to the public endpoint. OUTFILE and S3 TVF use `endpoint`; local object listing and cleanup use `clientEndpoint` (or `endpoint` when omitted).

## Workflow

1. `SHOW TABLES`, `SHOW CREATE TABLE`, `DESC`, and `SHOW PARTITIONS` discover the source. Async materialized views are skipped. Existing target tables are checked for matching columns; missing target tables are created from source DDL, including empty tables. For automatic partitioning, source partition instances are omitted from the target DDL so the target creates partitions as data arrives.
2. Partitions are sorted by range start (or name where no date range is available) and processed sequentially, oldest first.
3. Each partition uses `s3://bucket/prefix/source_db/target_db/table/partition/`, so separate target mappings never share backup objects. SQLite stores partition ID and `VisibleVersion`. Initial sync exports the full partition. The target partition is matched by the lower and upper bounds in `SHOW PARTITIONS.Range` when available, falling back to `PartitionName` if range bounds cannot be parsed.
4. In `overwrite` mode, a changed partition clears its backup prefix, reruns full OUTFILE, then atomically replaces only the matching target partition with one `INSERT OVERWRITE TABLE ... PARTITION(target_name)`. It does not truncate first. A missing target partition is restored using auto partitioning.
5. In `time-window` mode, initial full sync also records `MAX(time_field)` per partition. When its version changes, the tool exports `(previous_watermark, current_MAX]` to `.../partition/incremental/<batch-label>/` and imports the batch with a labeled INSERT. An interrupted import checks `SHOW LOAD` before retrying. If the maximum time does not advance, the tool falls back to full partition overwrite. A missing target partition is also fully restored.
6. State is written atomically after backup and import. A failed partition is logged, and the tool continues to other partitions. Unchanged partitions are skipped on later scans.

`BACKUP_START`, `BACKUP_DONE`, `OVERWRITE_START`, `IMPORT_DONE`, `DELTA_BACKUP_START`, `DELTA_IMPORT_START`, `DELTA_IMPORT_DONE`, `PARTITION_FAILED`, and `SUMMARY` are printed to stdout. SQLite stores mode, table schema hashes, partition versions, time watermarks where applicable, and object names; it does not contain passwords or access keys. Each table or partition checkpoint updates one database row. The SQLite database uses WAL journaling; back it up with SQLite's backup API or `sqlite3 partition-sync-state.db '.backup backup-state.db'`, not by copying only the live `.db` file.

Inspect saved versions directly:

```bash
sqlite3 partition-sync-state.db 'SELECT source_db, target_db, table_name, partition_name, backup_identity, imported_identity, watermark FROM partition_state ORDER BY source_db, target_db, table_name, partition_name LIMIT 20;'
```

## Constraints

- The target table must have compatible automatic partitioning. If a target partition cannot be matched by range or name after import, the tool stops for that partition to prevent duplicate writes.
- Column changes in an existing table require a schema migration before the next sync. The tool creates missing tables, but does not apply `ALTER TABLE` for existing tables.
- A dedicated bucket prefix is required. In `overwrite` mode, a changed partition prefix is cleared before re-export. In `time-window` mode, delta batches remain under the partition prefix; a full fallback replaces that backup and its delta history.
- Deleting the SQLite state database causes the tool to re-export and overwrite partitions. Keep its checkpoint when moving or restarting the process. The tool does not import legacy JSON state files.
- Validate OUTFILE, S3 TVF, and IAM permissions on one table before scaling to an entire database.
- The source partition must be stable between export and import. If its visible version changes during either step, the partition is retried on the next scan. Concurrent writes to the target partition during an overwrite are outside this tool's coordination.
- `time-window` mode assumes appended rows have a non-null, strictly increasing time value. It cannot detect older rows inserted at or before the saved watermark, nor updates/deletes of older rows when the maximum time also advances. Use `overwrite` mode for that workload. An ambiguous labeled import stops that partition for manual verification instead of risking duplicate rows.
