# journal_okx 分区修复与小时增量同步手册

适用版本：`doris-partition-sync v1.0.14`。本手册针对源、目标**不同实例**中同名的 `journal_okx` 库。若源、目标连接指向同一个实例的同一张表，不要执行覆盖计划。

## 1. 本次修复范围

依据 2026-10-01 至 2026-10-02 的两端分区行数采集文件，候选范围如下。两端不是同一时刻的快照；行数差异不直接证明数据丢失。执行前应复核最新行数，并确认所选分区没有持续写入。

| 表 | 行数不同 | 仅源端存在 | 本次计划分区数 | 备注 |
| --- | ---: | ---: | ---: | --- |
| `journal_history_position_v2` | 127 | 0 | 127 | 涉及 2024 年历史分区及 2026-09-30、2026-10-01；不是连续日期范围 |
| `journal_order_finish_v3` | 1 | 0 | 1 | `p20261001000000` |
| `position_v5` | 0 | 1 | 1 | `p20261001000000` |
| `position_v5_realtime` | 1 | 0 | 1 | `p20261001000000`，目标行数曾高于源端 |
| `top_trader` | 0 | 2 | 2 | `p20260929000000`、`p20260930000000` |
| `user_stat_summary` | 0 | 2 | 2 | `p20260929000000`、`p20260930000000` |
| **合计** | **129** | **5** | **134** | 完整清单见下方 JSON |

完整的、已校验无重复分区的计划文件是 [`overwrite-plan-journal-okx-20261002.json`](../overwrite-plan-journal-okx-20261002.json)。其中写的是**源端**分区名；目标端名称可以不同。工具先按范围找目标分区并使用实际目标名覆盖；目标范围缺失且目标表为 AUTO PARTITION 时，使用 `INSERT OVERWRITE ... PARTITION(*)` 自动建分区。目标表必须事先存在且列数、列名、顺序、类型与源端一致；工具不会删除目标表。

## 2. 前置检查

1. 确认源 FE、目标 FE 是预期的两个实例或互不共享表元数据的环境；确认各自有 `journal_okx` 库。
2. 确认源 FE/BE 可以向 OSS 写 OUTFILE，目标 FE/BE 可以从 OSS 读 Parquet，运行工具的 ECS 可以列举及删除独立 OSS 前缀。
3. 对“仅源端存在”的 5 个分区，确认目标表确实使用 AUTO PARTITION。否则覆盖预检查会拒绝执行。
4. 先在维护窗口复核差异，特别是仍可能写入的 2026-10-01 分区。若源端在备份过程中 visible version 变化，本次分区会失败，需待其稳定后重试。
5. 检查磁盘空间、OSS 容量、目标计算集群负载和 SQL 超时。127 个历史大分区会按时间顺序串行覆盖，可能运行较久。

可分别连接两端执行 `SHOW DATABASES LIKE 'journal_okx'`、`SHOW TABLES FROM journal_okx`、`SHOW CREATE TABLE journal_okx.<表名>` 和 `SHOW PARTITIONS FROM journal_okx.<表名>`。不要仅凭 `SHOW PARTITIONS` 的分区名判断对应关系，应比较 `Range`。对具体分区以 `SELECT COUNT(*) FROM journal_okx.<表名> PARTITION(<分区名>)` 复核源端行数；目标端先按 `Range` 找到对应分区再计数。

## 3. 安装程序

在本地取得 [v1.0.14 Linux amd64 安装包](../dist/doris-partition-sync-v1.0.14-linux-amd64.tar.gz)，传到运行工具的 ECS，例如：

```bash
scp dist/doris-partition-sync-v1.0.14-linux-amd64.tar.gz <ECS_USER>@<ECS_HOST>:/tmp/
```

在 ECS 上：

```bash
cd /tmp
tar -xzf doris-partition-sync-v1.0.14-linux-amd64.tar.gz
cd doris-partition-sync-v1.0.14-linux-amd64
cp partition-sync.example.json partition-sync.json
cp partition-sync.env.example partition-sync.env
chmod 600 partition-sync.env
```

把 `partition-sync.json` 改成以下结构。`<SOURCE_FE_HOST>` 和 `<TARGET_FE_HOST>` 必须替换为真实地址；不要把刚才验证 `minimax` 的测试实例地址误当作 `journal_okx` 源/目标实例。OSS 桶 `okx-test` 仅为已验证过的测试桶，正式运行前确认授权、容量及前缀策略。源、目标都叫 `journal_okx`，但通过不同的 `source`、`target` 连接区分。

```json
{
  "source": {
    "host": "<SOURCE_FE_HOST>",
    "port": 9030,
    "user": "root",
    "password": "${SOURCE_PASSWORD}",
    "session": ["query_timeout=7200"]
  },
  "target": {
    "host": "<TARGET_FE_HOST>",
    "port": 9030,
    "user": "root",
    "password": "${TARGET_PASSWORD}",
    "session": ["query_timeout=7200"]
  },
  "databases": [{"source": "journal_okx", "target": "journal_okx"}],
  "s3": {
    "bucket": "okx-test",
    "prefix": "journal-okx/hourly/run01",
    "region": "cn-shanghai",
    "endpoint": "https://oss-cn-shanghai-internal.aliyuncs.com",
    "clientEndpoint": "https://oss-cn-shanghai.aliyuncs.com",
    "pathStyle": false,
    "authMode": "static",
    "accessKey": "${S3_ACCESS_KEY}",
    "secretKey": "${S3_SECRET_KEY}",
    "maxFileSize": "1024MB"
  },
  "stateFile": "/var/lib/doris-partition-sync/partition-sync-state.db",
  "includeTables": "",
  "excludeTables": "",
  "includePartitions": "",
  "partitions": [],
  "metadataTimeout": "30m",
  "interval": "1m",
  "hourlyStart": "",
  "timeZone": "Asia/Shanghai",
  "managedTasks": true
}
```

`source.cluster`、`target.cluster` 可选；只有需要指定不同计算集群时才填。ECS 位于 OSS 内网时，`clientEndpoint` 可与 `endpoint` 相同；位于外网时保留可访问的公网 endpoint。若使用 IAM，请按实际云厂商权限改 `authMode`/`roleArn`，不要同时照搬静态 AK/SK 示例。

`partition-sync.env` 示例，填入真实值，**不要提交到 Git**：

```dotenv
SOURCE_PASSWORD='<填写源端密码>'
TARGET_PASSWORD='<填写目标端密码>'
S3_ACCESS_KEY='<填写 OSS AccessKey ID>'
S3_SECRET_KEY='<填写 OSS AccessKey Secret>'
SYNC_MODE=hourly-window
```

`managedTasks=true` 时不要设置 `TIME_FIELD`；时间字段在加入每张表的任务时指定。完成配置后安装：

```bash
sudo ./install.sh --config ./partition-sync.json --env ./partition-sync.env
sudo systemctl status doris-partition-sync --no-pager
sudo journalctl -u doris-partition-sync -n 50 --no-pager -o cat
sudo -u doris-partition-sync /opt/doris-partition-sync/doris-partition-sync task list \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db
```

安装脚本会自动启动 systemd 服务。**全新环境尚未添加任务时，`task list` 应返回 `[]`，服务只会周期扫描并打印 `tasks=0`，不会备份或导入任何表；不需要先停止服务。** 配置安装到 `/etc/doris-partition-sync/`，程序位于 `/opt/doris-partition-sync/`，常驻 SQLite 状态位于 `/var/lib/doris-partition-sync/`。

## 4. 执行一次性分区覆盖

**第一步：仅安装覆盖计划，不会执行同步。** 在安装包解压目录运行：

```bash
sudo install -m 0640 -o root -g doris-partition-sync \
  ./overwrite-plan-journal-okx-20261002.json \
  /etc/doris-partition-sync/overwrite-plan-journal-okx-20261002.json
```

`systemctl stop` 仅适用于**已经添加小时任务**的环境：若有任务正在写这些目标表，须先等当前 `HOURLY_IMPORT_START` 对应 `HOURLY_DONE`，再停服务；不要在 SQL 执行中途强停。全新环境 `tasks=0` 时跳过此操作。

**第二步：执行一次性覆盖。下面这条命令会真正改写目标分区数据。** 建议先复制计划 JSON，仅保留一张表的一个分区，配合独立的测试 state 文件和 OSS 前缀验证；确认无误后，再执行完整 134 分区计划。正式后台命令：

```bash
sudo -u doris-partition-sync sh -c '
  set -a
  . /etc/doris-partition-sync/partition-sync.env
  set +a
  nohup /opt/doris-partition-sync/doris-partition-sync \
    --config /etc/doris-partition-sync/partition-sync.json \
    --sync-mode overwrite --once --force-overwrite \
    --overwrite-plan /etc/doris-partition-sync/overwrite-plan-journal-okx-20261002.json \
    --state-file /var/lib/doris-partition-sync/journal-okx-overwrite-20261002.db \
    --s3-prefix journal-okx/overwrite/20261002 \
    > /var/lib/doris-partition-sync/journal-okx-overwrite-20261002.log 2>&1 < /dev/null &
  echo "overwrite PID=$!"
'
```

覆盖用的 SQLite 文件和 OSS 前缀必须与常驻小时服务各自不同，且两个 OSS 前缀不能互为子目录。不要并发对同一目标表执行覆盖或小时导入。覆盖任务会先预检查全部库表、列和选中分区，再按 JSON 中表的顺序、每张表分区范围从旧到新执行。遇到错误立即停止；不能把 `OVERWRITE_PLAN_PREFLIGHT` 当作已完成导入。

查看日志和状态：

```bash
tail -f /var/lib/doris-partition-sync/journal-okx-overwrite-20261002.log
sudo -u doris-partition-sync /opt/doris-partition-sync/doris-partition-sync \
  --status --state-file /var/lib/doris-partition-sync/journal-okx-overwrite-20261002.db
```

以日志中的 `OVERWRITE_PLAN_DONE tables=6 backed_up=134 imported=134` 为完整成功信号，并对两端重新做分区行数核对。任务失败时保留日志、状态库和备份对象；先确认错误及目标分区实际数据，再决定是否重试。再次执行同一 `--force-overwrite` 计划会重新导出并覆盖所选分区，不是跳过已完成的分区。不要在行数差异未解释前盲目反复覆盖。

## 5. 启动小时增量任务

覆盖完成、校验通过后再加入小时任务。全新安装时服务通常已在运行，可先确认：

```bash
sudo systemctl status doris-partition-sync --no-pager
```

如果之前为避免与已有任务冲突而停止了服务，此时再运行 `sudo systemctl start doris-partition-sync`。服务运行本身不会产生小时任务；只有执行下面的 `task add` 后才开始同步该表。

小时增量要求每张表选择一个 `DATETIME` 字段。先在源端 `DESC journal_okx.<表名>` 核对字段；清单未提供字段定义，本手册不猜测列名。再确认目标端准备开始的每个小时范围**没有已经导入的行**。小时模式使用普通 `INSERT`，对已有行的小时重放会重复写入（尤其是 Duplicate Key 表）。对不满足时间字段或目标空窗条件的表，不要直接添加小时任务。

可在源端和目标端分别执行以下 SQL，比较覆盖结束附近的小时行数；将占位符换成实际表名、时间列和时间范围：

```sql
SELECT DATE_FORMAT(<DATETIME_FIELD>, '%Y-%m-%d %H:00:00') AS hour_start,
       COUNT(*) AS row_count
FROM journal_okx.<TABLE_NAME>
WHERE <DATETIME_FIELD> >= '2026-10-01 00:00:00'
  AND <DATETIME_FIELD> <  '2026-10-03 00:00:00'
GROUP BY DATE_FORMAT(<DATETIME_FIELD>, '%Y-%m-%d %H:00:00')
ORDER BY hour_start;
```

结果里没有列出的空小时，还需用该小时的 `COUNT(*)` 直接验证为 0；不能只按最后一条结果推断后续小时都为空。

例如 `journal_history_position_v2` 经核实时间列为 `<DATETIME_FIELD>`，目标在 `<FIRST_EMPTY_HOUR>` 及之后没有已导入数据时：

```bash
sudo -u doris-partition-sync /opt/doris-partition-sync/doris-partition-sync task add \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db journal_okx --target-db journal_okx \
  --table journal_history_position_v2 \
  --time-field REAL_DATETIME_COLUMN \
  --start '2026-10-02 08:00:00' --time-zone Asia/Shanghai
```

上例的 `--start` **仅示意格式**，必须换成第一个待同步且目标端为空的整点小时。`--start` 表示第一个窗口的起点，不是任务启动时间。程序按 `[start, start+1h)` 处理，逐小时衔接，不跳过积压；每个已完成窗口推进 SQLite 中的 `nextStart`。当前实现保留一个完整小时的缓冲：例如北京时间 10:00，最新可处理的是 `[08:00,09:00)`，`[09:00,10:00)` 需到 11:00 才可处理。`interval=1m` 是扫描频率，不会缩短这个缓冲。如业务定义的“T+1 小时”是 10:00 立刻同步 `[09:00,10:00)`，当前版本不满足，需先调整程序规则。

运行状态：

```bash
sudo -u doris-partition-sync /opt/doris-partition-sync/doris-partition-sync task list \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db
sudo -u doris-partition-sync /opt/doris-partition-sync/doris-partition-sync task status \
  --state-file /var/lib/doris-partition-sync/partition-sync-state.db \
  --source-db journal_okx --target-db journal_okx --table journal_history_position_v2
sudo journalctl -u doris-partition-sync -f -o cat
```

`nextStart` 是第一个尚未提交的小时，`pendingEnd` 是当前待提交窗口的终点，`phase`/`lastError` 展示运行或故障状态。整小时完成以 `HOURLY_DONE` 和推进后的 `nextStart` 为准。指定一天做逐小时行数对比时，需给 `task status` 增加 `--config /etc/doris-partition-sync/partition-sync.json --date YYYY-MM-DD`，并在同一个命令环境里加载 `partition-sync.env` 中的密码变量；该查询可能扫描大量数据。

如果 SQL 提交后进程中断，结果可能不明确；程序会停在该小时要求人工核对。不要直接删除 SQLite 状态或回拨 `nextStart`，否则可能造成重复导入。小时任务不会自动补抓已经提交过的旧小时的迟到写入；这类变更应另行计划分区覆盖，并与小时任务错开执行。

## 6. 已验证与未验证

- v1.0.14 曾在 `minimax -> minimax_target` 的专用小测试表上完整验证 3 个分区的 OUTFILE、OSS、按实际目标分区名覆盖、`PARTITION(*)` 自动建分区和结果对比；测试表及独立 OSS 前缀已清理。
- 本文的 `journal_okx` 六张表、134 个分区**尚未在其实际源/目标环境执行**。刚才提供的测试实例只发现 `minimax` 与 `minimax_target`，不能直接用它运行本计划。
- 小时任务的每张表时间字段、首个空目标小时、源/目标 FE 地址及正式 OSS 权限仍需在实际环境中确认。
