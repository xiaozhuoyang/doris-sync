package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type database struct {
	db      *sql.DB
	options Endpoint
}

type partition struct {
	Name    string
	Version string
	ID      string
	Range   string
}

type column struct{ Name, Type string }

func openDatabase(ctx context.Context, o Endpoint) (*database, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd = o.User, o.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", o.Host, o.Port)
	cfg.Timeout = 15 * time.Second
	cfg.ReadTimeout = 2 * time.Hour
	cfg.WriteTimeout = 2 * time.Hour
	cfg.InterpolateParams = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &database{db: db, options: o}, nil
}

func (d *database) close() { _ = d.db.Close() }

func (d *database) withConn(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if d.options.Cluster != "" {
		if !regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`).MatchString(d.options.Cluster) {
			return fmt.Errorf("invalid cluster name")
		}
		if _, err := conn.ExecContext(ctx, "USE @"+d.options.Cluster); err != nil {
			return err
		}
	}
	for _, assignment := range d.options.Session {
		if !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z_0-9]*\s*=`).MatchString(assignment) {
			return fmt.Errorf("invalid session assignment %q", assignment)
		}
		if _, err := conn.ExecContext(ctx, "SET "+assignment); err != nil {
			return err
		}
	}
	return fn(conn)
}

func (d *database) exec(ctx context.Context, statement string) error {
	return d.withConn(ctx, func(conn *sql.Conn) error { _, err := conn.ExecContext(ctx, statement); return err })
}

func (d *database) query(ctx context.Context, statement string) ([]map[string]string, error) {
	var result []map[string]string
	err := d.withConn(ctx, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, statement)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		for rows.Next() {
			values := make([]sql.NullString, len(cols))
			args := make([]any, len(cols))
			for i := range values {
				args[i] = &values[i]
			}
			if err := rows.Scan(args...); err != nil {
				return err
			}
			entry := make(map[string]string, len(cols))
			for i, col := range cols {
				if values[i].Valid {
					entry[normalizeHeader(col)] = values[i].String
				}
			}
			result = append(result, entry)
		}
		return rows.Err()
	})
	return result, err
}

func normalizeHeader(s string) string {
	return strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(s, "_", ""), " ", ""))
}

func (d *database) tables(ctx context.Context, databaseName string) ([]string, error) {
	rows, err := d.query(ctx, "SHOW TABLES FROM "+ident(databaseName))
	if err != nil {
		return nil, err
	}
	var result []string
	for _, row := range rows {
		for _, value := range row {
			result = append(result, value)
			break
		}
	}
	sort.Strings(result)
	return result, nil
}

func (d *database) createTableSQL(ctx context.Context, db, table string) (string, error) {
	rows, err := d.query(ctx, "SHOW CREATE TABLE "+qtable(db, table))
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("SHOW CREATE TABLE returned %d rows", len(rows))
	}
	ddl := rows[0]["createtable"]
	if ddl == "" {
		return "", fmt.Errorf("SHOW CREATE TABLE has no Create Table field")
	}
	return ddl, nil
}

func (d *database) columns(ctx context.Context, db, table string) ([]column, error) {
	rows, err := d.query(ctx, "DESC "+qtable(db, table))
	if err != nil {
		return nil, err
	}
	result := make([]column, 0, len(rows))
	for _, row := range rows {
		if row["field"] != "" {
			result = append(result, column{Name: row["field"], Type: row["type"]})
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("DESC returned no columns for %s.%s", db, table)
	}
	return result, nil
}

func (d *database) partitions(ctx context.Context, db, table string) ([]partition, error) {
	rows, err := d.query(ctx, "SHOW PARTITIONS FROM "+qtable(db, table))
	if err != nil {
		return nil, err
	}
	result := make([]partition, 0, len(rows))
	for _, row := range rows {
		name := row["partitionname"]
		version := row["visibleversion"]
		if name == "" || version == "" {
			return nil, fmt.Errorf("SHOW PARTITIONS lacks PartitionName or VisibleVersion for %s.%s", db, table)
		}
		if _, err := strconv.ParseInt(version, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid VisibleVersion %q: %w", version, err)
		}
		result = append(result, partition{Name: name, Version: version, ID: row["partitionid"], Range: row["range"]})
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := rangeOrderKey(result[i]), rangeOrderKey(result[j])
		if a == b {
			return result[i].Name < result[j].Name
		}
		return a < b
	})
	return result, nil
}

var rangeDate = regexp.MustCompile(`\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?`)
var rangeKeys = regexp.MustCompile(`(?i)keys:\s*\[([^\]]*)\]`)

func rangeOrderKey(p partition) string {
	if match := rangeDate.FindString(p.Range); match != "" {
		return match
	}
	return p.Name
}

func partitionRangeBounds(raw string) (string, bool) {
	matches := rangeKeys.FindAllStringSubmatch(raw, -1)
	if len(matches) != 2 {
		return "", false
	}
	parts := make([]string, 2)
	for i, match := range matches {
		parts[i] = strings.Trim(strings.TrimSpace(match[1]), `"' `)
	}
	return parts[0] + "|" + parts[1], true
}

func matchTargetPartition(source partition, targets []partition) (partition, bool, error) {
	sourceBounds, hasSourceBounds := partitionRangeBounds(source.Range)
	var sameName partition
	nameFound := false
	var sameRange partition
	rangeFound := false
	for _, target := range targets {
		if target.Name == source.Name {
			sameName, nameFound = target, true
		}
		if hasSourceBounds {
			if targetBounds, ok := partitionRangeBounds(target.Range); ok && targetBounds == sourceBounds {
				if rangeFound {
					return partition{}, false, fmt.Errorf("multiple target partitions match source range %s", sourceBounds)
				}
				sameRange, rangeFound = target, true
			}
		}
	}
	if rangeFound {
		return sameRange, true, nil
	}
	if nameFound {
		if hasSourceBounds {
			if targetBounds, ok := partitionRangeBounds(sameName.Range); ok && targetBounds != sourceBounds {
				return partition{}, false, fmt.Errorf("partition %s has different source and target ranges", source.Name)
			}
		}
		return sameName, true, nil
	}
	return partition{}, false, nil
}

func isAsyncView(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "show create materialized view") || strings.Contains(message, "async materialized view")
}
