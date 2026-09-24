package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

func ident(s string) string          { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func qtable(db, table string) string { return ident(db) + "." + ident(table) }

var createPrefix = regexp.MustCompile("(?is)^\\s*CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`(?:``|[^`])+`\\.)?`(?:``|[^`])+`")

func targetDDL(ddl, db, table string) (string, error) {
	if !createPrefix.MatchString(ddl) {
		return "", fmt.Errorf("unsupported SHOW CREATE TABLE result")
	}
	return createPrefix.ReplaceAllStringFunc(ddl, func(_ string) string { return "CREATE TABLE " + qtable(db, table) }), nil
}

func outfileSQL(db, table, partition, uri string, columns []string, cfg S3Options) string {
	return outfileWhereSQL(db, table, partition, uri, columns, "", cfg)
}

func outfileWhereSQL(db, table, partition, uri string, columns []string, where string, cfg S3Options) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = ident(column)
	}
	props := s3SQLProperties(cfg)
	props["max_file_size"] = cfg.MaxFileSize
	return fmt.Sprintf("SELECT %s FROM %s PARTITION(%s)%s INTO OUTFILE %s FORMAT AS PARQUET PROPERTIES (%s)",
		strings.Join(quoted, ", "), qtable(db, table), ident(partition), where, sqlString(uri), sqlProperties(props))
}

func importSQL(db, table, uri string, columns []string, cfg S3Options) string {
	return importLabeledSQL(db, table, uri, columns, "", cfg)
}

func importLabeledSQL(db, table, uri string, columns []string, label string, cfg S3Options) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = ident(column)
	}
	props := s3SQLProperties(cfg)
	props["uri"] = uri
	props["format"] = "parquet"
	labelClause := ""
	if label != "" {
		labelClause = " WITH LABEL " + ident(label)
	}
	return fmt.Sprintf("INSERT INTO %s%s (%s) SELECT %s FROM s3(%s)", qtable(db, table), labelClause,
		strings.Join(quoted, ", "), strings.Join(quoted, ", "), sqlProperties(props))
}

func sqlLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func overwriteSQL(db, table, partition, uri string, columns []string, empty bool, cfg S3Options) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = ident(column)
	}
	selectSQL := "SELECT " + strings.Join(quoted, ", ")
	if empty {
		selectSQL += " FROM " + qtable(db, table) + " WHERE 1 = 0"
	} else {
		props := s3SQLProperties(cfg)
		props["uri"], props["format"] = uri, "parquet"
		selectSQL += " FROM s3(" + sqlProperties(props) + ")"
	}
	return "INSERT OVERWRITE TABLE " + qtable(db, table) + " PARTITION(" + ident(partition) + ") (" + strings.Join(quoted, ", ") + ") " + selectSQL
}

func s3SQLProperties(cfg S3Options) map[string]string {
	props := map[string]string{"s3.region": cfg.Region}
	if cfg.Endpoint != "" {
		props["s3.endpoint"] = cfg.Endpoint
	}
	if cfg.AuthMode == "static" {
		props["s3.access_key"] = cfg.AccessKey
		props["s3.secret_key"] = cfg.SecretKey
	} else if cfg.RoleARN != "" {
		props["s3.role_arn"] = cfg.RoleARN
	}
	return props
}

func sqlProperties(props map[string]string) string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]string, 0, len(keys))
	for _, key := range keys {
		items = append(items, sqlString(key)+" = "+sqlString(props[key]))
	}
	return strings.Join(items, ", ")
}

func sqlString(s string) string {
	return "\"" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\"", "\\\"") + "\""
}
