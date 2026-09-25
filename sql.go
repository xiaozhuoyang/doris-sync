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
	result := createPrefix.ReplaceAllStringFunc(ddl, func(_ string) string { return "CREATE TABLE " + qtable(db, table) })
	return omitAutoPartitionDefinitions(result)
}

var autoPartitionClause = regexp.MustCompile(`(?i)\bAUTO\s+PARTITION\s+BY\s+(?:RANGE|LIST)\b`)
var distributionClause = regexp.MustCompile(`(?im)^[ \t]*DISTRIBUTED[ \t]+BY\b`)

func omitAutoPartitionDefinitions(ddl string) (string, error) {
	match := autoPartitionClause.FindStringIndex(ddl)
	if match == nil {
		return ddl, nil
	}
	expressionStart := strings.IndexByte(ddl[match[1]:], '(')
	if expressionStart < 0 {
		return "", fmt.Errorf("AUTO PARTITION expression is missing")
	}
	expressionEnd, err := closingParen(ddl, match[1]+expressionStart)
	if err != nil {
		return "", err
	}
	definitionStart := expressionEnd + 1
	for definitionStart < len(ddl) && (ddl[definitionStart] == ' ' || ddl[definitionStart] == '\n' || ddl[definitionStart] == '\r' || ddl[definitionStart] == '\t') {
		definitionStart++
	}
	if definitionStart >= len(ddl) || ddl[definitionStart] != '(' {
		return "", fmt.Errorf("AUTO PARTITION definitions are missing")
	}
	distribution := distributionClause.FindStringIndex(ddl[definitionStart:])
	if distribution == nil {
		return "", fmt.Errorf("AUTO PARTITION definitions have no DISTRIBUTED BY boundary")
	}
	distributionStart := definitionStart + distribution[0]
	definitions := strings.TrimSpace(ddl[definitionStart:distributionStart])
	if !strings.HasPrefix(definitions, "(") || !strings.HasSuffix(definitions, ")") {
		return "", fmt.Errorf("unexpected AUTO PARTITION definitions")
	}
	return ddl[:definitionStart] + "()\n" + ddl[distributionStart:], nil
}

func closingParen(sql string, start int) (int, error) {
	depth := 0
	quote := byte(0)
	for i := start; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == '\\' && i+1 < len(sql) {
				i++
			} else if c == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, fmt.Errorf("unclosed parenthesis in AUTO PARTITION clause")
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
