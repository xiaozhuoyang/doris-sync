package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

type hourComparison struct {
	Hour       string `json:"hour"`
	SourceRows int64  `json:"sourceRows"`
	TargetRows int64  `json:"targetRows"`
	Difference int64  `json:"difference"`
}

type dayComparison struct {
	Date        string           `json:"date"`
	TimeZone    string           `json:"timeZone"`
	SourceTotal int64            `json:"sourceTotal"`
	TargetTotal int64            `json:"targetTotal"`
	Difference  int64            `json:"difference"`
	Hours       []hourComparison `json:"hours"`
}

type fullTableComparison struct {
	SourceRows int64 `json:"sourceRows"`
	TargetRows int64 `json:"targetRows"`
	Difference int64 `json:"difference"`
}

func compareFullTable(ctx context.Context, source, target Endpoint, metadataTimeout time.Duration, task statusFullTable) (fullTableComparison, error) {
	sourceDB, err := openDatabase(ctx, source, metadataTimeout)
	if err != nil {
		return fullTableComparison{}, fmt.Errorf("connect source: %w", err)
	}
	defer sourceDB.close()
	targetDB, err := openDatabase(ctx, target, metadataTimeout)
	if err != nil {
		return fullTableComparison{}, fmt.Errorf("connect target: %w", err)
	}
	defer targetDB.close()
	sourceRows, err := countTableRows(ctx, sourceDB, task.SourceDatabase, task.Table)
	if err != nil {
		return fullTableComparison{}, fmt.Errorf("count source rows: %w", err)
	}
	targetRows, err := countTableRows(ctx, targetDB, task.TargetDatabase, task.Table)
	if err != nil {
		return fullTableComparison{}, fmt.Errorf("count target rows: %w", err)
	}
	return fullTableComparison{SourceRows: sourceRows, TargetRows: targetRows, Difference: targetRows - sourceRows}, nil
}

func countTableRows(ctx context.Context, db *database, databaseName, table string) (int64, error) {
	rows, err := db.query(ctx, "SELECT COUNT(*) AS row_count FROM "+qtable(databaseName, table))
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, fmt.Errorf("expected one count row, got %d", len(rows))
	}
	count, err := strconv.ParseInt(rows[0]["rowcount"], 10, 64)
	if err != nil || count < 0 {
		return 0, fmt.Errorf("invalid row_count %q", rows[0]["rowcount"])
	}
	return count, nil
}

func compareTaskDay(ctx context.Context, source, target Endpoint, metadataTimeout time.Duration, task statusHourly, date string) (dayComparison, error) {
	loc, err := time.LoadLocation(task.TimeZone)
	if err != nil {
		return dayComparison{}, fmt.Errorf("task time zone: %w", err)
	}
	start, err := time.ParseInLocation("2006-01-02", date, loc)
	if err != nil || start.Format("2006-01-02") != date {
		return dayComparison{}, fmt.Errorf("--date must be YYYY-MM-DD")
	}
	end := start.AddDate(0, 0, 1)
	if end.Sub(start) != 24*time.Hour {
		return dayComparison{}, fmt.Errorf("date %s has a daylight-saving transition in %s; 24-hour comparison requires a fixed-offset day", date, task.TimeZone)
	}
	sourceDB, err := openDatabase(ctx, source, metadataTimeout)
	if err != nil {
		return dayComparison{}, fmt.Errorf("connect source: %w", err)
	}
	defer sourceDB.close()
	targetDB, err := openDatabase(ctx, target, metadataTimeout)
	if err != nil {
		return dayComparison{}, fmt.Errorf("connect target: %w", err)
	}
	defer targetDB.close()
	sourceCounts, err := countDayHours(ctx, sourceDB, task.SourceDatabase, task.Table, task.TimeField, start, end)
	if err != nil {
		return dayComparison{}, fmt.Errorf("count source rows: %w", err)
	}
	targetCounts, err := countDayHours(ctx, targetDB, task.TargetDatabase, task.Table, task.TimeField, start, end)
	if err != nil {
		return dayComparison{}, fmt.Errorf("count target rows: %w", err)
	}
	return buildDayComparison(date, task.TimeZone, sourceCounts, targetCounts), nil
}

func countDayHours(ctx context.Context, db *database, databaseName, table, field string, start, end time.Time) ([24]int64, error) {
	var counts [24]int64
	statement := "SELECT HOUR(" + ident(field) + ") AS hour_of_day, COUNT(*) AS row_count FROM " +
		qtable(databaseName, table) + hourlyWhere(field, start, end) +
		" GROUP BY HOUR(" + ident(field) + ") ORDER BY hour_of_day"
	rows, err := db.query(ctx, statement)
	if err != nil {
		return counts, err
	}
	for _, row := range rows {
		hour, err := strconv.Atoi(row["hourofday"])
		if err != nil || hour < 0 || hour > 23 {
			return counts, fmt.Errorf("invalid hour_of_day %q", row["hourofday"])
		}
		count, err := strconv.ParseInt(row["rowcount"], 10, 64)
		if err != nil || count < 0 {
			return counts, fmt.Errorf("invalid row_count %q", row["rowcount"])
		}
		counts[hour] = count
	}
	return counts, nil
}

func buildDayComparison(date, zone string, source, target [24]int64) dayComparison {
	result := dayComparison{Date: date, TimeZone: zone, Hours: make([]hourComparison, 24)}
	for hour := range result.Hours {
		result.Hours[hour] = hourComparison{
			Hour:       fmt.Sprintf("%02d:00", hour),
			SourceRows: source[hour],
			TargetRows: target[hour],
			Difference: target[hour] - source[hour],
		}
		result.SourceTotal += source[hour]
		result.TargetTotal += target[hour]
	}
	result.Difference = result.TargetTotal - result.SourceTotal
	return result
}
