package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

func runTaskCommand(args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: task add|status|list [flags]")
	}
	command := args[0]
	if command != "add" && command != "status" && command != "list" {
		return fmt.Errorf("unknown task command %q", command)
	}
	flags := flag.NewFlagSet("task "+command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateFile := flags.String("state-file", "partition-sync-state.db", "SQLite state file")
	configFile := flags.String("config", "", "configuration file for live row comparison")
	date := flags.String("date", "", "compare source and target rows for YYYY-MM-DD")
	sourceDB := flags.String("source-db", "", "source database")
	targetDB := flags.String("target-db", "", "target database")
	table := flags.String("table", "", "table name")
	timeField := flags.String("time-field", "", "DATETIME column")
	start := flags.String("start", "", "first hourly window start")
	zone := flags.String("time-zone", "UTC", "wall-clock time zone")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if *date != "" && (command != "status" || *configFile == "") {
		return fmt.Errorf("--date requires task status and --config")
	}
	if command == "add" {
		state, err := openState(*stateFile)
		if err != nil {
			return err
		}
		defer state.close()
		task := hourlyTask{Pair: DatabasePair{Source: *sourceDB, Target: *targetDB}, Table: *table, TimeField: *timeField, Start: *start, TimeZone: *zone}
		if err := state.addHourlyTask(task); err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "TASK_ADDED source=%s target=%s table=%s start=%s time_field=%s time_zone=%s\n", *sourceDB, *targetDB, *table, *start, *timeField, *zone)
		return err
	}
	report, err := readStatus(*stateFile)
	if err != nil {
		return err
	}
	if !report.ManagedTasks {
		return fmt.Errorf("state file is not in managedTasks mode")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if command == "list" {
		return encoder.Encode(report.HourlyWindows)
	}
	if *sourceDB == "" || *targetDB == "" || *table == "" {
		return fmt.Errorf("task status requires --source-db, --target-db, and --table")
	}
	for _, item := range report.HourlyWindows {
		if item.SourceDatabase == *sourceDB && item.TargetDatabase == *targetDB && item.Table == *table {
			if *date != "" {
				opts, err := loadOptions(*configFile)
				if err != nil {
					return err
				}
				if !opts.ManagedTasks {
					return fmt.Errorf("configuration must enable managedTasks")
				}
				allowed := false
				for _, pair := range opts.Databases {
					if pair.Source == *sourceDB && pair.Target == *targetDB {
						allowed = true
						break
					}
				}
				if !allowed {
					return fmt.Errorf("database mapping %s -> %s is not in configuration", *sourceDB, *targetDB)
				}
				metadataTimeout, _ := time.ParseDuration(opts.MetadataTimeout)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
				defer cancel()
				comparison, err := compareTaskDay(ctx, opts.Source, opts.Target, metadataTimeout, item, *date)
				if err != nil {
					return err
				}
				item.DayComparison = &comparison
			}
			return encoder.Encode(item)
		}
	}
	return fmt.Errorf("task not found: %s.%s -> %s", *sourceDB, *table, *targetDB)
}
