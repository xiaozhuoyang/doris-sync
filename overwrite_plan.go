package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type overwritePlan struct {
	Tables []overwriteTable `json:"tables"`
}

type overwriteTable struct {
	SourceDatabase string   `json:"sourceDatabase"`
	TargetDatabase string   `json:"targetDatabase"`
	Table          string   `json:"table"`
	Partitions     []string `json:"partitions"`
}

func loadOverwritePlan(path string, mappings []DatabasePair) (*overwritePlan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var plan overwritePlan
	if err := decoder.Decode(&plan); err != nil {
		return nil, fmt.Errorf("overwrite plan: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("overwrite plan must contain exactly one JSON object")
	}
	if err := validateOverwritePlan(plan, mappings); err != nil {
		return nil, err
	}
	return &plan, nil
}

func validateOverwritePlan(plan overwritePlan, mappings []DatabasePair) error {
	if len(plan.Tables) == 0 {
		return fmt.Errorf("overwrite plan requires at least one table")
	}
	seen := make(map[string]bool)
	seenTargets := make(map[string]bool)
	for i, item := range plan.Tables {
		if item.SourceDatabase == "" || item.TargetDatabase == "" || item.Table == "" ||
			strings.TrimSpace(item.SourceDatabase) != item.SourceDatabase ||
			strings.TrimSpace(item.TargetDatabase) != item.TargetDatabase ||
			strings.TrimSpace(item.Table) != item.Table {
			return fmt.Errorf("overwrite plan tables[%d] requires exact sourceDatabase, targetDatabase and table names", i)
		}
		pairFound := false
		for _, pair := range mappings {
			if pair.Source == item.SourceDatabase && pair.Target == item.TargetDatabase {
				pairFound = true
				break
			}
		}
		if !pairFound {
			return fmt.Errorf("overwrite plan tables[%d]: database mapping %s -> %s is not configured", i, item.SourceDatabase, item.TargetDatabase)
		}
		if len(item.Partitions) == 0 {
			return fmt.Errorf("overwrite plan tables[%d]: partitions must not be empty", i)
		}
		if err := validatePartitionNames(item.Partitions); err != nil {
			return fmt.Errorf("overwrite plan tables[%d]: %w", i, err)
		}
		key := stateKey(item.SourceDatabase, item.TargetDatabase, item.Table, "")
		if seen[key] {
			return fmt.Errorf("overwrite plan contains duplicate table %s.%s -> %s.%s", item.SourceDatabase, item.Table, item.TargetDatabase, item.Table)
		}
		targetKey := stateKey("", item.TargetDatabase, item.Table, "")
		if seenTargets[targetKey] {
			return fmt.Errorf("overwrite plan targets %s.%s more than once", item.TargetDatabase, item.Table)
		}
		seen[key] = true
		seenTargets[targetKey] = true
	}
	return nil
}

func (s *syncer) cycleOverwritePlan(ctx context.Context, plan overwritePlan) error {
	type prepared struct {
		item         overwriteTable
		pair         DatabasePair
		targetTables map[string]bool
	}
	var ready []prepared
	var summary stats
	s.logger.Printf("OVERWRITE_PLAN_START tables=%d", len(plan.Tables))
	for _, item := range plan.Tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		pair := DatabasePair{Source: item.SourceDatabase, Target: item.TargetDatabase}
		sourceTables, err := s.source.tables(ctx, pair.Source)
		if err != nil {
			return fmt.Errorf("preflight %s.%s: %w", pair.Source, item.Table, err)
		}
		if !containsName(sourceTables, item.Table) {
			return fmt.Errorf("preflight %s.%s: source table does not exist", pair.Source, item.Table)
		}
		targetNames, err := s.target.tables(ctx, pair.Target)
		if err != nil {
			return fmt.Errorf("preflight %s.%s: %w", pair.Target, item.Table, err)
		}
		targetTables := map[string]bool{item.Table: containsName(targetNames, item.Table)}
		s.options.Partitions = item.Partitions
		clear(s.seenPartitions)
		summary.tables++
		valid, err := s.ensureTargetTable(ctx, pair, item.Table, targetTables, &summary)
		if err != nil {
			return fmt.Errorf("preflight %s.%s: %w", pair.Source, item.Table, err)
		}
		if !valid {
			return fmt.Errorf("preflight %s.%s: only OLAP tables can be force-overwritten", pair.Source, item.Table)
		}
		for _, name := range item.Partitions {
			if !s.seenPartitions[name] {
				return fmt.Errorf("preflight %s.%s: source partition %s does not exist", pair.Source, item.Table, name)
			}
		}
		columns, err := s.source.columns(ctx, pair.Source, item.Table)
		if err != nil {
			return err
		}
		columnJSON, _ := json.Marshal(columns)
		hash := sha256.Sum256(columnJSON)
		key := stateKey(pair.Source, pair.Target, item.Table, "")
		if previous := s.state.Tables[key]; previous != "" && previous != hex.EncodeToString(hash[:]) {
			return fmt.Errorf("preflight %s.%s: source schema changed; migrate target table before continuing", pair.Source, item.Table)
		}
		ready = append(ready, prepared{item: item, pair: pair, targetTables: targetTables})
		s.logger.Printf("OVERWRITE_PLAN_PREFLIGHT database=%s table=%s partitions=%d", pair.Source, item.Table, len(item.Partitions))
	}
	for i, job := range ready {
		s.options.Partitions = job.item.Partitions
		s.logger.Printf("OVERWRITE_PLAN_TABLE_START table=%d/%d database=%s target_database=%s name=%s partitions=%d", i+1, len(ready), job.pair.Source, job.pair.Target, job.item.Table, len(job.item.Partitions))
		if err := s.syncTable(ctx, job.pair, job.item.Table, job.targetTables, &summary); err != nil {
			s.logger.Printf("OVERWRITE_PLAN_TABLE_FAILED database=%s table=%s error=%q", job.pair.Source, job.item.Table, err)
			return fmt.Errorf("forced overwrite %s.%s: %w", job.pair.Source, job.item.Table, err)
		}
		s.logger.Printf("OVERWRITE_PLAN_TABLE_DONE database=%s table=%s", job.pair.Source, job.item.Table)
	}
	s.logger.Printf("OVERWRITE_PLAN_DONE tables=%d backed_up=%d imported=%d", len(ready), summary.backedUp, summary.imported)
	return nil
}

func containsName(names []string, expected string) bool {
	for _, name := range names {
		if name == expected {
			return true
		}
	}
	return false
}
