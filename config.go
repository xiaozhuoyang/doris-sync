package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

type Endpoint struct {
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	User     string   `json:"user"`
	Password string   `json:"password"`
	Cluster  string   `json:"cluster"`
	Session  []string `json:"session"`
}

type DatabasePair struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type S3Options struct {
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	Region         string `json:"region"`
	Endpoint       string `json:"endpoint"`
	ClientEndpoint string `json:"clientEndpoint"`
	PathStyle      bool   `json:"pathStyle"`
	AuthMode       string `json:"authMode"`
	AccessKey      string `json:"accessKey"`
	SecretKey      string `json:"secretKey"`
	RoleARN        string `json:"roleArn"`
	MaxFileSize    string `json:"maxFileSize"`
}

type Options struct {
	Source            Endpoint       `json:"source"`
	Target            Endpoint       `json:"target"`
	Databases         []DatabasePair `json:"databases"`
	S3                S3Options      `json:"s3"`
	StateFile         string         `json:"stateFile"`
	IncludeTables     string         `json:"includeTables"`
	ExcludeTables     string         `json:"excludeTables"`
	IncludePartitions string         `json:"includePartitions"`
	Partitions        []string       `json:"partitions"`
	MetadataTimeout   string         `json:"metadataTimeout"`
	Interval          string         `json:"interval"`
}

func loadOptions(path string) (Options, error) {
	f, err := os.Open(path)
	if err != nil {
		return Options{}, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var o Options
	if err := decoder.Decode(&o); err != nil {
		return o, err
	}
	if o.Source.Port == 0 {
		o.Source.Port = 9030
	}
	if o.Target.Port == 0 {
		o.Target.Port = 9030
	}
	if o.StateFile == "" {
		o.StateFile = "partition-sync-state.db"
	}
	if o.Interval == "" {
		o.Interval = "1h"
	}
	if o.MetadataTimeout == "" {
		o.MetadataTimeout = "10m"
	}
	if o.S3.MaxFileSize == "" {
		o.S3.MaxFileSize = "1024MB"
	}
	if o.S3.AuthMode == "" {
		o.S3.AuthMode = "static"
	}
	if o.Source.Host == "" || o.Target.Host == "" || o.Source.User == "" || o.Target.User == "" {
		return o, errors.New("source and target host/user are required")
	}
	if len(o.Databases) == 0 {
		return o, errors.New("at least one databases mapping is required")
	}
	for _, pair := range o.Databases {
		if pair.Source == "" || pair.Target == "" {
			return o, errors.New("each databases mapping needs source and target")
		}
	}
	if o.S3.Bucket == "" || o.S3.Region == "" {
		return o, errors.New("s3 bucket and region are required")
	}
	if strings.Trim(o.S3.Prefix, "/") == "" {
		return o, errors.New("s3 prefix must be a nonempty dedicated directory")
	}
	for _, value := range []*string{&o.Source.Password, &o.Target.Password, &o.S3.AccessKey, &o.S3.SecretKey} {
		resolved, err := resolveSecret(*value)
		if err != nil {
			return o, err
		}
		*value = resolved
	}
	if strings.EqualFold(o.S3.AuthMode, "static") && (o.S3.AccessKey == "" || o.S3.SecretKey == "") {
		return o, errors.New("static s3 auth requires accessKey and secretKey")
	}
	if o.S3.AuthMode != "static" && o.S3.AuthMode != "iam" {
		return o, fmt.Errorf("unsupported s3 authMode %q", o.S3.AuthMode)
	}
	if o.IncludeTables != "" {
		if _, err := regexp.Compile(o.IncludeTables); err != nil {
			return o, err
		}
	}
	if o.ExcludeTables != "" {
		if _, err := regexp.Compile(o.ExcludeTables); err != nil {
			return o, err
		}
	}
	if o.IncludePartitions != "" {
		if _, err := regexp.Compile(o.IncludePartitions); err != nil {
			return o, fmt.Errorf("includePartitions: %w", err)
		}
	}
	if len(o.Partitions) > 0 {
		if o.IncludePartitions != "" {
			return o, errors.New("partitions and includePartitions cannot both be set")
		}
		if err := validatePartitionNames(o.Partitions); err != nil {
			return o, err
		}
	}
	if o.Interval != "" {
		d, err := time.ParseDuration(o.Interval)
		if err != nil || d < time.Minute {
			return o, errors.New("interval must be a duration of at least 1m")
		}
	}
	if d, err := time.ParseDuration(o.MetadataTimeout); err != nil || d < time.Minute {
		return o, errors.New("metadataTimeout must be a duration of at least 1m")
	}
	return o, nil
}

func resolveSecret(value string) (string, error) {
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return value, nil
	}
	name := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
	result, ok := os.LookupEnv(name)
	if !ok || result == "" {
		return "", fmt.Errorf("environment variable %s is empty or unset", name)
	}
	return result, nil
}

func (o Options) allowsTable(name string) bool {
	if o.IncludeTables != "" && !regexp.MustCompile(o.IncludeTables).MatchString(name) {
		return false
	}
	return o.ExcludeTables == "" || !regexp.MustCompile(o.ExcludeTables).MatchString(name)
}

func (o Options) allowsPartition(name string) bool {
	if len(o.Partitions) > 0 {
		for _, part := range o.Partitions {
			if part == name {
				return true
			}
		}
		return false
	}
	return o.IncludePartitions == "" || regexp.MustCompile(o.IncludePartitions).MatchString(name)
}

func parsePartitionNames(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	names := strings.Split(raw, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	if err := validatePartitionNames(names); err != nil {
		return nil, err
	}
	return names, nil
}

func validatePartitionNames(names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" || strings.TrimSpace(name) != name {
			return errors.New("partition names must be nonempty and have no surrounding spaces")
		}
		if seen[name] {
			return fmt.Errorf("duplicate partition name %q", name)
		}
		seen[name] = true
	}
	return nil
}
