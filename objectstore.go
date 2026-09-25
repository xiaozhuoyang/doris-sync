package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type objectStore struct {
	client  *s3.Client
	options S3Options
}

const objectRequestTimeout = 45 * time.Second

func openObjectStore(ctx context.Context, o S3Options) (*objectStore, error) {
	var configOptions []func(*awsconfig.LoadOptions) error
	configOptions = append(configOptions, awsconfig.WithRegion(o.Region))
	if o.AuthMode == "static" {
		configOptions = append(configOptions, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(o.AccessKey, o.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, err
	}
	if o.AuthMode == "iam" && o.RoleARN != "" {
		awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), o.RoleARN))
	}
	client := s3.NewFromConfig(awsCfg, func(opt *s3.Options) {
		opt.UsePathStyle = o.PathStyle
		endpoint := o.ClientEndpoint
		if endpoint == "" {
			endpoint = o.Endpoint
		}
		if endpoint != "" {
			opt.BaseEndpoint = aws.String(endpoint)
		}
	})
	return &objectStore{client: client, options: o}, nil
}

func (s *objectStore) partitionPrefix(sourceDB, targetDB, table, partition string) string {
	root := strings.Trim(s.options.Prefix, "/")
	return root + "/" + url.PathEscape(sourceDB) + "/" + url.PathEscape(targetDB) + "/" + url.PathEscape(table) + "/" + url.PathEscape(partition) + "/"
}

func backupGenerationPrefix(partitionPrefix string) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return partitionPrefix + "full/" + hex.EncodeToString(id[:]) + "/", nil
}

func (s *objectStore) uri(key string) string { return "s3://" + s.options.Bucket + "/" + key }

func (s *objectStore) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.options.Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		requestCtx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
		page, err := pager.NextPage(requestCtx)
		cancel()
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && obj.Size != nil && *obj.Size > 0 && strings.HasSuffix(strings.ToLower(*obj.Key), ".parquet") && !strings.Contains(strings.TrimPrefix(*obj.Key, prefix), "/") {
				keys = append(keys, *obj.Key)
			}
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *objectStore) clear(ctx context.Context, prefix string) error {
	root := strings.Trim(s.options.Prefix, "/")
	if root == "" || !strings.HasPrefix(prefix, root+"/") || strings.Count(strings.TrimPrefix(prefix, root+"/"), "/") < 4 {
		return fmt.Errorf("refusing to clear unsafe prefix %q", prefix)
	}
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.options.Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		requestCtx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
		page, err := pager.NextPage(requestCtx)
		cancel()
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			requestCtx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
			_, err := s.client.DeleteObject(requestCtx, &s3.DeleteObjectInput{Bucket: aws.String(s.options.Bucket), Key: obj.Key})
			cancel()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
