package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/cockroachdb/errors"

	smithyhttp "github.com/aws/smithy-go/transport/http"
	bhttp "github.com/longhorn/backupstore/http"
)

type service struct {
	Region string
	Bucket string
	Client *http.Client
}

const (
	VirtualHostedStyle = "VIRTUAL_HOSTED_STYLE"

	// AWSRetryMaxAttempts is the default maximum number of retry attempts for a single API operation that fails with a retryable error.
	AWSRetryMaxAttempts = 5
	// AWSRetryMaximumAttempts maximum number attempts that should be made.
	AWSRetryMaximumAttempts = 10
	// AWSRetryMaximumBackoff specifies the maximum duration between retried attempts.
	AWSRetryMaximumBackoff = 300 * time.Second
)

func newService(u *url.URL) (*service, error) {
	s := service{}
	if u.User != nil {
		s.Region = u.Host
		s.Bucket = u.User.Username()
	} else {
		//We would depends on AWS_REGION environment variable
		s.Bucket = u.Host
	}

	// add custom ca to http client that is used by s3 service
	customCerts := getCustomCerts()
	client, err := bhttp.GetClientWithCustomCerts(customCerts)
	if err != nil {
		return nil, err
	}

	if tr, ok := client.Transport.(*http.Transport); ok {
		transfermanager.WithRoundRobinDNS()(tr)
	}

	s.Client = client

	return &s, nil
}

func (s *service) newInstance(ctx context.Context, retryBackoff bool) (*s3.Client, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s.Region),
		config.WithRetryMaxAttempts(AWSRetryMaxAttempts),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
	)
	if err != nil {
		return nil, err
	}
	// get custom endpoint
	endpoints := os.Getenv("AWS_ENDPOINTS")
	if endpoints != "" {
		cfg.BaseEndpoint = aws.String(endpoints)
	}

	usePathStyle := false
	virtualHostedStyleEnabled := os.Getenv(VirtualHostedStyle)
	if virtualHostedStyleEnabled == "true" {
		usePathStyle = false
	} else if virtualHostedStyleEnabled == "false" {
		usePathStyle = true
	} else if endpoints != "" {
		usePathStyle = true
	}

	if s.Client != nil {
		cfg.HTTPClient = s.Client
	}

	// Create S3 client with options
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		if retryBackoff {
			o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
				so.MaxAttempts = AWSRetryMaximumAttempts
				so.MaxBackoff = AWSRetryMaximumBackoff
			})
		}
	}), nil
}

func (s *service) Close() {
}

func parseAwsError(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		message := fmt.Sprintf("AWS Error: %s %s", ae.ErrorCode(), ae.ErrorMessage())
		return fmt.Errorf("%s", message)
	}
	// Try to extract HTTP status code and request ID if available
	var re smithyhttp.ResponseError
	if errors.As(err, &re) {
		return fmt.Errorf("AWS HTTP Error: %d %v", re.HTTPStatusCode(), re.Err)
	}

	// Check for operation errors (includes operation context)
	var oe *smithy.OperationError
	if errors.As(err, &oe) {
		return fmt.Errorf("AWS Operation Error %s %v", oe.Operation(), oe.Err)
	}

	return err
}

func (s *service) ListObjects(ctx context.Context, key, delimiter string) ([]types.Object, []types.CommonPrefix, error) {
	svc, err := s.newInstance(ctx, false)
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()
	// WARNING: Directory must end in "/" in S3, otherwise it may match
	// unintentionally
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.Bucket),
		Prefix:    aws.String(key),
		Delimiter: aws.String(delimiter),
	}

	var (
		objects        []types.Object
		commonPrefixes []types.CommonPrefix
	)
	paginator := s3.NewListObjectsV2Paginator(svc, params)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list objects with param: %+v error: %v",
				params, parseAwsError(err))
		}
		objects = append(objects, page.Contents...)
		commonPrefixes = append(commonPrefixes, page.CommonPrefixes...)
	}

	return objects, commonPrefixes, nil
}

func (s *service) HeadObject(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	svc, err := s.newInstance(ctx, false)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}
	resp, err := svc.HeadObject(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for object: %v error: %v", key, parseAwsError(err))
	}
	return resp, nil
}

func (s *service) PutObject(ctx context.Context, key string, reader io.ReadSeeker) error {
	svc, err := s.newInstance(ctx, true)
	if err != nil {
		return err
	}
	defer s.Close()

	// Use the AWS S3 uploader which handles signing correctly
	uploader := manager.NewUploader(svc)

	// Ensure reader is at the beginning
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek reader to start: %v", err)
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	_, err = uploader.Upload(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to put object: %v error: %v", key, parseAwsError(err))
	}
	return nil
}

func (s *service) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	svc, err := s.newInstance(ctx, false)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}

	resp, err := svc.GetObject(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %v error: %v", key, parseAwsError(err))
	}

	return resp.Body, nil
}

func (s *service) DeleteObjects(ctx context.Context, key string) error {

	objects, _, err := s.ListObjects(ctx, key, "")
	if err != nil {
		return errors.Wrapf(err, "failed to list objects with prefix %v before removing them", key)
	}

	svc, err := s.newInstance(ctx, false)
	if err != nil {
		return errors.Wrap(err, "failed to get a new s3 client instance before removing objects")
	}
	defer s.Close()

	var deletionFailures []string
	for _, object := range objects {
		_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    object.Key,
		})

		if err != nil {
			log.Errorf("Failed to delete object: %v error: %v", aws.ToString(object.Key), parseAwsError(err))
			deletionFailures = append(deletionFailures, aws.ToString(object.Key))
		}
	}

	if len(deletionFailures) > 0 {
		return fmt.Errorf("failed to delete objects %v", deletionFailures)
	}

	return nil
}
