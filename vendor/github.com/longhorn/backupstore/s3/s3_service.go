package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
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

	// AWSSignAcceptEncoding controls whether `Accept-Encoding` is included in the
	// SigV4 SignedHeaders set. Set it to "false" for an endpoint that is reached
	// through a proxy which alters the header in transit.
	AWSSignAcceptEncoding = "AWS_SIGN_ACCEPT_ENCODING"

	// AWSRetryMaxAttempts is the default maximum number of retry attempts for a single API operation that fails with a retryable error.
	AWSRetryMaxAttempts = 5
	// AWSRetryMaximumAttempts maximum number attempts that should be made.
	AWSRetryMaximumAttempts = 10
	// AWSRetryMaximumBackoff specifies the maximum duration between retried attempts.
	AWSRetryMaximumBackoff = 300 * time.Second

	// InvalidRequestErrorMsg is the error message returned by S3 Compatible services when the authorization mechanism is not supported,
	// which can be caused by using AWS Signature Version 2 for signing requests to AWS S3 regions that require AWS Signature Version 4.
	InvalidRequestErrorMsg = "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."

	// maxSinglePutObjectSize is the maximum object size (5 GiB) that S3 (and S3-compatible
	// providers) allow to be uploaded via a single PutObject request. Used by
	// PutObjectAsSinglePart to fail fast with a clear error instead of letting the
	// request reach S3 and come back as a raw EntityTooLarge error.
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
	maxSinglePutObjectSize int64 = 5 * 1024 * 1024 * 1024
)

// warnInvalidSignAcceptEncoding keeps the warning for a malformed
// AWS_SIGN_ACCEPT_ENCODING value out of the per-request path.
var warnInvalidSignAcceptEncoding sync.Once

// ignoreAcceptEncodingSigning reports whether `Accept-Encoding` must be excluded
// from the SigV4 SignedHeaders set for the given endpoint.
//
// aws-sdk-go-v2 sends `Accept-Encoding: identity` and, unlike v1, includes
// `accept-encoding` in SignedHeaders. Anything that alters the header between
// the client and the endpoint therefore breaks signature verification at the
// endpoint with SignatureDoesNotMatch.
// (https://github.com/aws/aws-sdk-go-v2/issues/1816 and https://github.com/rclone/rclone/issues/6670)
//
// Google Cloud Storage always alters the header (it appends gzip(gfe)), so it is
// detected by endpoint and never depends on the user setting. A reverse proxy or
// a CDN in front of an S3-compatible endpoint does the same thing (Cloudflare
// replaces the value with "gzip, br" by design) but cannot be detected from the
// endpoint. AWS_SIGN_ACCEPT_ENCODING=false lets the user exclude the header for
// those endpoints. It defaults to true, which keeps the existing behavior.
func ignoreAcceptEncodingSigning(endpoints string) bool {
	if strings.Contains(endpoints, "storage.googleapis.com") {
		return true
	}

	// The value comes from a Secret, which commonly carries a trailing newline.
	value := strings.TrimSpace(os.Getenv(AWSSignAcceptEncoding))
	if value == "" {
		return false
	}

	sign, err := strconv.ParseBool(value)
	if err != nil {
		// Warn once rather than per request, because a new client is built for
		// every S3 operation. Without this the user sees the same
		// SignatureDoesNotMatch failure the setting is meant to fix, with no
		// indication that the value was rejected.
		warnInvalidSignAcceptEncoding.Do(func() {
			log.Warnf("Invalid %v value %q, expecting a boolean. Keeping Accept-Encoding in the request signature.",
				AWSSignAcceptEncoding, value)
		})
		return false
	}
	return !sign
}

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
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
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
		// Remove `Accept-Encoding` from SignedHeaders for endpoints that alter it in
		// transit. ignoreSigningHeaders restores the header after signing, so the
		// request on the wire is unchanged.
		if ignoreAcceptEncodingSigning(endpoints) {
			ignoreSigningHeaders(o, []string{"Accept-Encoding"})
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
	var re *smithyhttp.ResponseError
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
	// manager.NewUploader defaults RequestChecksumCalculation to
	// aws.RequestChecksumCalculationWhenSupported, independent of the
	// RequestChecksumCalculation set on the underlying s3.Client. That default
	// makes the uploader always add a CRC32 trailing checksum for multipart
	// uploads, which forces the request body to use aws-chunked content
	// encoding. Some S3-compatible providers (e.g. OCI) don't support
	// aws-chunked and reject the request with "NotImplemented: AWS chunked
	// encoding is not supported". Align the uploader with the client's
	// WhenRequired setting so checksums (and aws-chunked encoding) are only
	// used when required.
	uploader := manager.NewUploader(svc, func(u *manager.Uploader) {
		u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	// Ensure reader is at the beginning
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return errors.Wrapf(err, "failed to seek reader to offset 0")
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	_, err = uploader.Upload(ctx, params)
	if err != nil {
		// If the error message contains InvalidRequestErrorMsg, it indicates that the S3-Compatible services do not support AWS Signature Version 4 and the request should be signed with AWS Signature Version 2.
		// In this case, we can fallback to a single-part upload which is compatible with AWS Signature Version 2.
		// Usually, this happens with the backup configuration file.
		if strings.Contains(err.Error(), InvalidRequestErrorMsg) {
			log.Debugf("Falling back to single-part upload for bucket/key: %v/%s", s.Bucket, key)
			return s.PutObjectSinglePart(ctx, svc, key, reader)
		}
		return errors.Wrapf(parseAwsError(err), "failed to put object: %v", key)
	}
	return nil
}

// PutObjectAsSinglePart uploads an object using a single PutObject request,
// bypassing the manager.Uploader (which switches to a multipart upload path
// whenever the payload exceeds the SDK's default 5 MiB PartSize).
//
// This is the safe path for backup metadata (e.g. backup_*.cfg, volume.cfg)
// whose size grows with the number of blocks in a backup and can therefore
// cross the 5 MiB threshold on large volumes. Some S3-interop providers
// (e.g. Google Cloud Storage) reject the resulting multipart request path
// with SignatureDoesNotMatch, even though a plain PutObject request of the
// same object succeeds. A single PutObject request supports objects up to
// 5 GiB, which is well above any metadata blob written here.
func (s *service) PutObjectAsSinglePart(ctx context.Context, key string, reader io.ReadSeeker) error {
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrapf(err, "failed to determine size of object: %v", key)
	}
	if size > maxSinglePutObjectSize {
		// This is not expected to happen for backup metadata or data blocks, but
		// fail fast with a clear, actionable error instead of an opaque
		// EntityTooLarge response from S3 if it ever does.
		return fmt.Errorf("failed to put object: %v: object size %v bytes exceeds the %v byte limit of a single PutObject request",
			key, size, maxSinglePutObjectSize)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return errors.Wrapf(err, "failed to seek reader to offset 0")
	}

	svc, err := s.newInstance(ctx, true)
	if err != nil {
		return err
	}
	defer s.Close()

	return s.PutObjectSinglePart(ctx, svc, key, reader)
}

// PutObjectSinglePart is a fallback method for PutObject when the error message contains InvalidRequestErrorMsg, which indicates that the S3-Compatible services do not support AWS Signature Version 4 and the request should be signed with AWS Signature Version 2.
// This method performs a single-part upload which is compatible with AWS Signature Version 2, but it only supports objects up to 5 GiB in size.
func (s *service) PutObjectSinglePart(ctx context.Context, svc *s3.Client, key string, reader io.ReadSeeker) error {
	// Ensure reader is at the beginning
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return errors.Wrapf(err, "failed to seek reader to offset 0")
	}

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	// The maximum object size of a single part upload to S3 is 5GB. If the object size exceeds this limit, multipart upload should be used.
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
	_, err := svc.PutObject(ctx, params)
	if err != nil {
		return errors.Wrapf(parseAwsError(err), "failed to put object in single part: %v", key)
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
		return nil, errors.Wrapf(parseAwsError(err), "failed to get object: %v", key)
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
