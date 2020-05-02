package s3

import (
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

type Service struct {
	Region string
	Bucket string
	Client *http.Client
}

func (s *Service) New() (*s3.S3, error) {
	// get custom endpoint
	endpoints := os.Getenv("AWS_ENDPOINTS")
	config := &aws.Config{Region: &s.Region}
	if endpoints != "" {
		config.Endpoint = aws.String(endpoints)
		config.S3ForcePathStyle = aws.Bool(true)
	}

	if s.Client != nil {
		config.HTTPClient = s.Client
	}

	ses, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return s3.New(ses), nil
}

func (s *Service) Close() {
}

func parseAwsError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		message := fmt.Sprintln("AWS Error: ", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			message += fmt.Sprintln(reqErr.StatusCode(), reqErr.RequestID())
		}
		return fmt.Errorf(message)
	}
	return err
}

func (s *Service) ListObjects(key, delimiter string) ([]*s3.Object, []*s3.CommonPrefix, error) {
	svc, err := s.New()
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()
	// WARNING: Directory must end in "/" in S3, otherwise it may match
	// unintentially
	params := &s3.ListObjectsInput{
		Bucket:    aws.String(s.Bucket),
		Prefix:    aws.String(key),
		Delimiter: aws.String(delimiter),
	}
	resp, err := svc.ListObjects(params)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list objects with param: %+v response: %v error: %v",
			params, resp.String(), parseAwsError(err))
	}
	return resp.Contents, resp.CommonPrefixes, nil
}

func (s *Service) HeadObject(key string) (*s3.HeadObjectOutput, error) {
	svc, err := s.New()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	params := &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}
	resp, err := svc.HeadObject(params)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for object: %v response: %v error: %v",
			key, resp.String(), parseAwsError(err))
	}
	return resp, nil
}

func (s *Service) PutObject(key string, reader io.ReadSeeker) error {
	svc, err := s.New()
	if err != nil {
		return err
	}
	defer s.Close()

	params := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	resp, err := svc.PutObject(params)
	if err != nil {
		return fmt.Errorf("failed to put object: %v response: %v error: %v",
			key, resp.String(), parseAwsError(err))
	}
	return nil
}

func (s *Service) GetObject(key string) (io.ReadCloser, error) {
	svc, err := s.New()
	if err != nil {
		return nil, err
	}
	defer s.Close()

	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}

	resp, err := svc.GetObject(params)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %v response: %v error: %v",
			key, resp.String(), parseAwsError(err))
	}

	return resp.Body, nil
}

func (s *Service) DeleteObjects(keys []string) error {
	var keyList []string
	totalSize := 0
	for _, key := range keys {
		contents, _, err := s.ListObjects(key, "")
		if err != nil {
			return fmt.Errorf("failed to list object %v before removing it: %v", key, err)
		}
		size := len(contents)
		if size == 0 {
			continue
		}
		totalSize += size
		for _, obj := range contents {
			keyList = append(keyList, *obj.Key)
		}
	}

	svc, err := s.New()
	if err != nil {
		return fmt.Errorf("failed to get a new s3 client instance before removing objects: %v", err)
	}
	defer s.Close()

	identifiers := make([]*s3.ObjectIdentifier, totalSize)
	for i, k := range keyList {
		identifiers[i] = &s3.ObjectIdentifier{
			Key: aws.String(k),
		}
	}
	if len(identifiers) != 0 {
		quiet := true
		params := &s3.DeleteObjectsInput{
			Bucket: aws.String(s.Bucket),
			Delete: &s3.Delete{
				Objects: identifiers,
				Quiet:   &quiet,
			},
		}

		resp, err := svc.DeleteObjects(params)
		if err != nil {
			return fmt.Errorf("failed to delete objects with param: %+v response: %v error: %v", params,
				resp.String(), parseAwsError(err))
		}
	}
	return nil
}
