package azblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/cockroachdb/errors"

	azblobsvc "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/longhorn/backupstore/http"
)

const (
	azureURL           = "core.windows.net"
	azureConnNameKey   = "AccountName=%s;AccountKey=%s;"
	blobEndpoint       = "BlobEndpoint=%s;"
	blobEndpointScheme = "DefaultEndpointsProtocol=%s;"
	blobEndpointSuffix = "EndpointSuffix=%s;"
)

type service struct {
	Container       string
	EndpointSuffix  string
	ContainerClient *container.Client
}

func newService(u *url.URL) (*service, error) {
	s := service{}
	if u.User != nil {
		s.EndpointSuffix = u.Host
		s.Container = u.User.Username()
	} else {
		s.Container = u.Host
	}

	accountName := os.Getenv("AZBLOB_ACCOUNT_NAME")
	accountKey := os.Getenv("AZBLOB_ACCOUNT_KEY")
	azureEndpoint := os.Getenv("AZBLOB_ENDPOINT")

	connStr := fmt.Sprintf(azureConnNameKey, accountName, accountKey)
	if azureEndpoint != "" {
		blobEndpointURL := fmt.Sprintf("%s/%s", strings.TrimRight(azureEndpoint, "/"), accountName)
		endPointURL, err := url.Parse(azureEndpoint)
		if err != nil {
			return nil, err
		}
		connStr = fmt.Sprintf(blobEndpointScheme+connStr+blobEndpoint, endPointURL.Scheme, blobEndpointURL)
	}

	if s.EndpointSuffix != azureURL {
		connStr = connStr + fmt.Sprintf(blobEndpointSuffix, s.EndpointSuffix)
	}

	customCerts := getCustomCerts()
	httpClient, err := http.GetClientWithCustomCerts(customCerts)
	if err != nil {
		return nil, err
	}
	opts := azblobsvc.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: httpClient}}
	serviceClient, err := azblobsvc.NewClientFromConnectionString(connStr, &opts)
	if err != nil {
		return nil, err
	}

	s.ContainerClient = serviceClient.NewContainerClient(s.Container)
	return &s, nil
}

func getCustomCerts() []byte {
	// Certificates in PEM format (base64)
	certs := os.Getenv("AZBLOB_CERT")
	if certs == "" {
		return nil
	}

	return []byte(certs)
}

func (s *service) listBlobs(prefix, delimiter string) (*[]string, error) {
	listOptions := &container.ListBlobsHierarchyOptions{Prefix: &prefix}
	pager := s.ContainerClient.NewListBlobsHierarchyPager(delimiter, listOptions)

	var blobs []string
	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, v := range page.Segment.BlobItems {
			blobs = append(blobs, *v.Name)
		}
		for _, v := range page.Segment.BlobPrefixes {
			blobs = append(blobs, *v.Name)
		}
	}

	return &blobs, nil
}

func (s *service) getBlobProperties(blob string) (*blob.GetPropertiesResponse, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (s *service) putBlob(blob string, reader io.ReadSeeker) error {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	_, err := blobClient.Upload(context.Background(), streaming.NopCloser(reader), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *service) getBlob(blob string) (io.ReadCloser, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return response.Body, nil
}

func (s *service) deleteBlobs(blob string) error {
	blobs, err := s.listBlobs(blob, "")
	if err != nil {
		return errors.Wrapf(err, "failed to list blobs with prefix %v before removing them", blob)
	}

	var deletionFailures []string
	for _, blob := range *blobs {
		blobClient := s.ContainerClient.NewBlockBlobClient(blob)
		_, err = blobClient.Delete(context.Background(), nil)
		if err != nil {
			log.WithError(err).Errorf("Failed to delete blob object: %v", blob)
			deletionFailures = append(deletionFailures, blob)
		}
	}

	if len(deletionFailures) > 0 {
		return fmt.Errorf("failed to delete blobs %v", deletionFailures)
	}

	return nil
}
