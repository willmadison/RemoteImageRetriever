package metadata

import (
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
)

const Unlimited = 0

type ImageMetadata struct {
	SKU      string `json:"sku"`
	ImageURL string `json:"imageUrl"`
}

type ServerConfiguration struct {
	ServerName       string
	Port             int
	FailoverPartner  string
	ConnectionString string
}

type Credentials struct {
	UserName, Password string
}

type Retriever interface {
	Retrieve(maxResults int) ([]ImageMetadata, error)
	Close() error
}

type databaseRetriever struct {
}

var logger *log.Logger

func init() {
	logger = log.New()
	logger.Out = os.Stdout
}

func (d *databaseRetriever) Retrieve(maxResults int) ([]ImageMetadata, error) {
	start := time.Now()
	metadata, err := d.retrieveImageMetadata(maxResults)

	time.Sleep(2 * time.Second)

	numRecordsRetrieved := len(metadata)
	retrievalDuration := time.Since(start)

	logger.WithFields(log.Fields{
		"remoteImage.DatabaseRecordsRetrieved":  numRecordsRetrieved,
		"remoteImage.DatabaseRetrievalDuration": retrievalDuration.String(),
		"remoteImage.DatabaseRetrievalRate":     float64(numRecordsRetrieved) / retrievalDuration.Seconds(),
	}).Printf("Completely retrieved %d pieces image metadata from the data store...", numRecordsRetrieved)

	return metadata, err
}

func (d *databaseRetriever) retrieveImageMetadata(maxResults int) ([]ImageMetadata, error) {
	return []ImageMetadata{}, nil
}

func (d *databaseRetriever) Close() error {
	return nil
}

func NewRetriever(configuration ServerConfiguration, credentials Credentials) Retriever {
	return &databaseRetriever{}
}
