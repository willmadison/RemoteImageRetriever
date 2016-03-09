package transfer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"willmadison.com/image/metadata"
)

type RemoteImage struct {
	Contents   []byte
	FileName   string
	Identifier string
}

type Fetcher interface {
	Fetch() ([]RemoteImage, error)
	Confirm(m RemoteImage) error
}

type sqsFetcher struct {
	maxImages, maxFetchers int
}

func NewSQSFetcher(maxImages, maxFetchers int) Fetcher {
	return sqsFetcher{
		maxImages:   maxImages,
		maxFetchers: maxFetchers,
	}
}

func (s sqsFetcher) Fetch() ([]RemoteImage, error) {
	remoteImages := []RemoteImage{}

	start := time.Now()

	messages, err := s.retrievePhotoRetrievalMessages(s.maxImages)

	if err != nil {
		return remoteImages, err
	}

	messageBatches := batchPhotoRetrievalMessages(s.maxFetchers, messages)

	var wg sync.WaitGroup

	for _, batch := range messageBatches {
		wg.Add(1)

		go func(messageBatch []string) {
			defer wg.Done()

			for _, message := range messageBatch {
				remoteImage, err := retrieveImage(message)

				if err == nil {
					remoteImages = append(remoteImages, remoteImage)
				} else {
					logger.Error("Encountered an error retrieving a remote image: ", err)
				}
			}
		}(batch)
	}

	wg.Wait()

	fetchDuration := time.Since(start)

	numImagesFetched := len(remoteImages)

	logger.WithFields(log.Fields{
		"remoteImage.NumImagesFetched": numImagesFetched,
		"remoteImage.FetchDuration":    fetchDuration.String(),
		"remoteImage.FetchRate":        float64(numImagesFetched) / fetchDuration.Seconds(),
	}).Println("Completely fetched images from remote location.")

	return remoteImages, nil
}

func (s sqsFetcher) Confirm(image RemoteImage) error {
	logger.Println("Confirmed...")
	return nil
}

func (s sqsFetcher) retrievePhotoRetrievalMessages(maxImages int) ([]string, error) {
	start := time.Now()

	messages := []string{}

	logger.Println("Attempting to retrieve at most", maxImages, "images....")

	time.Sleep(2 * time.Second)

	retrievalDuration := time.Since(start)
	numMessagesRetrieved := len(messages)

	logger.WithFields(log.Fields{
		"remoteImage.SqsMessagesRetrieved":  numMessagesRetrieved,
		"remoteImage.SqsRetrievalDuration":  retrievalDuration.String(),
		"remoteImage.SqsImageRetrievalRate": float64(numMessagesRetrieved) / retrievalDuration.Seconds(),
	}).Println("Completely retrieved image metadata.")

	return messages, nil
}

func batchPhotoRetrievalMessages(maxFetchers int, messages []string) [][]string {
	messageBatches := [][]string{}

	numMessages := len(messages)

	batchSize := numMessages / maxFetchers

	if batchSize > 1 {
		from := 0
		to := from + batchSize

		for from < batchSize {
			if to > numMessages {
				to = numMessages
			}

			messageBatches = append(messageBatches, messages[from:to])

			from += batchSize
			to += batchSize
		}
	} else {
		messageBatches = append(messageBatches, messages)
	}

	return messageBatches
}

func retrieveImage(message string) (RemoteImage, error) {
	remoteImage := RemoteImage{}

	imageMetadata, err := convertToImageMetadata(message)

	if err != nil {
		return remoteImage, err
	}

	extension, err := deriveExtension(imageMetadata.ImageURL)

	if err != nil {
		return remoteImage, err
	}

	remoteImage.FileName = fmt.Sprintf("%s.%s", strings.ToUpper(imageMetadata.SKU), extension)
	remoteImage.Identifier = "DummyIdentifier"
	remoteImage.Contents, err = fetchImageContents(imageMetadata.ImageURL)

	if err != nil {
		return remoteImage, err
	}

	return remoteImage, nil
}

func deriveExtension(imageURL string) (string, error) {
	url, err := url.Parse(imageURL)

	if err != nil {
		return "", err
	}

	urlParts := strings.Split(url.Path, ".")

	extension := urlParts[len(urlParts)-1]

	return extension, nil
}

func fetchImageContents(imageURL string) ([]byte, error) {
	response, err := http.Get(imageURL)

	if err != nil {
		return []byte{}, err
	}

	defer response.Body.Close()

	return ioutil.ReadAll(response.Body)
}

func convertToImageMetadata(message string) (metadata.ImageMetadata, error) {
	imageMetadata := metadata.ImageMetadata{}
	return imageMetadata, nil
}
