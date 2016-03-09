package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"

	"willmadison.com/image/metadata"
	"willmadison.com/image/transfer"
)

var logger *log.Logger

func init() {
	logger = log.New()
	logger.Out = os.Stdout
}

func main() {
	var (
		maxRetrievers = flag.Int("maxRetrievers", 20, "the maximum number of concurrent image retrievers executing at once.")
		maxImages     = flag.Int("maxImages", 1000, "the maximum number of images to attempt to retrieve at once.")

		connectionString        = flag.String("dbConnectionString", "server=localhost;port=1433;user id=placeholder;password=placeholder", "the fully qualified database connection string to connect to the source database of information for our image retrieval.")
		databaseServer          = flag.String("dbServer", "localhost", "the database server to connect to as the source of information for our image retrieval.")
		databasePort            = flag.Int("dbPort", 1433, "the port on which the database is running which should be used as the source of information for our image retrieval.")
		databaseFailoverPartner = flag.String("dbFailoverPartner", "", "the database server to utilize as a failover partner to connect to as the source of information for our image retrieval.")
		databaseUserName        = flag.String("dbUserName", "placeholder", "the database username to utilize when connecting to the source of information for our image retrieval.")
		databasePassword        = flag.String("dbPassword", "placeholder", "the database password to utilize when connecting to the source of information for our image retrieval.")

		pollFrequency = flag.Int("pollFrequency", 45, "the frequency in minutes with which we should poll looking for images to retrieve.")
		s3BucketName  = flag.String("s3BucketName", "fabricdev-image-processing", "The S3 bucket which we should upload images to be processed by the auto image processing application.")
	)

	flag.Parse()

	configuration := metadata.ServerConfiguration{
		ServerName:       *databaseServer,
		Port:             *databasePort,
		FailoverPartner:  *databaseFailoverPartner,
		ConnectionString: *connectionString,
	}

	credentials := metadata.Credentials{
		UserName: *databaseUserName,
		Password: *databasePassword,
	}

	retriever := metadata.NewRetriever(configuration, credentials)
	ticker := time.NewTicker(time.Duration(*pollFrequency) * time.Minute)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	pipeline := make(chan metadata.ImageMetadata, *maxImages)

	publisher := transfer.NewSQSPublisher()
	fetcher := transfer.NewSQSFetcher(*maxImages, *maxRetrievers)
	destination := transfer.NewS3Destination(*s3BucketName)
	uploader := transfer.NewUploader()

	go publishMetadata(publisher, pipeline, done)

	go uploadImages(fetcher, uploader, destination, done)

	for {
		select {
		case <-quit:
			logger.Println("Remote image fetcher exiting...")
			retriever.Close()
			ticker.Stop()
			close(pipeline)
			close(done)
		case <-ticker.C:
			retrieveRemoteImages(retriever, *maxImages, pipeline)
		}
	}
}

func publishMetadata(p transfer.Publisher, pipeline <-chan metadata.ImageMetadata, done <-chan struct{}) {
	for {
		select {
		case <-done:
			logger.Println("Publisher exiting...")
			return
		default:
			imagesPublished := 0

			for imageMetadata := range pipeline {
				err := p.Publish(imageMetadata)

				if err != nil {
					logger.Error("Encountered an error attempting to publish remote image metadata: ", err)

					logger.Info("Sleeping for 2 minutes...")
					time.Sleep(2 * time.Minute)
				} else {
					imagesPublished += 1
				}
			}

		}
	}
}

func uploadImages(f transfer.Fetcher, u transfer.Uploader, d transfer.Destination, done <-chan struct{}) {
	for {
		select {
		case <-done:
			logger.Println("Uploader exiting...")
			return
		default:
			images, err := f.Fetch()

			numImages := len(images)

			if err != nil {
				logger.Error("Encountered an error fetching remote images! ", err)

				logger.Info("Sleeping for 2 minutes...")
				time.Sleep(2 * time.Minute)
			} else if numImages > 0 {
				start := time.Now()

				numConfirmations := 0

				confirmations := make(chan struct{})

				go func() {
					for _ = range confirmations {
						numConfirmations++
					}
				}()

				var wg sync.WaitGroup

				for _, image := range images {
					wg.Add(1)

					go func(i transfer.RemoteImage) {
						defer wg.Done()
						err = u.Upload(i, d)

						if err == nil {
							f.Confirm(i)
							confirmations <- struct{}{}
						} else {
							logger.Error("Encountered an error during image upload: ", err)
						}
					}(image)
				}

				wg.Wait()

				close(confirmations)

				uploadDuration := time.Since(start)

				logger.WithFields(log.Fields{
					"remoteImage.SuccessfulUploads": numConfirmations,
					"remoteImage.UploadDuration":    uploadDuration.String(),
					"remoteImage.UploadRate":        float64(numImages) / uploadDuration.Seconds(),
				}).Printf("Completely fetched & uploaded %d remote product images!", numConfirmations)
			} else {
				logger.Info("No images to upload at the moment, sleeping for 1 minute...")
				time.Sleep(1 * time.Minute)
			}
		}
	}
}

func retrieveRemoteImages(r metadata.Retriever, maxImages int, pipeline chan<- metadata.ImageMetadata) {
	metadata, err := r.Retrieve(maxImages)

	if err != nil {
		logger.Error("Encountered an error retrieving image metadata: ", err)

		logger.Info("Sleeping for 2 minutes...")
		time.Sleep(2 * time.Minute)
	} else {
		for _, imageMetadata := range metadata {
			pipeline <- imageMetadata
		}
	}
}
