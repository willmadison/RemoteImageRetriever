package transfer

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"willmadison.com/image/metadata"
)

var logger *log.Logger

func init() {
	logger = log.New()
	logger.Out = os.Stdout
}

type Publisher interface {
	Publish(m metadata.ImageMetadata) error
}

type sqsPublisher struct {
}

func NewSQSPublisher() Publisher {
	return sqsPublisher{}
}

func (s sqsPublisher) Publish(m metadata.ImageMetadata) error {
	return nil
}
