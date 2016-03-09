package transfer

type Destination interface {
	Receive(r RemoteImage) error
}

type Uploader interface {
	Upload(r RemoteImage, d Destination) error
}

type basicUploader struct{}

type s3Destination struct {
	bucketName string
}

var defaultUploader basicUploader

func (s s3Destination) Receive(r RemoteImage) error {
	return nil
}

func NewS3Destination(bucketName string) Destination {
	return s3Destination{
		bucketName: bucketName,
	}
}

func NewUploader() Uploader {
	return defaultUploader
}

func (b basicUploader) Upload(r RemoteImage, d Destination) error {
	return d.Receive(r)
}
