package main

import (
	"testing"
	"time"

	"willmadison.com/image/metadata"
	"willmadison.com/image/transfer"
)

type testRetriever struct {
	closed bool
}

func (t *testRetriever) Retrieve(maxResults int) ([]metadata.ImageMetadata, error) {
	return []metadata.ImageMetadata{metadata.ImageMetadata{SKU: "GopherSku", ImageURL: "https://golang.org/doc/gopher/gopherbw.png"}}, nil
}

func (t *testRetriever) Close() error {
	t.closed = true
	return nil
}

type testFetcher struct {
	fetchFrom     <-chan metadata.ImageMetadata
	fetchCalled   bool
	confirmCalled bool
}

func (t *testFetcher) Fetch() ([]transfer.RemoteImage, error) {
	t.fetchCalled = true

	metadata := <-t.fetchFrom

	logger.Printf("Fetched: %+v", metadata)

	remoteImages := []transfer.RemoteImage{}

	remoteImages = append(remoteImages, transfer.RemoteImage{Contents: []byte{}, FileName: metadata.SKU + ".png"})

	return remoteImages, nil
}

func (t *testFetcher) Confirm(r transfer.RemoteImage) error {
	t.confirmCalled = true
	return nil
}

type testPublisher struct {
	toBePublished chan<- metadata.ImageMetadata
	publishCalled bool
}

func (t *testPublisher) Publish(m metadata.ImageMetadata) error {
	t.publishCalled = true
	logger.Printf("Publishing: %+v", m)

	t.toBePublished <- m
	return nil
}

type testDestination struct {
	receiveCalled bool
}

func (t *testDestination) Receive(r transfer.RemoteImage) error {
	t.receiveCalled = true
	logger.Printf("Receiving: %+v", r)
	return nil
}

type testUploader struct {
	uploadCalled bool
}

func (t *testUploader) Upload(r transfer.RemoteImage, d transfer.Destination) error {
	t.uploadCalled = true
	logger.Printf("Uploading: %+v", r)
	return d.Receive(r)
}

func TestRetrieveRemoteImages(t *testing.T) {
	publishing := make(chan metadata.ImageMetadata)

	retriever := &testRetriever{}
	publisher := &testPublisher{toBePublished: publishing}
	fetcher := &testFetcher{fetchFrom: publishing}
	destination := &testDestination{}
	uploader := &testUploader{}

	done := make(chan struct{})
	pipeline := make(chan metadata.ImageMetadata, 1)

	go publishMetadata(publisher, pipeline, done)

	go uploadImages(fetcher, uploader, destination, done)

	retrieveRemoteImages(retriever, 1, pipeline)

	logger.Info("Sleeping for 1 Seconds to allow for the orchestration to play it's way through...")
	time.Sleep(1 * time.Second)

	retriever.Close()
	close(pipeline)
	close(done)

	if !retriever.closed {
		t.Fatal("Retriever was not properly closed!")
	}

	if !publisher.publishCalled {
		t.Fatal("Publish was not properly invoked!")
	}

	if !fetcher.fetchCalled {
		t.Fatal("Fetch was not properly invoked!")
	}

	if !fetcher.confirmCalled {
		t.Fatal("Confirm was not properly invoked!")
	}

	if !destination.receiveCalled {
		t.Fatal("Receive was not properly invoked!")
	}

	if !uploader.uploadCalled {
		t.Fatal("Upload was not properly invoked!")
	}
}
