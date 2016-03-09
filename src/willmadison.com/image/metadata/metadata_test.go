package metadata

import (
	"os"
	"testing"
)

func TestRetrieve(t *testing.T) {
	config := ServerConfiguration{
		ConnectionString: os.Getenv("RETRIEVER_CONN_STRING"),
	}

	if config.ConnectionString != "" {
		creds := Credentials{}

		retriever := NewRetriever(config, creds)
		defer retriever.Close()

		maxResults := 10

		metadata, err := retriever.Retrieve(maxResults)

		if err != nil {
			t.Fatal(err)
		}

		if len(metadata) > maxResults {
			t.Fatal("Too many results found! Expected", maxResults)
		}
	}
}
