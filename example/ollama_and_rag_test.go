package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

func TestOllamaAndRAGIntegration(t *testing.T) {
	// Step 1.1: Connect to your local Weaviate instance
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Failed to create Weaviate client")
	require.NotNil(t, client, "Client should not be nil")

	// Check if Movie class already exists and clean it up if needed
	exists, err := client.Schema().ClassExistenceChecker().WithClassName("Movie").Do(context.Background())
	require.NoError(t, err, "Failed to check class existence")

	if exists {
		// Clean up existing class
		err = client.Schema().ClassDeleter().WithClassName("Movie").Do(context.Background())
		require.NoError(t, err, "Failed to delete existing Movie class")
		fmt.Println("Cleaned up existing Movie collection")
	}

	// Step 1.2: Create a collection
	classObj := &models.Class{
		Class:      "Movie",
		Vectorizer: "text2vec-ollama",
		ModuleConfig: map[string]interface{}{
			"text2vec-ollama": map[string]interface{}{ // Configure the Ollama embedding integration
				"apiEndpoint": "http://ollama:11434", // If using Docker you might need: http://host.docker.internal:11434
				"model":       "dengcao/bge-large-zh-v1.5", // The model to use
			},
			"generative-ollama": map[string]interface{}{ // Configure the Ollama generative integration
				"apiEndpoint": "http://ollama:11434", // If using Docker you might need: http://host.docker.internal:11434
				"model":       "llama3.2", // The model to use
			},
		},
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "Failed to create Movie class")
	fmt.Println("Created Movie collection")

	// Verify the class was created successfully
	exists, err = client.Schema().ClassExistenceChecker().WithClassName("Movie").Do(context.Background())
	require.NoError(t, err, "Failed to verify class creation")
	assert.True(t, exists, "Movie class should exist after creation")

	// Step 1.3: Import three objects
	dataObjects := []map[string]interface{}{
		{
			"title":       "The Matrix",
			"description": "A computer hacker learns about the true nature of reality and his role in the war against its controllers.",
			"genre":       "Science Fiction",
		},
		{
			"title":       "Spirited Away",
			"description": "A young girl becomes trapped in a mysterious world of spirits and must find a way to save her parents and return home.",
			"genre":       "Animation",
		},
		{
			"title":       "The Lord of the Rings: The Fellowship of the Ring",
			"description": "A meek Hobbit and his companions set out on a perilous journey to destroy a powerful ring and save Middle-earth.",
			"genre":       "Fantasy",
		},
	}

	// Insert objects
	objects := make([]*models.Object, len(dataObjects))
	for i, obj := range dataObjects {
		objects[i] = &models.Object{
			Class:      "Movie",
			Properties: obj,
		}
	}

	batchResult, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	require.NoError(t, err, "Failed to batch insert objects")
	require.NotNil(t, batchResult, "Batch result should not be nil")
	
	successCount := 0
	for _, result := range batchResult {
		if result.Result.Errors == nil {
			successCount++
		}
	}
	
	assert.Equal(t, len(dataObjects), successCount, "All objects should be inserted successfully")
	fmt.Printf("Imported & vectorized %d objects into the Movie collection\n", successCount)


	// Cleanup: Delete the class after test
	defer func() {
		err = client.Schema().ClassDeleter().WithClassName("Movie").Do(context.Background())
		if err != nil {
			t.Logf("Warning: Failed to cleanup Movie class: %v", err)
		} else {
			fmt.Println("Cleaned up Movie collection after test")
		}
	}()
}