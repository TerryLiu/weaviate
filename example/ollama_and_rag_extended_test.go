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

func TestWeaviateConnection(t *testing.T) {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Should connect to Weaviate")
	require.NotNil(t, client, "Client should not be nil")
	
	// Test basic connection by getting schema info
	schema, err := client.Schema().Getter().Do(context.Background())
	require.NoError(t, err, "Should get schema")
	require.NotNil(t, schema, "Schema should not be nil")
	
	fmt.Printf("Connected to Weaviate successfully. Schema has %d classes\n", len(schema.Classes))
}

func TestMovieCollectionOperations(t *testing.T) {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err)

	// Setup: Clean up any existing Movie class
	exists, err := client.Schema().ClassExistenceChecker().WithClassName("Movie").Do(context.Background())
	require.NoError(t, err)
	
	if exists {
		err = client.Schema().ClassDeleter().WithClassName("Movie").Do(context.Background())
		require.NoError(t, err)
		fmt.Println("Cleaned up existing Movie class")
	}

	// Test 1: Create Movie class
	classObj := &models.Class{
		Class:      "Movie",
		Vectorizer: "none", // Using "none" for simpler testing without external dependencies
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "description", 
				DataType: []string{"text"},
			},
			{
				Name:     "genre",
				DataType: []string{"text"},
			},
		},
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "Should create Movie class")
	fmt.Println("Created Movie collection")

	// Verify class creation
	exists, err = client.Schema().ClassExistenceChecker().WithClassName("Movie").Do(context.Background())
	require.NoError(t, err)
	assert.True(t, exists, "Movie class should exist")

	// Test 2: Insert movie objects
	movies := []map[string]interface{}{
		{
			"title":       "The Matrix",
			"description": "A computer hacker learns about the true nature of reality",
			"genre":       "Science Fiction",
		},
		{
			"title":       "Inception", 
			"description": "A thief who steals corporate secrets through dream-sharing technology",
			"genre":       "Science Fiction",
		},
		{
			"title":       "The Godfather",
			"description": "The aging patriarch of an organized crime dynasty transfers control",
			"genre":       "Crime",
		},
	}

	objects := make([]*models.Object, len(movies))
	for i, movie := range movies {
		objects[i] = &models.Object{
			Class:      "Movie",
			Properties: movie,
		}
	}

	batchResult, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batchResult)

	// Count successful insertions
	successCount := 0
	for _, result := range batchResult {
		if result.Result.Errors == nil {
			successCount++
		}
	}
	assert.Equal(t, len(movies), successCount, "All movies should be inserted")
	fmt.Printf("Successfully inserted %d movies\n", successCount)

	// Test 3: Verify data can be retrieved (basic existence check)
	getResult, err := client.GraphQL().Get().
		WithClassName("Movie").
		Do(context.Background())

	require.NoError(t, err, "Basic query should succeed")
	require.NotNil(t, getResult, "Query result should not be nil")
	
	fmt.Println("Basic data retrieval test passed")

	// Cleanup
	err = client.Schema().ClassDeleter().WithClassName("Movie").Do(context.Background())
	require.NoError(t, err, "Should cleanup Movie class")
	fmt.Println("Cleaned up Movie collection")
}

func TestErrorHandling(t *testing.T) {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err)

	// Test querying non-existent class
	_, err = client.Schema().ClassExistenceChecker().WithClassName("NonExistentClass").Do(context.Background())
	require.NoError(t, err, "Should handle non-existent class gracefully")

	// Test creating class with invalid configuration
	invalidClass := &models.Class{
		Class: "",
	}
	err = client.Schema().ClassCreator().WithClass(invalidClass).Do(context.Background())
	assert.Error(t, err, "Should fail with invalid class name")
}