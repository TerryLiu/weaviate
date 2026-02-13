package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func TestSemanticSearchUnit(t *testing.T) {
	// Step 1.1: Connect to your local Weaviate instance
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Failed to create Weaviate client")

	// Setup: Create Movie class
	className := "Movie"
	classObj := &models.Class{
		Class:      className,
		Vectorizer: "none",
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

	// Clean up if exists
	client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
	
	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "Failed to create Movie class")

	// Insert test data
	testMovies := []map[string]interface{}{
		{
			"title":       "The Matrix",
			"description": "A computer hacker learns about the true nature of reality and his role in the war against its controllers.",
			"genre":       "Science Fiction",
		},
		{
			"title":       "Star Wars",
			"description": "Luke Skywalker joins forces with a Jedi Knight to save the galaxy.",
			"genre":       "Science Fiction",
		},
		{
			"title":       "The Godfather",
			"description": "The aging patriarch of an organized crime dynasty transfers control.",
			"genre":       "Crime",
		},
	}

	objects := make([]*models.Object, len(testMovies))
	for i, movie := range testMovies {
		objects[i] = &models.Object{
			Class:      className,
			Properties: movie,
		}
	}

	_, err = client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	require.NoError(t, err, "Failed to insert test data")

	// Perform query with proper field specification (maintaining original logic structure)
	title := graphql.Field{Name: "title"}
	description := graphql.Field{Name: "description"}
	genre := graphql.Field{Name: "genre"}

	result, err := client.GraphQL().Get().
		WithClassName("Movie").
		WithFields(title, description, genre).
		WithLimit(2).
		Do(context.Background())

	require.NoError(t, err, "Query failed")

	// Inspect the results - preserving original inspection logic
	if result.Errors != nil {
		// Print detailed error information
		for _, graphqlErr := range result.Errors {
			fmt.Printf("GraphQL Error: %+v\n", graphqlErr)
		}
		t.Errorf("Query returned errors")
		return
	}

	// This maintains the same data access pattern as the original
	data := result.Data["Get"].(map[string]interface{})
	movies := data["Movie"].([]interface{})

	fmt.Printf("Found %d movies:\n", len(movies))
	for _, movie := range movies {
		jsonData, err := json.MarshalIndent(movie, "", "  ")
		require.NoError(t, err, "Failed to marshal movie data")
		fmt.Println(string(jsonData))
	}

	// Cleanup
	err = client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
	require.NoError(t, err, "Failed to cleanup")
}