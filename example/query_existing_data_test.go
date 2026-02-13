package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

func TestQueryExistingMovieData(t *testing.T) {
	// Connect to Weaviate instance
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Should connect to Weaviate successfully")
	require.NotNil(t, client, "Client should not be nil")

	// Verify Movie class exists (assumes it was created by other tests)
	className := "Movie"
	exists, err := client.Schema().ClassExistenceChecker().WithClassName(className).Do(context.Background())
	require.NoError(t, err, "Should check class existence without error")
	require.True(t, exists, "Movie class should exist (created by other tests)")

	// Test 1: Query all existing movies
	t.Run("QueryAllMovies", func(t *testing.T) {
		title := graphql.Field{Name: "title"}
		description := graphql.Field{Name: "description"}
		genre := graphql.Field{Name: "genre"}

		result, err := client.GraphQL().Get().
			WithClassName("Movie").
			WithFields(title, description, genre).
			Do(context.Background())

		require.NoError(t, err, "Should query all movies successfully")
		require.NotNil(t, result, "Query result should not be nil")

		// Validate results structure
		if result.Errors != nil {
			t.Errorf("Query returned errors: %v", result.Errors)
			return
		}

		data := result.Data["Get"].(map[string]interface{})
		movies := data["Movie"].([]interface{})

		assert.Greater(t, len(movies), 0, "Should find existing movies")
		fmt.Printf("✓ Found %d existing movies\n", len(movies))

		// Display sample results
		for i, movie := range movies {
			if i >= 3 { // Limit display to first 3 results
				break
			}
			jsonData, err := json.MarshalIndent(movie, "", "  ")
			require.NoError(t, err, "Should marshal movie data")
			fmt.Printf("Movie %d:\n%s\n", i+1, string(jsonData))
		}
	})

	// Test 2: Query with limit
	t.Run("QueryMoviesWithLimit", func(t *testing.T) {
		result, err := client.GraphQL().Get().
			WithClassName("Movie").
			WithLimit(2).
			Do(context.Background())

		require.NoError(t, err, "Should query with limit successfully")
		require.NotNil(t, result, "Query result should not be nil")

		if result.Data != nil {
			fmt.Println("✓ Successfully queried movies with limit=2")
		}
	})

	// Test 3: Count existing records
	t.Run("CountExistingMovies", func(t *testing.T) {
		// Use raw GraphQL for aggregate query
		query := `
		{
			Aggregate {
				Movie {
					meta {
						count
					}
				}
			}
		}`

		result, err := client.GraphQL().Raw().WithQuery(query).Do(context.Background())
		require.NoError(t, err, "Should count movies successfully")
		require.NotNil(t, result, "Aggregate result should not be nil")

		if result.Data != nil {
			fmt.Println("✓ Successfully counted existing movies")
		}
	})

	// Test 4: Query specific fields only
	t.Run("QuerySpecificFields", func(t *testing.T) {
		title := graphql.Field{Name: "title"}
		
		result, err := client.GraphQL().Get().
			WithClassName("Movie").
			WithFields(title).
			WithLimit(1).
			Do(context.Background())

		require.NoError(t, err, "Should query specific fields successfully")
		require.NotNil(t, result, "Query result should not be nil")

		if result.Data != nil {
			fmt.Println("✓ Successfully queried specific fields only")
		}
	})

	fmt.Println("\n🎉 All query tests on existing data completed successfully!")
}