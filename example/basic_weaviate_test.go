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

func TestWeaviateBasicOperations(t *testing.T) {
	// Test 1: Connection to Weaviate
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Should connect to Weaviate")
	require.NotNil(t, client, "Client should not be nil")
	
	// Verify connection by getting schema
	schema, err := client.Schema().Getter().Do(context.Background())
	require.NoError(t, err, "Should get schema successfully")
	require.NotNil(t, schema, "Schema should not be nil")
	
	fmt.Printf("✓ Connected to Weaviate. Found %d existing classes\n", len(schema.Classes))

	// Test 2: Class management
	className := "TestMovie"
	
	// Clean up if class exists
	exists, err := client.Schema().ClassExistenceChecker().WithClassName(className).Do(context.Background())
	require.NoError(t, err)
	
	if exists {
		err = client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
		require.NoError(t, err)
		fmt.Println("✓ Cleaned up existing test class")
	}

	// Create test class
	classObj := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "genre",
				DataType: []string{"text"},
			},
		},
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "Should create class successfully")
	fmt.Println("✓ Created test class")

	// Verify class creation
	exists, err = client.Schema().ClassExistenceChecker().WithClassName(className).Do(context.Background())
	require.NoError(t, err)
	assert.True(t, exists, "Class should exist after creation")
	fmt.Println("✓ Verified class creation")

	// Test 3: Data insertion
	testData := []map[string]interface{}{
		{
			"title": "Test Movie 1",
			"genre": "Action",
		},
		{
			"title": "Test Movie 2", 
			"genre": "Comedy",
		},
	}

	objects := make([]*models.Object, len(testData))
	for i, data := range testData {
		objects[i] = &models.Object{
			Class:      className,
			Properties: data,
		}
	}

	batchResult, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	require.NoError(t, err, "Should insert objects successfully")
	require.NotNil(t, batchResult, "Batch result should not be nil")

	successCount := 0
	for _, result := range batchResult {
		if result.Result.Errors == nil {
			successCount++
		}
	}
	assert.Equal(t, len(testData), successCount, "All objects should be inserted successfully")
	fmt.Printf("✓ Inserted %d objects successfully\n", successCount)

	// Test 4: Error handling
	// Test invalid class creation
	invalidClass := &models.Class{
		Class: "", // Empty class name should cause error
	}
	err = client.Schema().ClassCreator().WithClass(invalidClass).Do(context.Background())
	assert.Error(t, err, "Should fail with invalid class name")
	fmt.Println("✓ Error handling works correctly")

	// Cleanup
	err = client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
	require.NoError(t, err, "Should cleanup test class")
	fmt.Println("✓ Cleaned up test class")

	fmt.Println("\n🎉 All tests passed successfully!")
}