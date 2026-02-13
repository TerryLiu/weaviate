package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
)

func TestRAG(t *testing.T) {
    // Step 2.1: Connect to your local Weaviate instance
    cfg := weaviate.Config{
        Host:   "localhost:8080",
        Scheme: "http",
    }
    client, err := weaviate.NewClient(cfg)
    if err != nil {
        t.Fatalf("Failed to create client: %v", err)
    }

    // Step 2.2: Perform basic search for movies
    // Using raw GraphQL query with where filter (without generative search)
    query := `
    {
        Get {
            Movie(
                where: {
                    path: ["genre"]
                    operator: Equal
                    valueString: "Science Fiction"
                }
                limit: 1
            ) {
                title
                description
                genre
            }
        }
    }`

    result, err := client.GraphQL().Raw().WithQuery(query).Do(context.Background())

    if err != nil {
        t.Fatalf("GraphQL query failed: %v", err)
    }

    // Inspect the results
    if result.Errors != nil {
        t.Errorf("GraphQL result contains errors: %v", result.Errors)
        // Print detailed error information
        for i, err := range result.Errors {
            t.Logf("Error %d: %v", i, err)
        }
        return
    }

    // Process and display results
    if result.Data != nil {
        jsonData, err := json.MarshalIndent(result.Data, "", "  ")
        if err == nil {
            fmt.Printf("✅ Query successful! Found movies:\n%s\n", string(jsonData))
        }
    } else {
        fmt.Println("❌ No data returned from query")
    }

}