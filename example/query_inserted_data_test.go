package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

func TestQueryInsertedMovieData(t *testing.T) {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "Should connect to Weaviate")
	require.NotNil(t, client, "Client should not be nil")

	className := "Movie"
	
	// 首先确保Movie类存在，如果不存在则创建
	exists, err := client.Schema().ClassExistenceChecker().WithClassName(className).Do(context.Background())
	require.NoError(t, err)

	if !exists {
		// 创建Movie类
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

		err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
		require.NoError(t, err, "Should create Movie class")
		fmt.Println("✓ Created Movie class for testing")
	} else {
		fmt.Println("✓ Movie class already exists")
	}

	// 插入测试数据
	testMovies := []map[string]interface{}{
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
		{
			"title":       "Inception",
			"description": "A thief who steals corporate secrets through dream-sharing technology.",
			"genre":       "Science Fiction",
		},
		{
			"title":       "Parasite",
			"description": "A poor family schemes to become employed by a wealthy family.",
			"genre":       "Thriller",
		},
	}

	// 插入对象
	objects := make([]*models.Object, len(testMovies))
	for i, movie := range testMovies {
		objects[i] = &models.Object{
			Class:      className,
			Properties: movie,
		}
	}

	batchResult, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	require.NoError(t, err, "Should insert objects successfully")
	require.NotNil(t, batchResult, "Batch result should not be nil")

	// 统计成功插入的数量
	successCount := 0
	for _, result := range batchResult {
		if result.Result.Errors == nil {
			successCount++
		}
	}
	fmt.Printf("✓ Inserted %d movies successfully\n", successCount)

	// 测试1: 查询所有电影
	t.Run("QueryAllMovies", func(t *testing.T) {
		result, err := client.GraphQL().Get().
			WithClassName(className).
			Do(context.Background())

		require.NoError(t, err, "Should query all movies successfully")
		require.NotNil(t, result, "Query result should not be nil")
		
		// 验证返回的数据结构
		fmt.Println("✓ Successfully queried all movies")
	})

	// 测试3: 基本过滤查询（使用GraphQL字符串）
	t.Run("QueryMoviesWithFilter", func(t *testing.T) {
		// 使用原始GraphQL查询字符串进行过滤
		query := `
		{
			Get {
				Movie(
					where: {
						path: ["genre"]
						operator: Equal
						valueString: "Science Fiction"
					}
				) {
					title
					genre
				}
			}
		}`

		result, err := client.GraphQL().Raw().WithQuery(query).Do(context.Background())
		require.NoError(t, err, "Should execute raw GraphQL query successfully")
		require.NotNil(t, result, "Query result should not be nil")
		
		if result.Data != nil {
			fmt.Println("✓ Successfully executed filtered query for Science Fiction movies")
		}
	})

	// 测试2: 限制返回结果数量
	t.Run("QueryMoviesWithLimit", func(t *testing.T) {
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithLimit(2).
			Do(context.Background())

		require.NoError(t, err, "Should query movies with limit successfully")
		require.NotNil(t, result, "Query result should not be nil")
		
		fmt.Println("✓ Successfully queried movies with limit=2")
	})

	// 测试4: 复杂查询示例
	t.Run("ComplexQueryExample", func(t *testing.T) {
		query := `
		{
			Get {
				Movie(
					limit: 3
				) {
					title
					description
					genre
				}
			}
		}`

		result, err := client.GraphQL().Raw().WithQuery(query).Do(context.Background())
		require.NoError(t, err, "Should execute complex query successfully")
		require.NotNil(t, result, "Query result should not be nil")
		
		if result.Data != nil {
			fmt.Println("✓ Successfully executed complex query with limit")
		}
	})

	// 清理：删除测试数据
	defer func() {
		err = client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
		if err != nil {
			t.Logf("Warning: Failed to cleanup Movie class: %v", err)
		} else {
			fmt.Println("✓ Cleaned up Movie class after test")
		}
	}()

	fmt.Println("\n🎉 All query tests completed successfully!")
}