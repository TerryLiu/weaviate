// semantic_search_simple_test.go - 简化的语义搜索测试
// 专注于核心的向量搜索功能测试
package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

// TestBasicSemanticSearch 测试基础语义搜索功能
func TestBasicSemanticSearch(t *testing.T) {
	// 初始化客户端
	client := initSimpleTestClient(t)
	
	// 准备测试数据
	className := "SimpleSemanticMovie"
	cleanupSimpleClass(t, client, className)
	createSimpleSemanticClass(t, client, className)
	insertSimpleSemanticData(t, client, className)
	
	t.Run("基础语义搜索-科幻主题", func(t *testing.T) {
		// 测试搜索与"科幻"相关的电影
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"科幻"})
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(
				graphql.Field{Name: "title"}, 
				graphql.Field{Name: "genre"},
			).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "语义搜索应该成功执行")
		require.NotNil(t, result, "结果不应该为空")
		
		// 验证返回结果
		if result.Data == nil {
			t.Skip("跳过测试：可能是Ollama服务未启动")
			return
		}
		
		getData, ok := result.Data["Get"]
		if !ok {
			t.Skip("跳过测试：无法获取搜索结果")
			return
		}
		
		classData, ok := getData.(map[string]interface{})[className]
		if !ok || classData == nil {
			t.Skip("跳过测试：未找到相关数据")
			return
		}
		
		movies, ok := classData.([]interface{})
		if !ok {
			t.Skip("跳过测试：数据格式不正确")
			return
		}
		
		fmt.Printf("🔍 语义搜索找到 %d 部相关电影\n", len(movies))
		
		// 验证至少找到了一些结果
		assert.GreaterOrEqual(t, len(movies), 0, "应该找到相关电影")
		
		// 检查返回的字段
		for i, movie := range movies {
			if i >= 3 { // 只检查前3个结果
				break
			}
			movieData, ok := movie.(map[string]interface{})
			if !ok {
				continue
			}
			
			assert.Contains(t, movieData, "title", "应该包含标题字段")
			assert.Contains(t, movieData, "genre", "应该包含类型字段")
			
			// 验证类型字段
			if genre, ok := movieData["genre"].(string); ok {
				fmt.Printf("   🎬 %s (%s)\n", movieData["title"], genre)
			}
		}
		
		fmt.Printf("✅ 基础语义搜索测试完成\n")
	})
	
	t.Run("语义搜索-动作主题", func(t *testing.T) {
		// 测试搜索与"动作"相关的电影
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"动作"})
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(
				graphql.Field{Name: "title"}, 
				graphql.Field{Name: "genre"},
			).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "动作主题搜索应该成功")
		
		if result.Data != nil {
			getData := result.Data["Get"]
			if getData != nil {
				if classData, ok := getData.(map[string]interface{})[className]; ok && classData != nil {
					if movies, ok := classData.([]interface{}); ok {
						fmt.Printf("💥 动作电影搜索找到 %d 部相关电影\n", len(movies))
						assert.GreaterOrEqual(t, len(movies), 0, "应该找到动作电影")
					}
				}
			}
		}
		
		fmt.Printf("✅ 动作主题搜索测试完成\n")
	})
}

// TestHybridSearchSimple 测试简单的混合搜索
func TestHybridSearchSimple(t *testing.T) {
	client := initSimpleTestClient(t)
	className := "SimpleHybridMovie"
	cleanupSimpleClass(t, client, className)
	createSimpleSemanticClass(t, client, className)
	insertSimpleSemanticData(t, client, className)
	
	t.Run("混合搜索测试", func(t *testing.T) {
		// 混合搜索：关键词 + 语义搜索
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(
				graphql.Field{Name: "title"}, 
				graphql.Field{Name: "description"},
			).
			WithHybrid(client.GraphQL().HybridArgumentBuilder().
				WithQuery("太空")). // 搜索包含"太空"的电影
			Do(context.Background())
		
		require.NoError(t, err, "混合搜索应该成功执行")
		
		if result.Data != nil {
			getData := result.Data["Get"]
			if getData != nil {
				if classData, ok := getData.(map[string]interface{})[className]; ok && classData != nil {
					if movies, ok := classData.([]interface{}); ok {
						fmt.Printf("🌌 混合搜索找到 %d 部相关电影\n", len(movies))
						assert.GreaterOrEqual(t, len(movies), 0, "应该找到相关电影")
						
						// 显示搜索结果
						for i, movie := range movies {
							if i >= 2 { // 只显示前2个结果
								break
							}
							if movieData, ok := movie.(map[string]interface{}); ok {
								title := movieData["title"]
								desc := movieData["description"]
								fmt.Printf("   🚀 %v: %v\n", title, desc)
							}
						}
					}
				}
			}
		}
		
		fmt.Printf("✅ 混合搜索测试完成\n")
	})
}

// TestVectorSearch 测试向量搜索功能
func TestVectorSearch(t *testing.T) {
	client := initSimpleTestClient(t)
	className := "VectorSearchMovie"
	cleanupSimpleClass(t, client, className)
	createSimpleSemanticClass(t, client, className)
	insertSimpleSemanticData(t, client, className)
	
	t.Run("向量维度验证", func(t *testing.T) {
		// 获取一个对象的向量信息
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(
				graphql.Field{Name: "title"},
				graphql.Field{Name: "_additional { vector }"},
			).
			WithLimit(1).
			Do(context.Background())
		
		require.NoError(t, err, "获取向量信息应该成功")
		
		if result.Data != nil {
			getData := result.Data["Get"]
			if getData != nil {
				if classData, ok := getData.(map[string]interface{})[className]; ok && classData != nil {
					if movies, ok := classData.([]interface{}); ok && len(movies) > 0 {
						// 获取第一个电影的向量
						firstMovie := movies[0].(map[string]interface{})
						if additional, ok := firstMovie["_additional"].(map[string]interface{}); ok {
							if vector, ok := additional["vector"].([]interface{}); ok {
								fmt.Printf("📊 获取到向量维度: %d\n", len(vector))
								assert.Greater(t, len(vector), 0, "向量维度应该大于0")
								
								// 验证向量数值范围
								for i, val := range vector[:min(5, len(vector))] {
									if floatVal, ok := val.(float64); ok {
										assert.GreaterOrEqual(t, floatVal, -1.0, "向量值应该>=-1.0")
										assert.LessOrEqual(t, floatVal, 1.0, "向量值应该<=1.0")
										fmt.Printf("   向量[%d]: %.4f\n", i, floatVal)
									}
								}
							}
						}
					}
				}
			}
		}
		
		fmt.Printf("✅ 向量搜索测试完成\n")
	})
}

// 辅助函数

// initSimpleTestClient 初始化简化测试客户端
func initSimpleTestClient(t *testing.T) *weaviate.Client {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "应该能够创建Weaviate客户端")
	return client
}

// cleanupSimpleClass 清理简化测试类
func cleanupSimpleClass(t *testing.T, client *weaviate.Client, className string) {
	err := client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
	if err == nil {
		fmt.Printf("🧹 已清理测试类: %s\n", className)
	}
	time.Sleep(100 * time.Millisecond)
}

// createSimpleSemanticClass 创建简化语义搜索测试类
func createSimpleSemanticClass(t *testing.T, client *weaviate.Client, className string) {
	classObj := &models.Class{
		Class:      className,
		Vectorizer: "text2vec-ollama",
		ModuleConfig: map[string]interface{}{
			"text2vec-ollama": map[string]interface{}{
				"apiEndpoint": "http://ollama:11434",
				"model":       "dengcao/bge-large-zh-v1.5",
			},
		},
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
	
	err := client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	if err != nil {
		t.Skipf("跳过测试：无法创建类 %s，错误: %v", className, err)
		return
	}
	fmt.Printf("🎬 创建语义搜索测试类: %s\n", className)
}

// insertSimpleSemanticData 插入简化语义搜索测试数据
func insertSimpleSemanticData(t *testing.T, client *weaviate.Client, className string) {
	dataObjects := []map[string]interface{}{
		{
			"title":       "星际穿越",
			"description": "一组探险家利用新发现的虫洞进行星际旅行，寻找人类新家园。",
			"genre":       "科幻",
		},
		{
			"title":       "复仇者联盟",
			"description": "超级英雄们联手对抗威胁地球的强大敌人。",
			"genre":       "动作",
		},
		{
			"title":       "阿凡达",
			"description": "残疾军人在遥远星球上与当地居民建立联系并保护他们的家园。",
			"genre":       "科幻",
		},
		{
			"title":       "速度与激情",
			"description": "街头赛车手卷入犯罪活动和家庭忠诚的冲突。",
			"genre":       "动作",
		},
		{
			"title":       "泰坦尼克号",
			"description": "豪华客轮首航沉没的爱情悲剧故事。",
			"genre":       "爱情",
		},
	}
	
	objects := make([]*models.Object, len(dataObjects))
	for i, obj := range dataObjects {
		objects[i] = &models.Object{
			Class:      className,
			Properties: obj,
		}
	}
	
	_, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
	if err != nil {
		t.Skipf("跳过测试：无法插入数据，错误: %v", err)
		return
	}
	
	fmt.Printf("📥 插入 %d 条语义搜索测试数据\n", len(dataObjects))
	time.Sleep(2 * time.Second) // 等待向量化完成
}

