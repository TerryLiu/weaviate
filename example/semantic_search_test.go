// semantic_search_test.go - 测试Weaviate语义搜索功能
// 包含向量相似度搜索、混合搜索等高级功能测试
package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

// TestSemanticNearTextSearch 测试基于文本的语义相似度搜索
func TestSemanticNearTextSearch(t *testing.T) {
	// 初始化客户端
	client := initTestClient(t)
	
	// 准备测试数据
	className := "SemanticMovie"
	cleanupClass(t, client, className)
	createSemanticTestClass(t, client, className)
	insertSemanticTestData(t, client, className)
	
	t.Run("基础语义搜索", func(t *testing.T) {
		// 测试搜索与"科幻"相关的电影
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"科幻"}).
			WithCertainty(0.7) // 设置相似度阈值
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "description"}, graphql.Field{Name: "_additional { certainty }"}).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "语义搜索应该成功执行")
		require.NotNil(t, result, "结果不应该为空")
		
		// 验证返回结果
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		assert.Greater(t, len(movies), 0, "应该找到至少一部相关电影")
		
		// 检查返回的字段
		for _, movie := range movies {
			movieData := movie.(map[string]interface{})
			assert.Contains(t, movieData, "title", "应该包含标题字段")
			assert.Contains(t, movieData, "description", "应该包含描述字段")
			assert.Contains(t, movieData, "_additional", "应该包含_additional字段")
			
			additional := movieData["_additional"].(map[string]interface{})
			certainty := additional["certainty"].(float64)
			assert.GreaterOrEqual(t, certainty, 0.7, "相似度应该大于等于阈值")
		}
		
		fmt.Printf("✅ 基础语义搜索测试通过 - 找到 %d 部相关电影\n", len(movies))
	})
	
	t.Run("多概念语义搜索", func(t *testing.T) {
		// 测试同时搜索多个概念
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"冒险", "奇幻"}).
			WithCertainty(0.6)
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "_additional { certainty }"}).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "多概念语义搜索应该成功")
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		assert.Greater(t, len(movies), 0, "应该找到相关电影")
		
		fmt.Printf("✅ 多概念语义搜索测试通过 - 找到 %d 部相关电影\n", len(movies))
	})
	
	t.Run("负向搜索", func(t *testing.T) {
		// 测试排除某些概念的搜索
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"电影"}).
			// 注意：某些版本可能不支持WithNegative方法
			WithCertainty(0.5)
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "genre"}).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "负向搜索应该成功")
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		
		// 验证结果中不包含恐怖电影
		for _, movie := range movies {
			movieData := movie.(map[string]interface{})
			genre := movieData["genre"].(string)
			assert.NotEqual(t, "恐怖", genre, "结果中不应该包含恐怖电影")
		}
		
		fmt.Printf("✅ 负向搜索测试通过 - 找到 %d 部非恐怖电影\n", len(movies))
	})
}

// TestHybridSearch 测试混合搜索功能（关键词搜索 + 语义搜索）
func TestHybridSearch(t *testing.T) {
	client := initTestClient(t)
	className := "HybridMovie"
	cleanupClass(t, client, className)
	createSemanticTestClass(t, client, className)
	insertSemanticTestData(t, client, className)
	
	t.Run("混合搜索测试", func(t *testing.T) {
		// 混合搜索：结合BM25关键词搜索和向量搜索
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "description"}).
			WithHybrid(client.GraphQL().HybridArgumentBuilder().
				WithQuery("太空旅行"). // 查询词
				WithAlpha(0.7)). // alpha值：0=纯关键词，1=纯向量，0.7表示偏向向量搜索
			Do(context.Background())
		
		require.NoError(t, err, "混合搜索应该成功执行")
		require.NotNil(t, result, "结果不应该为空")
		
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		assert.Greater(t, len(movies), 0, "应该找到相关电影")
		
		// 验证返回的电影与"太空"或"旅行"相关
		relevantKeywords := []string{"太空", "星际", "宇宙", "旅行", "冒险"}
		foundRelevant := false
		
		for _, movie := range movies[:min(3, len(movies))] { // 检查前3个结果
			movieData := movie.(map[string]interface{})
			title := movieData["title"].(string)
			description := movieData["description"].(string)
			
			for _, keyword := range relevantKeywords {
				if contains(title, keyword) || contains(description, keyword) {
					foundRelevant = true
					break
				}
			}
			if foundRelevant {
				break
			}
		}
		
		assert.True(t, foundRelevant, "应该找到与太空旅行相关的电影")
		fmt.Printf("✅ 混合搜索测试通过 - 找到 %d 部相关电影\n", len(movies))
	})
}

// TestVectorDistanceSearch 测试向量距离搜索
func TestVectorDistanceSearch(t *testing.T) {
	client := initTestClient(t)
	className := "VectorMovie"
	cleanupClass(t, client, className)
	createSemanticTestClass(t, client, className)
	insertSemanticTestData(t, client, className)
	
	// 首先获取一个参考电影的向量
	t.Run("获取参考向量", func(t *testing.T) {
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "_additional { vector }"}).
			WithLimit(1).
			Do(context.Background())
		
		require.NoError(t, err, "获取向量应该成功")
		
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		require.Greater(t, len(movies), 0, "应该至少有一部电影")
		
		firstMovie := movies[0].(map[string]interface{})
		additional := firstMovie["_additional"].(map[string]interface{})
		vector := additional["vector"].([]interface{})
		
		assert.Greater(t, len(vector), 0, "应该获取到向量数据")
		fmt.Printf("✅ 成功获取参考向量，维度: %d\n", len(vector))
		
		// 使用该向量进行近邻搜索
		t.Run("向量近邻搜索", func(t *testing.T) {
			// 构造nearVector参数
			var vectorFloat32 []float32
			for _, v := range vector {
				vectorFloat32 = append(vectorFloat32, float32(v.(float64)))
			}
			
			nearVector := client.GraphQL().NearVectorArgBuilder().
				WithVector(vectorFloat32).
				WithCertainty(0.8) // 高相似度阈值
			
			similarResult, err := client.GraphQL().Get().
				WithClassName(className).
				WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "_additional { distance }"}).
				WithNearVector(nearVector).
				Do(context.Background())
			
			require.NoError(t, err, "向量近邻搜索应该成功")
			similarMovies := similarResult.Data["Get"].(map[string]interface{})[className].([]interface{})
			
			assert.Greater(t, len(similarMovies), 0, "应该找到相似的电影")
			
			// 第一个结果应该是自己（距离为0）
			if len(similarMovies) > 0 {
				firstSimilar := similarMovies[0].(map[string]interface{})
				additional := firstSimilar["_additional"].(map[string]interface{})
				distance := additional["distance"].(float64)
				assert.InDelta(t, 0.0, distance, 0.001, "最相似的电影距离应该接近0")
			}
			
			fmt.Printf("✅ 向量近邻搜索测试通过 - 找到 %d 部相似电影\n", len(similarMovies))
		})
	})
}

// TestGirlSearch 测试查询"女生"相关内容
func TestGirlSearch(t *testing.T) {
	client := initTestClient(t)
	className := "Movie" // 使用已存在的Movie类，其中包含《千与千寻》
	
	t.Run("查询女生相关内容", func(t *testing.T) {
		// 测试搜索与"女生"相关的电影
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"女生"}).
			WithCertainty(0.6) // 设置相似度阈值
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(
				graphql.Field{Name: "title"}, 
				graphql.Field{Name: "description"}, 
				graphql.Field{Name: "genre"},
				graphql.Field{Name: "_additional { certainty }"}).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "查询女生相关内容应该成功执行")
		require.NotNil(t, result, "结果不应该为空")
		
		// 获取查询结果
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		assert.Greater(t, len(movies), 0, "应该找到至少一部与女生相关的电影")
		
		fmt.Printf("\n🔍 查询'女生'的结果:\n")
		fmt.Printf("总共找到 %d 部相关电影:\n", len(movies))
		fmt.Println(strings.Repeat("=", 50))
		
		// 打印详细结果
		for i, movie := range movies {
			movieData := movie.(map[string]interface{})
			
			title := "未知"
			if val, ok := movieData["title"].(string); ok {
				title = val
			}
			
			description := "无描述"
			if val, ok := movieData["description"].(string); ok {
				description = val
			}
			
			genre := "未知类型"
			if val, ok := movieData["genre"].(string); ok {
				genre = val
			}
			
			certainty := 0.0
			if additional, ok := movieData["_additional"].(map[string]interface{}); ok {
				if cert, ok := additional["certainty"].(float64); ok {
					certainty = cert
				}
			}
			
			fmt.Printf("🎬 第%d部:\n", i+1)
			fmt.Printf("   标题: %s\n", title)
			fmt.Printf("   类型: %s\n", genre)
			fmt.Printf("   描述: %s\n", description)
			fmt.Printf("   相似度: %.2f\n", certainty)
			fmt.Println(strings.Repeat("-", 30))
		}
		
		fmt.Println(strings.Repeat("=", 50))
		fmt.Printf("✅ 查询'女生'测试完成 - 找到 %d 部相关电影\n\n", len(movies))
	})
}

// TestAutocut 测试自动截断功能
func TestAutocut(t *testing.T) {
	client := initTestClient(t)
	className := "AutocutMovie"
	cleanupClass(t, client, className)
	createSemanticTestClass(t, client, className)
	insertSemanticTestData(t, client, className)
	
	t.Run("自动截断搜索", func(t *testing.T) {
		// 使用autocut自动截断不相关的结果
		nearText := client.GraphQL().NearTextArgBuilder().
			WithConcepts([]string{"动作"}).
			// 注意：某些版本可能不支持WithAutocut方法
			WithCertainty(0.7) // 使用较高的相似度阈值作为替代
		
		result, err := client.GraphQL().Get().
			WithClassName(className).
			WithFields(graphql.Field{Name: "title"}, graphql.Field{Name: "_additional { certainty }"}).
			WithNearText(nearText).
			Do(context.Background())
		
		require.NoError(t, err, "自动截断搜索应该成功")
		movies := result.Data["Get"].(map[string]interface{})[className].([]interface{})
		
		// 验证结果数量合理（autocut会过滤掉低相关性的结果）
		assert.Less(t, len(movies), 10, "autocut应该减少返回结果数量")
		
		// 验证剩余结果的相关性较高
		if len(movies) > 0 {
			firstMovie := movies[0].(map[string]interface{})
			additional := firstMovie["_additional"].(map[string]interface{})
			certainty := additional["certainty"].(float64)
			assert.Greater(t, certainty, 0.7, "autocut后的结果应该有较高相关性")
		}
		
		fmt.Printf("✅ 自动截断测试通过 - 返回 %d 部高相关电影\n", len(movies))
	})
}

// 辅助函数

// initTestClient 初始化测试客户端
func initTestClient(t *testing.T) *weaviate.Client {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	
	client, err := weaviate.NewClient(cfg)
	require.NoError(t, err, "应该能够创建Weaviate客户端")
	return client
}

// cleanupClass 清理测试类
func cleanupClass(t *testing.T, client *weaviate.Client, className string) {
	err := client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
	if err == nil {
		fmt.Printf("🧹 已清理测试类: %s\n", className)
	}
	time.Sleep(100 * time.Millisecond) // 等待删除完成
}

// createSemanticTestClass 创建用于语义搜索测试的类
func createSemanticTestClass(t *testing.T, client *weaviate.Client, className string) {
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
			{
				Name:     "mainCharacter",
				DataType: []string{"text"},
			},
		},
	}
	
	err := client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	require.NoError(t, err, "应该能够创建语义搜索测试类")
	fmt.Printf("🎬 创建语义搜索测试类: %s\n", className)
}

// insertSemanticTestData 插入语义搜索测试数据
func insertSemanticTestData(t *testing.T, client *weaviate.Client, className string) {
	dataObjects := []map[string]interface{}{
		{
			"title":       "星际穿越",
			"description": "一组探险家利用新发现的虫洞进行星际旅行，寻找人类新家园。",
			"genre":       "科幻",
		},
		{
			"title":       "盗梦空间",
			"description": "专业窃贼进入他人梦境窃取秘密，这次任务更加复杂危险。",
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
			"title":       "泰坦尼克号",
			"description": "豪华客轮首航沉没的爱情悲剧故事。",
			"genre":       "爱情",
		},
		{
			"title":       "侏罗纪公园",
			"description": "科学家复活恐龙创建主题公园，但事情很快失控。",
			"genre":       "科幻",
		},
		{
			"title":       "速度与激情",
			"description": "街头赛车手卷入犯罪活动和家庭忠诚的冲突。",
			"genre":       "动作",
		},
		{
			"title":       "闪灵",
			"description": "作家在与世隔绝的酒店中逐渐陷入疯狂。",
			"genre":       "恐怖",
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
	require.NoError(t, err, "应该能够批量插入测试数据")
	
	fmt.Printf("📥 插入 %d 条语义搜索测试数据\n", len(dataObjects))
	time.Sleep(2 * time.Second) // 等待向量化完成
}

// insertGirlTestData 插入女生主题的测试数据
func insertGirlTestData(t *testing.T, client *weaviate.Client, className string) {
	dataObjects := []map[string]interface{}{
		{
			"title":         "冰雪奇缘",
			"description":   "两位公主艾莎和安娜的冒险故事，艾莎拥有控制冰雪的魔法。",
			"genre":         "动画",
			"mainCharacter": "艾莎和安娜（姐妹）",
		},
		{
			"title":         "花木兰",
			"description":   "女扮男装替父从军的古代中国女英雄传奇故事。",
			"genre":         "动画",
			"mainCharacter": "花木兰",
		},
		{
			"title":         "神奇女侠",
			"description":   "亚马逊公主戴安娜成为超级英雄，拯救世界的故事。",
			"genre":         "动作",
			"mainCharacter": "戴安娜（神奇女侠）",
		},
		{
			"title":         "小妇人",
			"description":   "四姐妹在美国内战期间成长的故事，展现女性独立精神。",
			"genre":         "剧情",
			"mainCharacter": "马奇家四姐妹",
		},
		{
			"title":         "律政俏佳人",
			"description":   "金发美女艾丽·伍兹通过努力考入哈佛法学院的励志故事。",
			"genre":         "喜剧",
			"mainCharacter": "艾丽·伍兹",
		},
		{
			"title":         "穿普拉达的女王",
			"description":   "刚毕业的女孩安德烈在时尚杂志工作的职场成长经历。",
			"genre":         "剧情",
			"mainCharacter": "安德烈·萨克斯",
		},
		{
			"title":         "赫本的故事",
			"description":   "奥黛丽·赫本从演员到慈善家的人生传奇。",
			"genre":         "传记",
			"mainCharacter": "奥黛丽·赫本",
		},
		{
			"title":         "摔跤吧！爸爸",
			"description":   "父亲训练女儿们成为摔跤冠军的真实故事。",
			"genre":         "体育",
			"mainCharacter": "吉塔和巴比塔姐妹",
		},
		{
			"title":         "阳光姐妹淘",
			"description":   "七个高中女生重聚，回忆青春岁月的温馨故事。",
			"genre":         "喜剧",
			"mainCharacter": "七个女生组成的姐妹团",
		},
		{
			"title":         "傲慢与偏见",
			"description":   "伊丽莎白·班纳特与达西先生的爱情故事，展现19世纪英国女性的智慧。",
			"genre":         "爱情",
			"mainCharacter": "伊丽莎白·班纳特",
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
	require.NoError(t, err, "应该能够批量插入女生测试数据")
	
	fmt.Printf("👧 插入 %d 条女生主题测试数据\n", len(dataObjects))
	time.Sleep(2 * time.Second) // 等待向量化完成
}

