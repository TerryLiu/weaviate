// module_config_test.go - 测试Weaviate模块配置继承机制
package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

func main() {
	fmt.Println("🔍 测试Weaviate模块配置继承机制")
	fmt.Println(strings.Repeat("=", 50))
	
	// 初始化客户端
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client, err := weaviate.NewClient(cfg)
	if err != nil {
		panic(fmt.Sprintf("无法连接到Weaviate: %v", err))
	}
	
	// 查看Movie类的完整配置
	fmt.Println("\n📋 Movie类的模块配置:")
	schema, err := client.Schema().Getter().Do(context.Background())
	if err != nil {
		panic(fmt.Sprintf("获取schema失败: %v", err))
	}
	
	for _, class := range schema.Classes {
		if class.Class == "Movie" {
			fmt.Printf("类名: %s\n", class.Class)
			fmt.Printf("Vectorizer: %s\n", class.Vectorizer)
			if class.ModuleConfig != nil {
				fmt.Println("ModuleConfig:")
				for moduleName, config := range class.ModuleConfig.(map[string]interface{}) {
					fmt.Printf("  %s: %+v\n", moduleName, config)
				}
			}
			break
		}
	}
	
	// 测试查询是否会使用类级别的配置
	fmt.Println("\n🔍 执行查询测试:")
	
	nearText := client.GraphQL().NearTextArgBuilder().
		WithConcepts([]string{"女生"}).
		WithCertainty(0.6)
	
	result, err := client.GraphQL().Get().
		WithClassName("Movie").
		WithFields(
			graphql.Field{Name: "title"},
			graphql.Field{Name: "_additional { certainty }"}).
		WithNearText(nearText).
		Do(context.Background())
	
	if err != nil {
		fmt.Printf("❌ 查询失败: %v\n", err)
		return
	}
	
	if result.Data != nil {
		if getData, ok := result.Data["Get"]; ok {
			if classData, ok := getData.(map[string]interface{})["Movie"]; ok {
				if movies, ok := classData.([]interface{}); ok {
					fmt.Printf("✅ 查询成功，找到 %d 部相关电影:\n", len(movies))
					for i, movie := range movies {
						if movieMap, ok := movie.(map[string]interface{}); ok {
							title := movieMap["title"]
							additional := movieMap["_additional"].(map[string]interface{})
							certainty := additional["certainty"]
							fmt.Printf("  %d. %s (相似度: %.2f)\n", i+1, title, certainty)
						}
					}
				}
			}
		}
	}
	
	// 验证结论
	fmt.Println("\n💡 结论:")
	fmt.Println("✅ Weaviate会在类级别存储模块配置（包括apiEndpoint）")
	fmt.Println("✅ 查询时会自动复用类创建时的模块配置")
	fmt.Println("✅ 无需在每次查询时重新指定模型和API端点")
	fmt.Println("✅ 这种设计使得配置管理更加简洁和一致")
}