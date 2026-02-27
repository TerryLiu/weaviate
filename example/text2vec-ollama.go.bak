// text2vec-ollama.go - 演示如何使用Ollama向量嵌入和生成模型与Weaviate集成
package main

import (
	"context" // 上下文包，用于控制请求的生命周期
	"fmt"     // 格式化输入输出包
	"time"    // 时间处理包

	"github.com/weaviate/weaviate-go-client/v5/weaviate" // Weaviate Go客户端
	"github.com/weaviate/weaviate/entities/models"       // Weaviate数据模型
)

// main 函数 - 程序入口点
func main() {
    // 步骤 1.1: 连接到本地Weaviate实例
    cfg := weaviate.Config{
        Host:   "localhost:8080",  // Weaviate服务地址
        Scheme: "http",           // 使用HTTP协议
    }
    client, err := weaviate.NewClient(cfg)  // 创建Weaviate客户端
    if err != nil {
        panic(err)  // 如果连接失败则panic退出
    }

    // 步骤 1.2: 检查并清理已存在的Movie类
    className := "Movie"
    fmt.Printf("🔍 检查类 '%s' 是否存在...\n", className)
    	
    // 尝试删除已存在的类
    err = client.Schema().ClassDeleter().WithClassName(className).Do(context.Background())
    if err != nil {
    	fmt.Printf("ℹ️  类 '%s' 不存在或删除失败: %v\n", className, err)
    } else {
    	fmt.Printf("✅ 已删除已存在的类 '%s'\n", className)
    }
    
    // 等待删除完成
    fmt.Println("⏳ 等待系统清理...")
    time.Sleep(1 * time.Second)
    
    // 步骤 1.3: 创建新的集合（类）
    classObj := &models.Class{
        Class:      "Movie",                    // 类名：电影
        Vectorizer: "text2vec-ollama",         // 向量化器：使用Ollama文本向量化模块
        ModuleConfig: map[string]interface{}{
            "text2vec-ollama": map[string]interface{}{  // 配置Ollama嵌入集成
                "apiEndpoint": "http://ollama:11434",   // Ollama API端点地址
                "model":       "dengcao/bge-large-zh-v1.5",      // 使用的嵌入模型名称
            },
            "generative-ollama": map[string]interface{}{ // 配置Ollama生成集成
                "apiEndpoint": "http://ollama:11434",   // Ollama API端点地址
                "model":       "llama3.2",              // 使用的生成模型名称
            },
        },
    }

    // 在Weaviate中创建类结构
    err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
    if err != nil {
        panic(err)  // 如果创建失败则panic退出
    }

    // 步骤 1.4: 导入三个电影对象数据
    dataObjects := []map[string]interface{}{
        {
            "title":       "黑客帝国",  // 电影标题
            "description": "一名计算机黑客了解到现实的真实本质以及他在对抗控制者战争中的角色。",  // 电影描述
            "genre":       "科幻",     // 电影类型
        },
        {
            "title":       "千与千寻",  // 电影标题
            "description": "一个小女孩被困在一个神秘的精神世界中，必须找到拯救父母并回家的方法。",  // 电影描述
            "genre":       "动画",     // 电影类型
        },
        {
            "title":       "指环王：护戒使者",  // 电影标题
            "description": "一个卑微的霍比特人和他的伙伴们踏上危险的旅程，要摧毁一枚强大的戒指来拯救中土世界。",  // 电影描述
            "genre":       "奇幻",     // 电影类型
        },
    }

    // 插入对象到Weaviate
    objects := make([]*models.Object, len(dataObjects))  // 创建对象切片
    for i, obj := range dataObjects {                     // 遍历数据对象
        objects[i] = &models.Object{
            Class:      "Movie",      // 指定所属类
            Properties: obj,          // 设置对象属性
        }
    }

    // 批量导入对象到Weaviate
    _, err = client.Batch().ObjectsBatcher().WithObjects(objects...).Do(context.Background())
    if err != nil {
        panic(err)  // 如果导入失败则panic退出
    }

    // 输出成功信息
    fmt.Printf("已导入并向量化 %d 个对象到Movie集合中\n", len(dataObjects))
}