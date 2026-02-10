# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

**Weaviate** 是一个开源的、云原生的向量数据库，可以存储对象和向量，支持大规模语义搜索。它将向量相似性搜索与关键词过滤、检索增强生成（RAG）和重排序结合在单一查询接口中。常见用例包括 RAG 系统、语义和图像搜索、推荐引擎、聊天机器人和内容分类。

Weaviate 支持两种存储向量的方法：使用[集成模型](https://docs.weaviate.io/weaviate/model-providers)（OpenAI、Cohere、HuggingFace 等）在导入时自动向量化，或直接导入[预计算的向量嵌入](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。生产部署受益于内置的多租户、复制、RBAC 授权和[许多其他功能](#weaviate-features)。

要快速入门，请查看以下教程之一：

- [快速入门 - Weaviate Cloud](https://docs.weaviate.io/weaviate/quickstart)
- [快速入门 - 本地 Docker 实例](https://docs.weaviate.io/weaviate/quickstart/local)

## 安装

Weaviate 提供多种安装和部署选项：

- [Docker](https://docs.weaviate.io/deploy/installation-guides/docker-installation)
- [Kubernetes](https://docs.weaviate.io/deploy/installation-guides/k8s-installation)
- [Weaviate Cloud](https://console.weaviate.cloud)

有关更多部署选项，请参阅[安装文档](https://docs.weaviate.io/deploy)，例如[AWS](https://docs.weaviate.io/deploy/installation-guides/aws-marketplace)和[GCP](https://docs.weaviate.io/deploy/installation-guides/gcp-marketplace)。

## 入门指南

您可以轻松地使用[Docker](https://docs.docker.com/desktop/)启动 Weaviate 和本地向量嵌入模型。
创建一个 `docker-compose.yml` 文件：

```yml
services:
  weaviate:
    image: cr.weaviate.io/semitechnologies/weaviate:1.32.2
    ports:
      - "8080:8080"
      - "50051:50051"
    environment:
      ENABLE_MODULES: text2vec-model2vec
      MODEL2VEC_INFERENCE_API: http://text2vec-model2vec:8080

  # 在导入过程中生成向量的轻量级嵌入模型
  text2vec-model2vec:
    image: cr.weaviate.io/semitechnologies/model2vec-inference:minishlab-potion-base-32M
```

使用以下命令启动 Weaviate 和嵌入服务：

```bash
docker compose up -d
```

安装 Python 客户端（或使用其他[客户端库](#client-libraries-and-apis)）：

```bash
pip install -U weaviate-client
```

以下 Python 示例展示了如何轻松地用数据填充 Weaviate 数据库、创建向量嵌入并执行语义搜索：

```python
import weaviate
from weaviate.classes.config import Configure, DataType, Property

# 连接到 Weaviate
client = weaviate.connect_to_local()

# 创建集合
client.collections.create(
    name="Article",
    properties=[Property(name="content", data_type=DataType.TEXT)],
    vector_config=Configure.Vectors.text2vec_model2vec(),  # 使用向量化器在导入时生成嵌入
    # vector_config=Configure.Vectors.self_provided()  # 如果您想导入自己的预生成嵌入
)

# 插入对象并生成嵌入
articles = client.collections.get("Article")
articles.data.insert_many(
    [
        {"content": "向量数据库支持语义搜索"},
        {"content": "机器学习模型生成嵌入"},
        {"content": "Weaviate 支持混合搜索功能"},
    ]
)

# 执行语义搜索
results = articles.query.near_text(query="按含义搜索对象", limit=1)
print(results.objects[0])

client.close()
```

此示例使用 `Model2Vec` 向量化器，但您可以选择任何其他[嵌入模型提供商](https://docs.weaviate.io/weaviate/model-providers)或[自带预生成向量](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。

## 客户端库和 API

Weaviate 为多种编程语言提供客户端库：

- [Python](https://docs.weaviate.io/weaviate/client-libraries/python)
- [JavaScript/TypeScript](https://docs.weaviate.io/weaviate/client-libraries/typescript)
- [Java](https://docs.weaviate.io/weaviate/client-libraries/java)
- [Go](https://docs.weaviate.io/weaviate/client-libraries/go)
- C# (🚧 即将推出 🚧)

还有额外的[社区维护库](https://docs.weaviate.io/weaviate/client-libraries/community)。

Weaviate 暴露[REST API](https://docs.weaviate.io/weaviate/api/rest)、[gRPC API](https://docs.weaviate.io/weaviate/api/grpc)和[GraphQL API](https://docs.weaviate.io/weaviate/api/graphql)与数据库服务器通信。

## Weaviate 功能

这些功能使您能够构建 AI 驱动的应用程序：

- **⚡ 快速搜索性能**：在毫秒内对数十亿向量执行复杂的语义[搜索](https://docs.weaviate.io/weaviate/search/similarity)。Weaviate 的架构用 Go 构建，速度和可靠性兼备，确保您的 AI 应用程序即使在重负载下也高度响应。请参阅我们的[ANN 基准测试](https://docs.weaviate.io/weaviate/benchmarks/ann)了解更多信息。

- **🔌 灵活的向量化**：使用来自 OpenAI、Cohere、HuggingFace、Google 等的[集成向量化器](https://docs.weaviate.io/weaviate/model-providers)在导入时无缝向量化数据。或者您可以导入[您自己的向量嵌入](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors)。

- **🔍 高级混合和图像搜索**：将语义搜索的强大功能与传统的[关键词（BM25）搜索](https://docs.weaviate.io/weaviate/search/bm25)、[图像搜索](https://docs.weaviate.io/weaviate/search/image)和[高级过滤](https://docs.weaviate.io/weaviate/search/filters)相结合，通过单一 API 调用获得最佳结果。

- **🤖 集成的 RAG 和重排序**：通过内置的[生成搜索（RAG）](https://docs.weaviate.io/weaviate/search/generative)和[重排序](https://docs.weaviate.io/weaviate/search/rerank)功能超越简单检索。直接从您的数据库中驱动复杂的问题解答系统、聊天机器人和摘要器，无需额外工具。

- **📈 生产就绪且可扩展**：Weaviate 专为关键任务应用程序而构建。从快速原型设计到大规模生产部署，原生支持[水平扩展](https://docs.weaviate.io/deploy/configuration/horizontal-scaling)、[多租户](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)、[复制](https://docs.weaviate.io/deploy/configuration/replication)和细粒度的[基于角色的访问控制（RBAC）](https://docs.weaviate.io/weaviate/configuration/rbac)。

- **💰 成本效益的操作**：通过内置的[向量压缩](https://docs.weaviate.io/weaviate/configuration/compression)大幅降低资源消耗和运营成本。向量量化和多向量编码在最小影响搜索性能的情况下减少内存使用。

有关所有功能的完整列表，请访问[官方 Weaviate 文档](https://docs.weaviate.io)。

## 有用资源

### 演示项目和配方

这些演示是展示 Weaviate 功能的工作应用程序。它们的源代码可在 GitHub 上获得。

- [Elysia](https://elysia.weaviate.io) ([GitHub](https://github.com/weaviate/elysia))：Elysia 是一个基于决策树的代理系统，能够智能地决定使用什么工具、获得了什么结果、是否应该继续该过程或是否已完成其目标。
- [Verba](https://weaviate.io/blog/verba-open-source-rag-app) ([GitHub](https://github.com/weaviate/verba))：一个社区驱动的开源应用程序，旨在提供端到端、简化和用户友好的检索增强生成（RAG）界面。
- [Healthsearch](https://weaviate.io/blog/healthsearch-demo) ([GitHub](https://github.com/weaviate/healthsearch-demo))：一个开源项目，旨在展示利用用户编写的评论和查询来检索基于特定健康效果的补充产品的潜力。
- Awesome-Moviate ([GitHub](https://github.com/weaviate-tutorials/awesome-moviate))：一个电影搜索和推荐引擎，允许基于关键词（BM25）、语义和混合搜索。

我们还维护广泛的**Jupyter 笔记本**和**TypeScript 代码片段**存储库，涵盖如何使用 Weaviate 功能和集成：

- [Weaviate Python 配方](https://github.com/weaviate/recipes/)
- [Weaviate TypeScript 配方](https://github.com/weaviate/recipes-ts/)

### 博客文章

- [什么是向量数据库](https://weaviate.io/blog/what-is-a-vector-database)
- [什么是向量搜索](https://weaviate.io/blog/vector-search-explained)
- [什么是混合搜索](https://weaviate.io/blog/hybrid-search-explained)
- [如何选择嵌入模型](https://weaviate.io/blog/how-to-choose-an-embedding-model)
- [什么是 RAG](https://weaviate.io/blog/introduction-to-rag)
- [RAG 评估](https://weaviate.io/blog/rag-evaluation)
- [高级 RAG 技术](https://weaviate.io/blog/advanced-rag)
- [什么是多模态 RAG](https://weaviate.io/blog/multimodal-rag)
- [什么是代理 RAG](https://weaviate.io/blog/what-is-agentic-rag)
- [什么是图 RAG](https://weaviate.io/blog/graph-rag)
- [后期交互模型概述](https://weaviate.io/blog/late-interaction-overview)

### Integrations

Weaviate integrates with many external services:

| Category                                                                                   | Description                                                | Integrations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **[Cloud Hyperscalers](https://docs.weaviate.io/integrations/cloud-hyperscalers)**         | Large-scale computing and storage                          | [AWS](https://docs.weaviate.io/integrations/cloud-hyperscalers/aws), [Google](https://docs.weaviate.io/integrations/cloud-hyperscalers/google)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **[Compute Infrastructure](https://docs.weaviate.io/integrations/compute-infrastructure)** | Run and scale containerized applications                   | [Modal](https://docs.weaviate.io/integrations/compute-infrastructure/modal), [Replicate](https://docs.weaviate.io/integrations/compute-infrastructure/replicate), [Replicated](https://docs.weaviate.io/integrations/compute-infrastructure/replicated)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **[Data Platforms](https://docs.weaviate.io/integrations/data-platforms)**                 | Data ingestion and web scraping                            | [Airbyte](https://docs.weaviate.io/integrations/data-platforms/airbyte), [Aryn](https://docs.weaviate.io/integrations/data-platforms/aryn), [Boomi](https://docs.weaviate.io/integrations/data-platforms/boomi), [Box](https://docs.weaviate.io/integrations/data-platforms/box), [Confluent](https://docs.weaviate.io/integrations/data-platforms/confluent), [Astronomer](https://docs.weaviate.io/integrations/data-platforms/astronomer), [Context Data](https://docs.weaviate.io/integrations/data-platforms/context-data), [Databricks](https://docs.weaviate.io/integrations/data-platforms/databricks), [Firecrawl](https://docs.weaviate.io/integrations/data-platforms/firecrawl), [IBM](https://docs.weaviate.io/integrations/data-platforms/ibm), [Unstructured](https://docs.weaviate.io/integrations/data-platforms/unstructured)                |
| **[LLM and Agent Frameworks](https://docs.weaviate.io/integrations/llm-agent-frameworks)** | Build agents and generative AI applications                | [Agno](https://docs.weaviate.io/integrations/llm-agent-frameworks/agno), [Composio](https://docs.weaviate.io/integrations/llm-agent-frameworks/composio), [CrewAI](https://docs.weaviate.io/integrations/llm-agent-frameworks/crewai), [DSPy](https://docs.weaviate.io/integrations/llm-agent-frameworks/dspy), [Dynamiq](https://docs.weaviate.io/integrations/llm-agent-frameworks/dynamiq), [Haystack](https://docs.weaviate.io/integrations/llm-agent-frameworks/haystack), [LangChain](https://docs.weaviate.io/integrations/llm-agent-frameworks/langchain), [LlamaIndex](https://docs.weaviate.io/integrations/llm-agent-frameworks/llamaindex), [N8n](https://docs.weaviate.io/integrations/llm-agent-frameworks/n8n), [Semantic Kernel](https://docs.weaviate.io/integrations/llm-agent-frameworks/semantic-kernel)                                   |
| **[Operations](https://docs.weaviate.io/integrations/operations)**                         | Tools for monitoring and analyzing generative AI workflows | [AIMon](https://docs.weaviate.io/integrations/operations/aimon), [Arize](https://docs.weaviate.io/integrations/operations/arize), [Cleanlab](https://docs.weaviate.io/integrations/operations/cleanlab), [Comet](https://docs.weaviate.io/integrations/operations/comet), [DeepEval](https://docs.weaviate.io/integrations/operations/deepeval), [Langtrace](https://docs.weaviate.io/integrations/operations/langtrace), [LangWatch](https://docs.weaviate.io/integrations/operations/langwatch), [Nomic](https://docs.weaviate.io/integrations/operations/nomic), [Patronus AI](https://docs.weaviate.io/integrations/operations/patronus), [Ragas](https://docs.weaviate.io/integrations/operations/ragas), [TruLens](https://docs.weaviate.io/integrations/operations/trulens), [Weights & Biases](https://docs.weaviate.io/integrations/operations/wandb) |

## Contributing

We welcome and appreciate contributions! Please see our [Contributor guide](https://docs.weaviate.io/contributor-guide) for the development setup, code style guidelines, testing requirements and the pull request process.

Join our [Slack community](https://weaviate.io/slack) or [Community forum](https://forum.weaviate.io/) to discuss ideas and get help.

## License

BSD 3-Clause License. See [LICENSE](./LICENSE) for details.
