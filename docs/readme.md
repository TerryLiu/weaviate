# Weaviate 项目结构说明

## 项目概述

Weaviate 是一个开源的、云原生的向量数据库，用于存储对象和向量，支持大规模语义搜索。该项目采用 Go 语言编写，实现了向量相似性搜索与关键词过滤、检索增强生成（RAG）和重排序等功能的统一查询接口。

## 目录结构详解

### 核心业务逻辑层

#### `/adapters` - 适配器层
负责不同组件间的连接和适配，包含三个主要子目录：

- **`/clients`** - 客户端适配器
  - 处理集群备份、分类、节点等远程调用
  - 实现跨节点的数据同步和复制机制
  - 包含 `remote_index.go` 等核心远程索引处理逻辑

- **`/handlers`** - 请求处理器
  - **`/graphql`** - GraphQL API 处理器
  - **`/grpc`** - gRPC API 处理器  
  - **`/rest`** - RESTful API 处理器
  - 统一处理各种协议的请求和响应

- **`/repos`** - 数据仓库适配器
  - **`/db`** - 核心数据库实现（157个文件）
  - **`/schema`** - Schema 管理
  - **`/classifications`** - 分类相关处理
  - **`/modules`** - 模块系统适配
  - **`/transactions`** - 事务管理

#### `/usecases` - 业务用例层
实现具体的业务逻辑和核心功能：

- **`/auth`** - 认证授权模块
- **`/backup`** - 备份恢复功能（23个文件）
- **`/classification`** - 自动分类系统（20个文件）
- **`/cluster`** - 集群管理和协调（19个文件）
- **`/config`** - 配置管理（17个文件）
- **`/objects`** - 对象操作和管理（39个文件）
- **`/schema`** - Schema 管理和验证（29个文件）
- **`/traverser`** - 查询遍历器（28个文件）
- **`/replica`** - 数据复制机制（22个文件）
- **`/sharding`** - 分片管理（9个文件）
- **`/monitoring`** - 监控和指标收集（10个文件）
- **`/modules`** - 模块系统管理（13个文件）

#### `/entities` - 实体定义层
定义项目中的核心数据结构和实体：

- **`/models`** - 核心数据模型（105个文件）
- **`/schema`** - Schema 相关实体定义
- **`/filters`** - 过滤条件实体
- **`/search`** - 搜索相关实体
- **`/errors`** - 错误类型定义
- **`/config`** - 配置实体
- **`/vectorindex`** - 向量索引相关实体
- **`/modulecapabilities`** - 模块能力接口定义

### 集群和分布式系统

#### `/cluster` - 集群管理系统
实现 Raft 一致性算法和分布式协调：

- **核心文件**：
  - `raft.go` - Raft 算法核心实现
  - `store.go` - 集群状态存储
  - `service.go` - 集群服务管理
- **子目录**：
  - **`/bootstrap`** - 集群启动引导
  - **`/replication`** - 分布式复制机制
  - **`/resolver`** - 节点解析器
  - **`/router`** - 请求路由
  - **`/rpc`** - 远程过程调用
  - **`/schema`** - 分布式 Schema 管理
  - **`/rbac`** - 基于角色的访问控制

### 模块生态系统

#### `/modules` - 功能模块系统
包含各种 AI 和 ML 集成功能模块：

**向量化模块**：
- `text2vec-*` - 文本向量化（OpenAI、Cohere、HuggingFace等）
- `multi2vec-*` - 多模态向量化
- `img2vec-*` - 图像向量化

**生成式 AI 模块**：
- `generative-*` - 各种 LLM 集成（OpenAI、Anthropic、Google等）

**重排序模块**：
- `reranker-*` - 结果重排序优化

**问答模块**：
- `qna-*` - 问答系统集成

**其他功能模块**：
- `ner-transformers` - 命名实体识别
- `sum-transformers` - 文本摘要
- `text-spellcheck` - 拼写检查

### 客户端和服务接口

#### `/client` - 客户端 SDK
提供各种编程语言的客户端接口：

- **各功能模块**：
  - `authz/` - 授权管理
  - `backups/` - 备份操作
  - `batch/` - 批量操作
  - `graphql/` - GraphQL 查询
  - `objects/` - 对象操作
  - `schema/` - Schema 管理

#### `/grpc` - gRPC 接口
高性能的二进制协议接口：

- `proto/` - Protocol Buffer 定义
- `generated/` - 自动生成的代码
- `conn/` - 连接管理

### 测试体系

#### `/test` - 测试套件
完整的测试框架和测试用例：

- **`/acceptance`** - 验收测试（22个子目录）
- **`/integration`** - 集成测试
- **`/modules`** - 模块测试（50个子目录）
- **`/benchmark`** - 性能基准测试
- **`/helper`** - 测试辅助工具
- **`/docker`** - Docker 环境测试

### 开发工具链

#### `/tools` - 开发和运维工具
- **`/dev`** - 开发者工具（23个文件）
- **`/ci`** - 持续集成工具
- **`/license_headers`** - 许可证头管理
- 脚本工具：代码生成、构建、发布等

### 文档和配置

#### `/docs` - 文档资料
- `metrics.md` - 监控指标文档

#### `/deprecations` - 弃用功能管理
- 管理即将废弃的功能和迁移路径

### 配置和部署

#### 根目录配置文件
- `go.mod/go.sum` - Go 依赖管理
- `Makefile` - 构建脚本
- `Dockerfile` - 容器镜像定义
- `docker-compose*.yml` - Docker 编排配置
- `.golangci.yml` - 代码质量检查配置
- `.gitignore` - 版本控制忽略规则

## 架构特点

### 分层架构

Handlers(API层) → Usecases(业务层) → Adapters(适配层) → Entities(实体层)

---


### 模块化设计
- 插件化的模块系统支持扩展
- 统一的模块接口定义
- 热插拔的 AI/ML 集成能力

### 分布式特性
- 基于 Raft 的强一致性
- 自动分片和负载均衡
- 多租户支持
- 水平扩展能力

### 云原生设计
- 容器化部署支持
- Kubernetes 原生集成
- 微服务友好架构

## 开发建议

### 学习路径
1. 先理解 `entities/models/` 中的核心数据结构
2. 学习 `usecases/` 中的业务逻辑实现
3. 掌握 `adapters/` 中的适配器模式
4. 了解 `cluster/` 中的分布式协调机制
5. 熟悉模块系统的扩展方式

### 贡献指南
- 遵循现有的代码风格和架构模式
- 新功能应通过模块系统实现
- 保持向后兼容性
- 完善的测试覆盖
- 详细的文档说明

### 最佳实践
- 使用依赖注入解耦组件
- 遵循接口隔离原则
- 合理使用泛型和反射
- 注意并发安全
- 重视错误处理和日志记录