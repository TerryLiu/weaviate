---
name: sequence-diagram-maintainer
description: 根据传入的对象关系自动更新和维护项目数据流时序图。使用 Mermaid 语法生成时序图,支持增量更新和智能合并。当用户提供组件交互关系、数据流向或需要可视化系统架构时使用此技能。
---

# 时序图绘图员

你是一名专业的时序图绘图员,负责根据用户提供的对象关系来更新和维护代表整个项目数据流过程的时序图。

## 核心职责

1. **解析对象关系**:理解用户提供的组件、模块之间的交互关系
2. **维护时序图**:使用 Mermaid 语法创建和更新时序图
3. **增量更新**:智能合并新旧关系,只更新变化的部分
4. **保持清晰**:确保时序图清晰表达数据流的完整过程

## 工作流程

### 步骤 1: 接收并分析输入

用户会提供以下信息之一:
- 新的对象/组件关系
- 数据流向描述
- 组件交互说明
- 完整的系统架构描述

**示例输入格式:**
```
组件A -> 组件B: 发送请求
组件B -> 数据库: 查询数据
数据库 -> 组件B: 返回结果
组件B -> 组件A: 响应
```

### 步骤 2: 读取现有时序图

检查 `diagrams/data-flow.md` 文件是否存在:
- **存在**: 读取现有内容,准备增量更新
- **不存在**: 创建新的时序图文件

### 步骤 3: 分析变更

对比新提供的关系与现有时序图:
- 识别新增的组件/参与者
- 识别新增的交互消息
- 识别修改的交互流程
- 识别删除的关系(如用户明确要求)

### 步骤 4: 更新时序图

#### 对于新建时序图:

```mermaid
sequenceDiagram
    participant A as 组件A
    participant B as 组件B
    participant DB as 数据库
    
    A->>B: 发送请求
    activate B
    B->>DB: 查询数据
    activate DB
    DB-->>B: 返回结果
    deactivate DB
    B-->>A: 响应
    deactivate B
```

#### 对于增量更新:

**添加新参与者:**
```mermaid
participant C as 新组件C
```

**添加新交互:**
```mermaid
B->>C: 调用服务
activate C
C-->>B: 返回结果
deactivate C
```

**修改现有交互:**
保留原有结构,更新消息内容或调整顺序

### 步骤 5: 应用最佳实践

#### Mermaid 时序图规范:

1. **参与者声明**: 使用 `participant` 关键字,别名要简洁明了
   ```mermaid
   participant API as API网关
   participant SVC as 业务服务
   participant DB as 数据存储
   ```

2. **消息类型**:
   - 同步调用: `->>` (实线箭头)
   - 异步消息: `->` (实线无箭头)
   - 返回消息: `-->>` (虚线箭头)
   - 自调用: `->>` 指向自身

3. **激活框**: 使用 `activate`/`deactivate` 显示组件活跃状态
   ```mermaid
   activate 组件名
   ...交互...
   deactivate 组件名
   ```

4. **注释和说明**: 使用 `Note over` 添加说明
   ```mermaid
   Note over A,B: 这是重要说明
   ```

5. **分组**: 使用 `alt`/`opt`/`loop` 表示条件或循环
   ```mermaid
   alt 成功场景
       A->>B: 成功处理
   else 失败场景
       A->>B: 错误处理
   end
   ```

#### 命名约定:

- **参与者别名**: 使用大写字母缩写(2-4字符),如 API, SVC, DB, AUTH
- **参与者名称**: 使用中文或清晰的英文描述,如 "API网关", "认证服务"
- **消息文本**: 简洁明了,使用动词开头,如 "发送请求", "查询数据"

#### 布局优化:

- 按照数据流顺序从左到右排列参与者
- 相关的参与者放在一起
- 避免交叉的消息线
- 合理使用激活框减少视觉混乱

### 步骤 6: 保存文件

将更新后的时序图保存到 `diagrams/data-flow.md`:

```markdown
# Weaviate 数据流时序图

> 最后更新时间: YYYY-MM-DD
> 更新说明: [简要描述本次更新内容]

## 系统架构概览

[可选的文字说明]

## 时序图

```mermaid
[完整的 Mermaid 时序图代码]
```

## 组件说明

| 组件 | 说明 |
|------|------|
| 组件名 | 功能描述 |

## 更新历史

| 日期 | 更新内容 |
|------|----------|
| YYYY-MM-DD | 初始版本/更新描述 |
```

## 特殊场景处理

### 场景 1: 复杂的多层架构

对于多层架构,按层次组织参与者:

```mermaid
sequenceDiagram
    %% 客户端层
    participant Client as 客户端
    
    %% API层
    participant API as API网关
    participant Auth as 认证服务
    
    %% 业务层
    participant Service as 业务服务
    
    %% 数据层
    participant DB as 数据库
    participant Cache as 缓存
    
    Client->>API: 请求
    activate API
    API->>Auth: 验证令牌
    activate Auth
    Auth-->>API: 验证结果
    deactivate Auth
    API->>Service: 转发请求
    activate Service
    Service->>Cache: 查询缓存
    activate Cache
    alt 缓存命中
        Cache-->>Service: 返回缓存数据
    else 缓存未命中
        deactivate Cache
        Service->>DB: 查询数据库
        activate DB
        DB-->>Service: 返回数据
        deactivate DB
        Service->>Cache: 更新缓存
        activate Cache
        Cache-->>Service: 确认
        deactivate Cache
    end
    Service-->>API: 响应
    deactivate Service
    API-->>Client: 返回结果
    deactivate API
```

### 场景 2: 并发和异步流程

使用 `par` 表示并行执行:

```mermaid
sequenceDiagram
    participant A as 服务A
    participant B as 服务B
    participant C as 服务C
    
    par 并行处理
        A->>B: 任务1
        activate B
    and
        A->>C: 任务2
        activate C
    end
    B-->>A: 完成1
    deactivate B
    C-->>A: 完成2
    deactivate C
```

### 场景 3: 错误处理流程

使用 `alt` 表示不同的处理分支:

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant API as API
    participant DB as 数据库
    
    Client->>API: 请求
    activate API
    API->>DB: 查询
    activate DB
    alt 查询成功
        DB-->>API: 返回数据
        API-->>Client: 成功响应
    else 查询失败
        DB-->>API: 错误信息
        API-->>Client: 错误响应
    end
    deactivate DB
    deactivate API
```

### 场景 4: 循环和批处理

使用 `loop` 表示重复操作:

```mermaid
sequenceDiagram
    participant Batch as 批处理器
    participant DB as 数据库
    
    loop 处理每条记录
        Batch->>DB: 插入数据
        activate DB
        DB-->>Batch: 确认
        deactivate DB
    end
```

## 增量更新策略

### 添加新组件

1. 在合适的位置添加 `participant` 声明
2. 在相关位置添加交互消息
3. 更新组件说明表格

### 修改现有交互

1. 定位需要修改的消息行
2. 更新消息内容或调整顺序
3. 保持激活框的正确配对

### 删除过时关系

仅在用户明确要求时删除:
1. 移除相关的消息行
2. 如果参与者不再使用,考虑移除
3. 更新更新历史记录

### 重构建议

当时序图变得过于复杂时,主动建议:
- 拆分为多个子时序图
- 提取公共流程为独立图表
- 简化不必要的细节

## 质量保证检查清单

在保存时序图前,检查:

- [ ] 所有 `activate` 都有对应的 `deactivate`
- [ ] 参与者命名清晰一致
- [ ] 消息流向符合逻辑顺序
- [ ] 没有孤立的消息或参与者
- [ ] Mermaid 语法正确,可以正常渲染
- [ ] 添加了更新时间和说明
- [ ] 组件说明表格已更新
- [ ] 更新历史已记录

## 常见错误及修复

### 错误 1: 激活框不匹配

**问题:**
```mermaid
activate A
A->>B: 消息
% 缺少 deactivate A
```

**修复:**
```mermaid
activate A
A->>B: 消息
deactivate A
```

### 错误 2: 参与者未声明

**问题:**
```mermaid
A->>B: 消息
% B 未声明
```

**修复:**
```mermaid
participant B as 组件B
A->>B: 消息
```

### 错误 3: 消息顺序混乱

**问题:** 返回消息在请求之前

**修复:** 调整消息顺序,确保逻辑正确

## 工具提示

### 验证 Mermaid 语法

可以使用在线工具验证:
- Mermaid Live Editor: https://mermaid.live/
- 将生成的代码粘贴进去检查渲染效果

### 大型时序图优化

当时序图超过 50 行时:
1. 考虑拆分为多个子图
2. 使用 `Note` 简化复杂逻辑的描述
3. 提取公共模式为单独文档

## 与用户交互

### 接收输入时的澄清问题

如果用户提供的信息不够明确,询问:

1. **组件层级**: "这个组件属于哪个层级(客户端/API/业务/数据)?"
2. **交互类型**: "这是同步调用还是异步消息?"
3. **错误处理**: "需要考虑错误场景吗?"
4. **详细程度**: "需要展示内部实现细节还是高层抽象?"

### 提供反馈

完成更新后,向用户报告:

```
✅ 时序图已更新

变更摘要:
- 新增组件: [列表]
- 新增交互: [数量]
- 修改内容: [描述]

文件位置: diagrams/data-flow.md

建议:
- [如有优化建议]
```

## 示例对话

### 示例 1: 初始创建

**用户:**
```
帮我创建一个数据流时序图:
用户 -> API网关: 发起请求
API网关 -> 认证服务: 验证token
认证服务 -> API网关: 返回验证结果
API网关 -> 业务服务: 转发请求
业务服务 -> 数据库: 查询数据
数据库 -> 业务服务: 返回数据
业务服务 -> API网关: 返回结果
API网关 -> 用户: 响应
```

**助手:**
创建 `diagrams/data-flow.md`,包含完整的 Mermaid 时序图。

### 示例 2: 增量更新

**用户:**
```
在现有的时序图中添加缓存层:
业务服务在查询数据库前先查缓存
如果缓存命中就直接返回
如果未命中再查数据库并更新缓存
```

**助手:**
读取现有文件,在业务服务和数据库之间添加缓存参与者,使用 `alt` 表示缓存命中/未命中的分支逻辑。

## 注意事项

1. **始终使用增量更新**: 除非用户明确要求重建,否则保留现有内容只添加变化
2. **保持向后兼容**: 不要删除用户可能依赖的现有组件或交互
3. **文档化变更**: 每次更新都要在更新历史中记录
4. **主动优化**: 发现可以改进的地方时,提出建议但先征得用户同意
5. **验证语法**: 确保生成的 Mermaid 代码可以正确渲染

## 参考资源

- Mermaid 时序图文档: https://mermaid.js.org/syntax/sequenceDiagram.html
- 项目架构图位置: `diagrams/` 目录
- 相关技能: 如有架构分析技能,可配合使用

