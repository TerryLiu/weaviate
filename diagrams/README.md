# Diagrams - Weaviate 架构图表

本目录包含 Weaviate 项目的各种架构图表和时序图。

## 📊 可用图表

### 数据流时序图

**文件**: [data-flow.md](./data-flow.md)

展示 Weaviate 核心组件之间的数据流转和交互过程,包括:
- REST API 和 GraphQL API 的请求处理流程
- 认证授权机制
- Schema 管理和验证
- 向量索引和 AI 模块集成
- LSMKV 存储引擎

**查看方式**:
- 在 GitHub/GitLab 中直接查看(自动渲染 Mermaid)
- 使用支持 Mermaid 的编辑器(如 VS Code + Mermaid 插件)
- 在线查看: [Mermaid Live Editor](https://mermaid.live/)

## 🛠️ 维护和更新

### 使用时序图维护技能

本项目配备了专业的时序图维护技能 `sequence-diagram-maintainer`,可以智能地更新和维护时序图。

**快速开始**:

```bash
# 1. 查看现有时序图
cat diagrams/data-flow.md

# 2. 告诉 AI 你想要添加的关系
# 例如: "使用时序图技能,添加 Raft 共识层到 data-flow.md"

# 3. AI 会自动更新时序图
```

**技能文档位置**: `.lingma/skills/sequence-diagram-maintainer/`

**推荐阅读顺序**:
1. [INDEX.md](../.lingma/skills/sequence-diagram-maintainer/INDEX.md) - 文档导航
2. [QUICKSTART.md](../.lingma/skills/sequence-diagram-maintainer/QUICKSTART.md) - 快速上手
3. [DEMO.md](../.lingma/skills/sequence-diagram-maintainer/DEMO.md) - 实战示例

### 手动编辑

也可以直接编辑 `data-flow.md` 文件,但需要熟悉 Mermaid 语法。

**Mermaid 学习资源**:
- [官方文档](https://mermaid.js.org/)
- [时序图语法](https://mermaid.js.org/syntax/sequenceDiagram.html)
- [在线编辑器](https://mermaid.live/)

## 📝 添加新图表

如需添加新的架构图或时序图:

1. 在 `diagrams/` 目录创建新的 `.md` 文件
2. 使用 Mermaid 语法绘制图表
3. 在本 README 中添加链接和说明
4. 提交 Pull Request

**命名规范**:
- 使用小写字母和连字符
- 描述性名称,如 `raft-consensus.md`, `backup-flow.md`
- 避免空格和特殊字符

## 🔄 更新规范

### 何时更新

- ✅ 新增系统组件或服务
- ✅ 修改组件间交互方式
- ✅ 调整数据流向
- ✅ 架构重构后

### 更新步骤

1. 使用时序图维护技能或直接编辑
2. 更新"更新历史"表格
3. 运行验证(使用 Mermaid Live Editor)
4. 提交变更,提供清晰的 commit message

### Commit Message 示例

```
docs(diagrams): add Raft consensus layer to data flow

- Add Raft participant between Schema and Storage
- Show log replication process
- Update component descriptions

Refs: #123
```

## 📚 相关文档

- [Weaviate 架构文档](../docs/readme.md)
- [时序图维护技能](../.lingma/skills/sequence-diagram-maintainer/)
- [Mermaid 官方文档](https://mermaid.js.org/)

## 💡 最佳实践

1. **保持简洁**: 高层抽象优于过度详细
2. **及时更新**: 架构变更后立即更新图表
3. **版本管理**: 使用 Git 追踪变更历史
4. **团队协作**: 分享和维护共同的图表库
5. **定期审查**: 确保图表与实际架构一致

## ❓ 常见问题

### Q: 如何查看渲染后的图表?

A: 
- GitHub/GitLab 会自动渲染 Mermaid 代码
- 本地使用 VS Code + Mermaid 插件
- 或使用在线工具: https://mermaid.live/

### Q: 图表太大怎么办?

A:
- 拆分为多个子图
- 主图展示高层架构
- 子图展示详细流程
- 使用链接关联

### Q: 如何保证图表质量?

A:
- 使用时序图维护技能的检查清单
- 运行 `test.sh` 验证安装
- 团队成员互相审查
- 定期与实际架构对比

### Q: 可以创建其他类型的图表吗?

A:
当然可以!Mermaid 支持多种图表类型:
- 时序图 (sequenceDiagram)
- 流程图 (flowchart)
- 类图 (classDiagram)
- 状态图 (stateDiagram)
- ER 图 (erDiagram)
- 等等...

## 🤝 贡献指南

欢迎贡献新的图表或改进现有图表:

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/new-diagram`)
3. 添加或更新图表
4. 提交变更 (`git commit -am 'Add new diagram'`)
5. 推送到分支 (`git push origin feature/new-diagram`)
6. 创建 Pull Request

## 📞 获取帮助

- 📖 查看技能文档: `.lingma/skills/sequence-diagram-maintainer/INDEX.md`
- 💬 团队讨论: 提交 Issue 或在团队频道提问
- 🐛 报告问题: 创建 Issue 并附上详细信息

---

**最后更新**: 2026-05-07  
**维护者**: Weaviate Team  
**许可证**: 与 Weaviate 项目保持一致
