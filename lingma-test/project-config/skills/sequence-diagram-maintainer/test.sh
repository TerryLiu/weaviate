#!/bin/bash

# 时序图技能测试脚本
# 用于验证 sequence-diagram-maintainer 技能是否正确安装和配置

set -e

echo "========================================="
echo "  时序图维护技能 - 安装验证"
echo "========================================="
echo ""

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查计数器
PASS=0
FAIL=0
WARN=0

# 检查函数
check_item() {
    local item=$1
    local path=$2
    
    if [ -e "$path" ]; then
        echo -e "${GREEN}✓${NC} $item"
        ((PASS++))
    else
        echo -e "${RED}✗${NC} $item"
        ((FAIL++))
    fi
}

warn_item() {
    local item=$1
    echo -e "${YELLOW}⚠${NC} $item"
    ((WARN++))
}

echo "1. 检查技能文件结构..."
echo "-----------------------------------------"
check_item "技能目录" ".lingma/skills/sequence-diagram-maintainer"
check_item "SKILL.md" ".lingma/skills/sequence-diagram-maintainer/SKILL.md"
check_item "README.md" ".lingma/skills/sequence-diagram-maintainer/README.md"
check_item "QUICKSTART.md" ".lingma/skills/sequence-diagram-maintainer/QUICKSTART.md"
echo ""

echo "2. 检查时序图文件..."
echo "-----------------------------------------"
check_item "diagrams 目录" "diagrams"
check_item "data-flow.md" "diagrams/data-flow.md"
echo ""

echo "3. 检查文件内容..."
echo "-----------------------------------------"

# 检查 SKILL.md 是否包含必要的内容
if grep -q "name: sequence-diagram-maintainer" .lingma/skills/sequence-diagram-maintainer/SKILL.md; then
    echo -e "${GREEN}✓${NC} SKILL.md 包含正确的 name 字段"
    ((PASS++))
else
    echo -e "${RED}✗${NC} SKILL.md 缺少 name 字段"
    ((FAIL++))
fi

if grep -q "description:" .lingma/skills/sequence-diagram-maintainer/SKILL.md; then
    echo -e "${GREEN}✓${NC} SKILL.md 包含 description 字段"
    ((PASS++))
else
    echo -e "${RED}✗${NC} SKILL.md 缺少 description 字段"
    ((FAIL++))
fi

if grep -q "mermaid" .lingma/skills/sequence-diagram-maintainer/SKILL.md; then
    echo -e "${GREEN}✓${NC} SKILL.md 包含 Mermaid 相关说明"
    ((PASS++))
else
    echo -e "${RED}✗${NC} SKILL.md 缺少 Mermaid 说明"
    ((FAIL++))
fi

# 检查 data-flow.md 是否包含时序图
if grep -q "```mermaid" diagrams/data-flow.md; then
    echo -e "${GREEN}✓${NC} data-flow.md 包含 Mermaid 时序图"
    ((PASS++))
else
    echo -e "${RED}✗${NC} data-flow.md 缺少 Mermaid 时序图"
    ((FAIL++))
fi

if grep -q "participant" diagrams/data-flow.md; then
    echo -e "${GREEN}✓${NC} data-flow.md 包含参与者声明"
    ((PASS++))
else
    echo -e "${RED}✗${NC} data-flow.md 缺少参与者声明"
    ((FAIL++))
fi

echo ""

echo "4. 检查文件权限..."
echo "-----------------------------------------"
if [ -r ".lingma/skills/sequence-diagram-maintainer/SKILL.md" ]; then
    echo -e "${GREEN}✓${NC} SKILL.md 可读"
    ((PASS++))
else
    echo -e "${RED}✗${NC} SKILL.md 不可读"
    ((FAIL++))
fi

if [ -r "diagrams/data-flow.md" ]; then
    echo -e "${GREEN}✓${NC} data-flow.md 可读"
    ((PASS++))
else
    echo -e "${RED}✗${NC} data-flow.md 不可读"
    ((FAIL++))
fi

echo ""

echo "5. 检查文件大小..."
echo "-----------------------------------------"
SKILL_SIZE=$(wc -l < .lingma/skills/sequence-diagram-maintainer/SKILL.md)
if [ "$SKILL_SIZE" -lt 500 ]; then
    echo -e "${GREEN}✓${NC} SKILL.md 行数合理 ($SKILL_SIZE 行)"
    ((PASS++))
else
    warn_item "SKILL.md 行数过多 ($SKILL_SIZE 行),建议精简"
fi

DIAGRAM_SIZE=$(wc -l < diagrams/data-flow.md)
if [ "$DIAGRAM_SIZE" -gt 0 ]; then
    echo -e "${GREEN}✓${NC} data-flow.md 非空 ($DIAGRAM_SIZE 行)"
    ((PASS++))
else
    echo -e "${RED}✗${NC} data-flow.md 为空"
    ((FAIL++))
fi

echo ""
echo "========================================="
echo "  验证结果汇总"
echo "========================================="
echo -e "${GREEN}通过: $PASS${NC}"
echo -e "${RED}失败: $FAIL${NC}"
if [ $WARN -gt 0 ]; then
    echo -e "${YELLOW}警告: $WARN${NC}"
fi
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}✓ 所有检查通过! 技能已正确安装。${NC}"
    echo ""
    echo "下一步:"
    echo "  1. 查看快速开始指南: cat .lingma/skills/sequence-diagram-maintainer/QUICKSTART.md"
    echo "  2. 查看现有时序图: cat diagrams/data-flow.md"
    echo "  3. 尝试更新时序图: 告诉 AI 添加新的组件关系"
    exit 0
else
    echo -e "${RED}✗ 存在失败的检查项,请检查上述错误。${NC}"
    exit 1
fi
