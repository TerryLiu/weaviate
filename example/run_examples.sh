#!/bin/bash

# run_examples.sh - 运行Weaviate示例Go程序的脚本
# 作者: 自动生成
# 日期: $(date +%Y-%m-%d)

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Go环境
check_go_environment() {
    log_info "检查Go环境..."
    
    if ! command -v go &> /dev/null; then
        log_error "未找到Go命令，请先安装Go环境"
        exit 1
    fi
    
    GO_VERSION=$(go version | cut -d' ' -f3)
    log_success "Go版本: $GO_VERSION"
    
    # 检查go.mod是否存在
    if [ ! -f "go.mod" ]; then
        log_warning "未找到go.mod文件，正在初始化..."
        go mod init example
    fi
}

# 下载依赖
download_dependencies() {
    log_info "下载Go模块依赖..."
    go mod tidy
    log_success "依赖下载完成"
}

# 编译Go程序
compile_program() {
    local source_file=$1
    local output_name=$2
    
    log_info "编译 $source_file..."
    
    # 检查源文件是否存在
    if [ ! -f "$source_file" ]; then
        log_error "源文件 $source_file 不存在"
        return 1
    fi
    
    # 编译
    if go build -o "$output_name" "$source_file"; then
        log_success "编译成功: $output_name"
        return 0
    else
        log_error "编译失败: $source_file"
        return 1
    fi
}

# 运行Go程序
run_program() {
    local binary_name=$1
    local program_name=$2
    
    log_info "运行程序: $program_name"
    
    if [ -f "$binary_name" ]; then
        ./"$binary_name"
        log_success "程序 $program_name 执行完成"
    else
        log_error "可执行文件 $binary_name 不存在"
        return 1
    fi
}

# 运行Go源文件（直接运行）
run_go_source() {
    local source_file=$1
    
    log_info "直接运行Go源文件: $source_file"
    
    if [ -f "$source_file" ]; then
        go run "$source_file"
        log_success "源文件 $source_file 执行完成"
    else
        log_error "源文件 $source_file 不存在"
        return 1
    fi
}

# 运行测试文件
run_tests() {
    log_info "运行测试文件..."
    
    # 查找所有测试文件
    local test_files=$(find . -name "*_test.go" -type f)
    
    if [ -z "$test_files" ]; then
        log_warning "未找到测试文件"
        return 0
    fi
    
    log_info "找到以下测试文件:"
    echo "$test_files" | while read file; do
        echo "  - $file"
    done
    
    # 运行所有测试
    if go test -v ./...; then
        log_success "所有测试执行完成"
    else
        log_error "测试执行失败"
        return 1
    fi
}

# 清理编译产物
cleanup() {
    log_info "清理编译产物..."
    rm -f main text2vec-ollama
    log_success "清理完成"
}

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  run            运行主程序 (text2vec-ollama.go)"
    echo "  test           运行所有测试文件"
    echo "  all            运行程序和测试"
    echo "  clean          清理编译产物"
    echo "  help           显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 run         # 运行主程序"
    echo "  $0 test        # 运行测试"
    echo "  $0 all         # 运行程序和测试"
}

# 主函数
main() {
    local action=${1:-run}
    
    log_info "开始执行Weaviate示例程序..."
    
    # 检查环境
    check_go_environment
    download_dependencies
    
    case $action in
        "run")
            # 直接运行Go源文件（推荐方式）
            run_go_source "text2vec-ollama.go"
            ;;
        "compile")
            # 编译并运行
            if compile_program "text2vec-ollama.go" "text2vec-ollama"; then
                run_program "text2vec-ollama" "text2vec-ollama.go"
            fi
            ;;
        "test")
            run_tests
            ;;
        "all")
            # 先运行主程序，再运行测试
            run_go_source "text2vec-ollama.go"
            run_tests
            ;;
        "clean")
            cleanup
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            log_error "未知选项: $action"
            show_help
            exit 1
            ;;
    esac
    
    log_success "脚本执行完成"
}

# 执行主函数
main "$@"