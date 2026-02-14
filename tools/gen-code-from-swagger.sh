#!/usr/bin/env bash

# 设置严格的错误处理模式：
# -e: 遇到非零退出码立即退出
# -o pipefail: 管道中任何命令失败都会导致整个管道失败
# -u: 使用未定义变量时报错
set -eou pipefail

# 指定使用的 go-swagger 版本
version=v0.30.4

# 获取当前脚本所在目录的绝对路径
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# 构造 swagger 可执行文件的完整路径
SWAGGER=$DIR/swagger-${version}

# 获取当前系统的架构和操作系统信息
GOARCH=$(go env GOARCH)
GOOS=$(go env GOOS)

# 如果 swagger 可执行文件不存在，则从 GitHub 下载
if [ ! -f "$SWAGGER" ]; then
  if [ "$GOOS" = "linux" ]; then
    # Linux 系统下载对应架构的版本
    curl -o "$SWAGGER" -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_"$(echo `uname`|tr '[:upper:]' '[:lower:]')"_"$GOARCH"
  else
    # 其他系统默认下载 amd64 版本
    curl -o "$SWAGGER" -L'#' https://github.com/go-swagger/go-swagger/releases/download/$version/swagger_"$(echo `uname`|tr '[:upper:]' '[:lower:]')"_amd64
  fi
  # 给下载的文件添加可执行权限
  chmod +x "$SWAGGER"
fi

# 安装 goimports 工具，确保所有开发者使用相同版本进行代码格式化
go install golang.org/x/tools/cmd/goimports@v0.1.12

# 显式获取 yamlpc 包，用于 YAML 格式处理
(go get github.com/go-openapi/runtime/yamlpc@v0.29.2)

# 清理旧的生成文件，避免冲突
(cd "$DIR"/..; rm -rf entities/models client adapters/handlers/rest/operations/)

# 生成服务端代码：基于 OpenAPI 规范创建 REST API 服务器
# 参数说明：
# --name=weaviate: 服务名称
# --model-package=entities/models: 模型包路径
# --server-package=adapters/handlers/rest: 服务器包路径
# --spec=openapi-specs/schema.json: OpenAPI 规范文档路径
# -P models.Principal: 认证主体类型
# --default-scheme=https: 默认使用 HTTPS 协议
# --struct-tags=yaml,json: 为结构体添加 YAML 和 JSON 标签
(cd "$DIR"/..; $SWAGGER generate server --name=weaviate --model-package=entities/models --server-package=adapters/handlers/rest --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https --struct-tags=yaml --struct-tags=json)

# 生成客户端代码：创建与 Weaviate 服务交互的 Go 客户端
(cd "$DIR"/..; $SWAGGER generate client --name=weaviate --model-package=entities/models --spec=openapi-specs/schema.json -P models.Principal --default-scheme=https)

echo Generate Deprecation code...
# 生成弃用功能的相关代码
(cd "$DIR"/..; GO111MODULE=on GOWORK=off go generate ./deprecations)

echo Now add custom UnmarmarshalJSON code to models.Vectors swagger generated file.
# 为 swagger 生成的 models.Vectors 文件添加自定义的 JSON 反序列化代码
(cd "$DIR"/..; GO111MODULE=on go run ./tools/swagger_custom_code/main.go)

echo Now add the header to the generated code too.
# 为生成的代码文件添加许可证头部信息
(cd "$DIR"/..; GO111MODULE=on go run ./tools/license_headers/main.go)
# 使用 goimports 格式化代码，并排除隐藏文件和 protobuf 自动生成的文件
# 分步骤处理不同类型的文件以确保正确格式化：
# 1. 普通 Go 文件（不含测试文件），排除 test 目录
# 2. test 目录下的普通 Go 文件（不含测试文件）
# 3. 仅处理 *_test.go 测试文件
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*_test.go' -not -path './test/*' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))
(cd "$DIR"/..; goimports -w $(find . -type f -name '*.go' -not -name '*_test.go' -path './test/*' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))
(cd "$DIR"/..; goimports -w $(find . -type f -name '*_test.go' -not -name '*pb.go' -not -path './vendor/*' -not -path "./.*/*"))

# 检查是否有文件变更需要提交
CHANGED=$(git status -s | wc -l)
if [ "$CHANGED" -gt 0 ]; then
  echo "检测到有文件变更需要提交："
  git status -s
fi

echo Success
# 脚本执行成功完成
