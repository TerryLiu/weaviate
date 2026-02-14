//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  版权所有 © 2016 - 2026 Weaviate B.V. 保留所有权利。
//
//  联系方式: hello@weaviate.io
//

// REST API 配置包
package rest

import (
	"context"           // 上下文管理
	"encoding/base64"   // Base64 编码解码
	"encoding/json"     // JSON 序列化反序列化
	"fmt"               // 格式化输入输出
	"maps"              // 映射工具函数
	"net"               // 网络相关功能
	"net/http"          // HTTP 客户端和服务端实现
	_ "net/http/pprof"  // 性能分析工具
	"os"                // 操作系统功能
	"path/filepath"     // 文件路径操作
	"regexp"            // 正则表达式
	goruntime "runtime" // Go 运行时
	"runtime/debug"     // 运行时调试功能
	"strconv"           // 字符串转换
	"strings"           // 字符串操作
	"time"              // 时间处理

	"github.com/KimMachineGun/automemlimit/memlimit"
	armonmetrics "github.com/armon/go-metrics"
	armonprometheus "github.com/armon/go-metrics/prometheus"
	"github.com/getsentry/sentry-go"
	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/swag"
	"github.com/pbnjay/memory"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/weaviate/fgprof"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/authz"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	clusterapigrpc "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	replicationHandlers "github.com/weaviate/weaviate/adapters/handlers/rest/replication"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/handlers/rest/tenantactivity"
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	modulestorage "github.com/weaviate/weaviate/adapters/repos/modules"
	schemarepo "github.com/weaviate/weaviate/adapters/repos/schema"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/entities/concurrency"
	entconfig "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/replication"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	modstgazure "github.com/weaviate/weaviate/modules/backup-azure"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	modstggcs "github.com/weaviate/weaviate/modules/backup-gcs"
	modstgs3 "github.com/weaviate/weaviate/modules/backup-s3"
	modgenerativeanthropic "github.com/weaviate/weaviate/modules/generative-anthropic"
	modgenerativeanyscale "github.com/weaviate/weaviate/modules/generative-anyscale"
	modgenerativeaws "github.com/weaviate/weaviate/modules/generative-aws"
	modgenerativecohere "github.com/weaviate/weaviate/modules/generative-cohere"
	modgenerativecontextualai "github.com/weaviate/weaviate/modules/generative-contextualai"
	modgenerativedatabricks "github.com/weaviate/weaviate/modules/generative-databricks"
	modgenerativedummy "github.com/weaviate/weaviate/modules/generative-dummy"
	modgenerativefriendliai "github.com/weaviate/weaviate/modules/generative-friendliai"
	modgenerativegoogle "github.com/weaviate/weaviate/modules/generative-google"
	modgenerativemistral "github.com/weaviate/weaviate/modules/generative-mistral"
	modgenerativenvidia "github.com/weaviate/weaviate/modules/generative-nvidia"
	modgenerativeoctoai "github.com/weaviate/weaviate/modules/generative-octoai"
	modgenerativeollama "github.com/weaviate/weaviate/modules/generative-ollama"
	modgenerativeopenai "github.com/weaviate/weaviate/modules/generative-openai"
	modgenerativexai "github.com/weaviate/weaviate/modules/generative-xai"
	modimage "github.com/weaviate/weaviate/modules/img2vec-neural"
	modmulti2multivecjinaai "github.com/weaviate/weaviate/modules/multi2multivec-jinaai"
	modmulti2multivecweaviate "github.com/weaviate/weaviate/modules/multi2multivec-weaviate"
	modmulti2vecaws "github.com/weaviate/weaviate/modules/multi2vec-aws"
	modbind "github.com/weaviate/weaviate/modules/multi2vec-bind"
	modclip "github.com/weaviate/weaviate/modules/multi2vec-clip"
	modmulti2veccohere "github.com/weaviate/weaviate/modules/multi2vec-cohere"
	modmulti2vecgoogle "github.com/weaviate/weaviate/modules/multi2vec-google"
	modmulti2vecjinaai "github.com/weaviate/weaviate/modules/multi2vec-jinaai"
	modmulti2vecnvidia "github.com/weaviate/weaviate/modules/multi2vec-nvidia"
	modmulti2vecvoyageai "github.com/weaviate/weaviate/modules/multi2vec-voyageai"
	modner "github.com/weaviate/weaviate/modules/ner-transformers"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	modqnaopenai "github.com/weaviate/weaviate/modules/qna-openai"
	modqna "github.com/weaviate/weaviate/modules/qna-transformers"
	modcentroid "github.com/weaviate/weaviate/modules/ref2vec-centroid"
	modrerankercohere "github.com/weaviate/weaviate/modules/reranker-cohere"
	modrerankercontextualai "github.com/weaviate/weaviate/modules/reranker-contextualai"
	modrerankerdummy "github.com/weaviate/weaviate/modules/reranker-dummy"
	modrerankerjinaai "github.com/weaviate/weaviate/modules/reranker-jinaai"
	modrerankernvidia "github.com/weaviate/weaviate/modules/reranker-nvidia"
	modrerankertransformers "github.com/weaviate/weaviate/modules/reranker-transformers"
	modrerankervoyageai "github.com/weaviate/weaviate/modules/reranker-voyageai"
	modsum "github.com/weaviate/weaviate/modules/sum-transformers"
	modspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck"
	modtext2multivecjinaai "github.com/weaviate/weaviate/modules/text2multivec-jinaai"
	modtext2vecaws "github.com/weaviate/weaviate/modules/text2vec-aws"
	modt2vbigram "github.com/weaviate/weaviate/modules/text2vec-bigram"
	modcohere "github.com/weaviate/weaviate/modules/text2vec-cohere"
	modcontextionary "github.com/weaviate/weaviate/modules/text2vec-contextionary"
	moddatabricks "github.com/weaviate/weaviate/modules/text2vec-databricks"
	modtext2vecgoogle "github.com/weaviate/weaviate/modules/text2vec-google"
	modgpt4all "github.com/weaviate/weaviate/modules/text2vec-gpt4all"
	modhuggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface"
	modjinaai "github.com/weaviate/weaviate/modules/text2vec-jinaai"
	modmistral "github.com/weaviate/weaviate/modules/text2vec-mistral"
	modt2vmodel2vec "github.com/weaviate/weaviate/modules/text2vec-model2vec"
	modmorph "github.com/weaviate/weaviate/modules/text2vec-morph"
	modnvidia "github.com/weaviate/weaviate/modules/text2vec-nvidia"
	modtext2vecoctoai "github.com/weaviate/weaviate/modules/text2vec-octoai"
	modollama "github.com/weaviate/weaviate/modules/text2vec-ollama"
	modopenai "github.com/weaviate/weaviate/modules/text2vec-openai"
	modtransformers "github.com/weaviate/weaviate/modules/text2vec-transformers"
	modvoyageai "github.com/weaviate/weaviate/modules/text2vec-voyageai"
	modweaviateembed "github.com/weaviate/weaviate/modules/text2vec-weaviate"
	modusagegcs "github.com/weaviate/weaviate/modules/usage-gcs"
	modusages3 "github.com/weaviate/weaviate/modules/usage-s3"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/classification"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/telemetry"
	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
	"github.com/weaviate/weaviate/usecases/traverser"
)

// 最低要求的 Contextionary 版本号
const MinimumRequiredContextionaryVersion = "1.0.2"

// 创建服务器配置函数
// 返回一个用于配置 HTTP 服务器的函数
func makeConfigureServer(appState *state.State) func(*http.Server, string, string) {
	return func(s *http.Server, scheme, addr string) {
		// 将主机名和协议方案添加到配置中
		appState.ServerConfig.Hostname = addr
		appState.ServerConfig.Scheme = scheme
	}
}

// 向量仓库接口定义
// 组合了多个接口以提供完整的向量存储功能
type vectorRepo interface {
	objects.BatchVectorRepo      // 批量向量操作接口
	traverser.VectorSearcher     // 向量搜索接口
	classification.VectorRepo    // 分类向量接口
	SetSchemaGetter(schema.SchemaGetter) // 设置模式获取器
	WaitForStartup(ctx context.Context) error // 等待启动完成
	Shutdown(ctx context.Context) error       // 关闭服务
}

// 获取 CPU 核心数
// 从 cgroup 的 cpuset 文件中读取可用的 CPU 核心信息
func getCores() (int, error) {
	cpuset, err := os.ReadFile("/sys/fs/cgroup/cpuset/cpuset.cpus")
	if err != nil {
		return 0, errors.Wrap(err, "读取 cpuset 文件失败")
	}
	return calcCPUs(strings.TrimSpace(string(cpuset)))
}

// 计算 CPU 核心数
// 解析 CPU 字符串格式（如 "0-3,5,7-9"）并计算总核心数
func calcCPUs(cpuString string) (int, error) {
	cores := 0
	if cpuString == "" {
		return 0, nil
	}

	// 按逗号分割处理多个范围
	ranges := strings.Split(cpuString, ",")
	for _, r := range ranges {
		// 检查是否为范围格式（包含连字符）
		if strings.Contains(r, "-") {
			parts := strings.Split(r, "-")
			if len(parts) != 2 {
				return 0, fmt.Errorf("无效的 CPU 范围格式: %s", r)
			}
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, fmt.Errorf("无效的 CPU 范围起始值: %s", parts[0])
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, fmt.Errorf("无效的 CPU 范围结束值: %s", parts[1])
			}
			cores += end - start + 1
		} else {
			// 单个 CPU 核心
			cores++
		}
	}

	return cores, nil
}

// 创建应用状态
// 初始化整个应用程序的状态，包括配置、数据库、模块等核心组件
func MakeAppState(ctx, serverShutdownCtx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	build.Version = ParseVersionFromSwaggerSpec() // 版本号始终从 swagger 规范静态加载

	// config.ServerVersion 已弃用：为了向后兼容而保留
	// 请使用 build.Version 替代
	config.ServerVersion = build.Version

	appState := startupRoutine(ctx, serverShutdownCtx, options)

	// 初始化 OpenTelemetry 追踪
	if err := opentelemetry.Init(appState.Logger); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Error("初始化 OpenTelemetry 失败")
	}

	if appState.ServerConfig.Config.Monitoring.Enabled {
		// 初始化 HTTP 和 gRPC 服务器监控指标
		appState.HTTPServerMetrics = monitoring.NewHTTPServerMetrics(monitoring.DefaultMetricsNamespace, prometheus.DefaultRegisterer)
		appState.GRPCServerMetrics = monitoring.NewGRPCServerMetrics(monitoring.DefaultMetricsNamespace, prometheus.DefaultRegisterer)

		// 初始化租户活动处理器
		appState.TenantActivity = tenantactivity.NewHandler()

		// 由于我们正在抓取 prometheus.DefaultRegisterer，它已经在内部模块初始化时默认配置了
		// go 收集器。但是，默认配置的 go 收集器缺少一些有趣的指标，
		// 因此，我们必须先注销它以避免重复的指标声明，
		// 然后重新注册扩展收集器。
		prometheus.Unregister(collectors.NewGoCollector())
		prometheus.MustRegister(collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{
				Matcher: regexp.MustCompile(`/sched/latencies:seconds`),
			}),
		))

		// 导出构建标签到 prometheus 指标
		build.SetPrometheusBuildInfo()
		prometheus.MustRegister(version.NewCollector(build.AppName))

		// 配置 Armon Prometheus 选项
		opts := armonprometheus.PrometheusOpts{
			Expiration: 0, // 永不使指标过期
			Registerer: prometheus.DefaultRegisterer,
		}

		sink, err := armonprometheus.NewPrometheusSinkFrom(opts)
		if err != nil {
			appState.Logger.WithField("action", "startup").WithError(err).Fatal("创建 raft 指标的 prometheus 接收器失败")
		}

		// 配置 Armon 指标
		cfg := armonmetrics.DefaultConfig("weaviate_internal") // 区分来自内部/依赖包的指标
		cfg.EnableHostname = false                             // 不添加 `host` 标签
		cfg.EnableHostnameLabel = false                        // 不添加 `hostname` 标签
		cfg.EnableServiceLabel = false                         // 不添加 `service` 标签
		cfg.EnableRuntimeMetrics = false                       // 运行时指标已由 prometheus 提供
		cfg.EnableTypePrefix = true                            // 添加有意义的后缀来识别指标类型
		cfg.TimerGranularity = time.Second                     // 时间应始终以秒为单位

		_, err = armonmetrics.NewGlobal(cfg, sink)
		if err != nil {
			appState.Logger.WithField("action", "startup").WithError(err).Fatal("创建指标注册表 raft 指标失败")
		}

		// 目前唯一支持的监控工具是 prometheus
		enterrors.GoWrapper(func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			mux.Handle("/tenant-activity", appState.TenantActivity)
			http.ListenAndServe(fmt.Sprintf(":%d", appState.ServerConfig.Config.Monitoring.Port), mux)
		}, appState.Logger)
	}

	if appState.ServerConfig.Config.Sentry.Enabled {
		err := sentry.Init(sentry.ClientOptions{
			// 设置相关配置
			Dsn:         appState.ServerConfig.Config.Sentry.DSN,
			Debug:       appState.ServerConfig.Config.Sentry.Debug,
			Release:     "weaviate-core@" + build.Version,
			Environment: appState.ServerConfig.Config.Sentry.Environment,
			// 如果请求则启用追踪
			EnableTracing:    !appState.ServerConfig.Config.Sentry.TracingDisabled,
			AttachStacktrace: true,
			// 基于配置的采样率
			SampleRate:         appState.ServerConfig.Config.Sentry.ErrorSampleRate,
			ProfilesSampleRate: appState.ServerConfig.Config.Sentry.ProfileSampleRate,
			TracesSampler: sentry.TracesSampler(func(ctx sentry.SamplingContext) float64 {
				// 从父事务继承决策（如果有的话）是否采样
				if ctx.Parent != nil && ctx.Parent.Sampled != sentry.SampledUndefined {
					return 1.0
				}

				// 过滤掉不需要的追踪
				switch ctx.Span.Name {
				// 我们对与指标端点相关的追踪不感兴趣
				case "GET /metrics":
				// 这些是一些常见的网络机器人会垃圾邮件服务器。虽然不能全部捕获，但我们可以减少一些数量
				case "GET /favicon.ico":
				case "GET /t4":
				case "GET /ab2g":
				case "PRI *":
				case "GET /api/sonicos/tfa":
				case "GET /RDWeb/Pages/en-US/login.aspx":
				case "GET /_profiler/phpinfo":
				case "POST /wsman":
				case "POST /dns-query":
				case "GET /dns-query":
					return 0.0
				}

				// 过滤掉 graphql 查询，目前我们没有围绕它的上下文检测，因此
				// 除了 graphql resolve -> do -> return 之外，只是一行空白信息。
				if ctx.Span.Name == "POST /v1/graphql" {
					return 0.0
				}

				// 否则返回配置的采样率
				return appState.ServerConfig.Config.Sentry.TracesSampleRate
			}),
		})
		if err != nil {
			appState.Logger.
				WithField("action", "startup").WithError(err).
				Fatal("sentry 初始化失败")
		}

		sentry.ConfigureScope(func(scope *sentry.Scope) {
			// 使用 sentry 用户功能设置集群 ID 和集群所有者，以便在 UI 中区分多个集群
			scope.SetUser(sentry.User{
				ID:       appState.ServerConfig.Config.Sentry.ClusterId,
				Username: appState.ServerConfig.Config.Sentry.ClusterOwner,
			})
			// 设置任何定义的标签
			for key, value := range appState.ServerConfig.Config.Sentry.Tags {
				scope.SetTag(key, value)
			}
		})
	}

	// 限制资源使用
	limitResources(appState)

	// 创建集群 HTTP 客户端
	appState.ClusterHttpClient = reasonableHttpClient(appState.ServerConfig.Config.Cluster.AuthConfig, appState.ServerConfig.Config.MinimumInternalTimeout)
	// 创建内存监控器
	appState.MemWatch = memwatch.NewMonitor(memwatch.LiveHeapReader, debug.SetMemoryLimit, 0.97)

	var vectorRepo vectorRepo
	// var vectorMigrator schema.Migrator
	// var migrator schema.Migrator

	// 初始化指标注册器
	metricsRegisterer := monitoring.NoopRegisterer
	if appState.ServerConfig.Config.Monitoring.Enabled {
		promMetrics := monitoring.GetMetrics()
		metricsRegisterer = promMetrics.Registerer
		appState.Metrics = promMetrics
	}

	// TODO: 配置 HTTP 传输以实现高效的集群内通信
	// 创建远程索引客户端
	remoteIndexClient := clients.NewRemoteIndex(appState.ClusterHttpClient)
	// 创建远程节点客户端
	remoteNodesClient := clients.NewRemoteNode(appState.ClusterHttpClient)
	// 创建复制客户端
	replicationClient := clients.NewReplicationClient(appState.ClusterHttpClient)
	// 创建数据库仓库
	repo, err := db.New(appState.Logger, appState.Cluster.LocalName(), db.Config{
		// 服务器版本配置
		ServerVersion:                       config.ServerVersion,
		GitHash:                             build.Revision,
		// 内存表配置
		MemtablesFlushDirtyAfter:            appState.ServerConfig.Config.Persistence.MemtablesFlushDirtyAfter,
		MemtablesInitialSizeMB:              10,
		MemtablesMaxSizeMB:                  appState.ServerConfig.Config.Persistence.MemtablesMaxSizeMB,
		MemtablesMinActiveSeconds:           appState.ServerConfig.Config.Persistence.MemtablesMinActiveDurationSeconds,
		MemtablesMaxActiveSeconds:           appState.ServerConfig.Config.Persistence.MemtablesMaxActiveDurationSeconds,
		// MMap 相关配置
		MinMMapSize:                         appState.ServerConfig.Config.Persistence.MinMMapSize,
		LazySegmentsDisabled:                appState.ServerConfig.Config.Persistence.LazySegmentsDisabled,
		SegmentInfoIntoFileNameEnabled:      appState.ServerConfig.Config.Persistence.SegmentInfoIntoFileNameEnabled,
		WriteMetadataFilesEnabled:           appState.ServerConfig.Config.Persistence.WriteMetadataFilesEnabled,
		MaxReuseWalSize:                     appState.ServerConfig.Config.Persistence.MaxReuseWalSize,
		// LSM 段清理配置
		SegmentsCleanupIntervalSeconds:      appState.ServerConfig.Config.Persistence.LSMSegmentsCleanupIntervalSeconds,
		SeparateObjectsCompactions:          appState.ServerConfig.Config.Persistence.LSMSeparateObjectsCompactions,
		MaxSegmentSize:                      appState.ServerConfig.Config.Persistence.LSMMaxSegmentSize,
		CycleManagerRoutinesFactor:          appState.ServerConfig.Config.Persistence.LSMCycleManagerRoutinesFactor,
		IndexRangeableInMemory:              appState.ServerConfig.Config.Persistence.IndexRangeableInMemory,
		// 数据路径和查询配置
		RootPath:                            appState.ServerConfig.Config.Persistence.DataPath,
		QueryLimit:                          appState.ServerConfig.Config.QueryDefaults.Limit,
		QueryMaximumResults:                 appState.ServerConfig.Config.QueryMaximumResults,
		QueryHybridMaximumResults:           appState.ServerConfig.Config.QueryHybridMaximumResults,
		QueryNestedRefLimit:                 appState.ServerConfig.Config.QueryNestedCrossReferenceLimit,
		MaxImportGoroutinesFactor:           appState.ServerConfig.Config.MaxImportGoroutinesFactor,
		// 向量维度跟踪配置
		TrackVectorDimensions:               appState.ServerConfig.Config.TrackVectorDimensions || appState.Modules.UsageEnabled(),
		TrackVectorDimensionsInterval:       appState.ServerConfig.Config.TrackVectorDimensionsInterval,
		UsageEnabled:                        appState.Modules.UsageEnabled(),
		ResourceUsage:                       appState.ServerConfig.Config.ResourceUsage,
		// 性能优化配置
		AvoidMMap:                           appState.ServerConfig.Config.AvoidMmap,
		DisableLazyLoadShards:               appState.ServerConfig.Config.DisableLazyLoadShards,
		ForceFullReplicasSearch:             appState.ServerConfig.Config.ForceFullReplicasSearch,
		TransferInactivityTimeout:           appState.ServerConfig.Config.TransferInactivityTimeout,
		// 对象 TTL 配置
		ObjectsTTLBatchSize:                 appState.ServerConfig.Config.ObjectsTTLBatchSize,
		ObjectsTTLPauseEveryNoBatches:       appState.ServerConfig.Config.ObjectsTTLPauseEveryNoBatches,
		ObjectsTTLPauseDuration:             appState.ServerConfig.Config.ObjectsTTLPauseDuration,
		ObjectsTTLConcurrencyFactor:         appState.ServerConfig.Config.ObjectsTTLConcurrencyFactor,
		LSMEnableSegmentsChecksumValidation: appState.ServerConfig.Config.Persistence.LSMEnableSegmentsChecksumValidation,
		// 传递最小因子为 1 的虚拟复制配置。否则该设置不向后兼容。
		// 用户可能在引入更改之前创建了因子=1的类。现在如果所需的最小值高于1，
		// 他们的设置将无法启动。我们希望所需的最小值仅适用于新创建的类 - 
		// 而不是阻止加载现有的类。
		Replication: replication.GlobalConfig{
			MinimumFactor:                     1,
			AsyncReplicationDisabled:          appState.ServerConfig.Config.Replication.AsyncReplicationDisabled,
			AsyncReplicationClusterMaxWorkers: appState.ServerConfig.Config.Replication.AsyncReplicationClusterMaxWorkers,
		},
		MaximumConcurrentShardLoads:                  appState.ServerConfig.Config.MaximumConcurrentShardLoads,
		MaximumConcurrentBucketLoads:                 appState.ServerConfig.Config.MaximumConcurrentBucketLoads,
		HNSWMaxLogSize:                               appState.ServerConfig.Config.Persistence.HNSWMaxLogSize,
		HNSWDisableSnapshots:                         appState.ServerConfig.Config.Persistence.HNSWDisableSnapshots,
		HNSWSnapshotIntervalSeconds:                  appState.ServerConfig.Config.Persistence.HNSWSnapshotIntervalSeconds,
		HNSWSnapshotOnStartup:                        appState.ServerConfig.Config.Persistence.HNSWSnapshotOnStartup,
		HNSWSnapshotMinDeltaCommitlogsNumber:         appState.ServerConfig.Config.Persistence.HNSWSnapshotMinDeltaCommitlogsNumber,
		HNSWSnapshotMinDeltaCommitlogsSizePercentage: appState.ServerConfig.Config.Persistence.HNSWSnapshotMinDeltaCommitlogsSizePercentage,
		HNSWWaitForCachePrefill:                      appState.ServerConfig.Config.HNSWStartupWaitForVectorCache,
		HNSWFlatSearchConcurrency:                    appState.ServerConfig.Config.HNSWFlatSearchConcurrency,
		HNSWAcornFilterRatio:                         appState.ServerConfig.Config.HNSWAcornFilterRatio,
		HNSWGeoIndexEF:                               appState.ServerConfig.Config.HNSWGeoIndexEF,
		VisitedListPoolMaxSize:                       appState.ServerConfig.Config.HNSWVisitedListPoolMaxSize,
		TenantActivityReadLogLevel:                   appState.ServerConfig.Config.TenantActivityReadLogLevel,
		TenantActivityWriteLogLevel:                  appState.ServerConfig.Config.TenantActivityWriteLogLevel,
		QuerySlowLogEnabled:                          appState.ServerConfig.Config.QuerySlowLogEnabled,
		QuerySlowLogThreshold:                        appState.ServerConfig.Config.QuerySlowLogThreshold,
		InvertedSorterDisabled:                       appState.ServerConfig.Config.InvertedSorterDisabled,
		MaintenanceModeEnabled:                       appState.Cluster.MaintenanceModeEnabledForLocalhost,
		AsyncIndexingEnabled:                         appState.ServerConfig.Config.AsyncIndexingEnabled,
		HFreshEnabled:                                appState.ServerConfig.Config.HFreshEnabled,
		OperationalMode:                              appState.ServerConfig.Config.OperationalMode,
	}, remoteIndexClient, appState.Cluster, remoteNodesClient, replicationClient, appState.Metrics, appState.MemWatch, nil, nil, nil) // TODO 客户端
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("无效的新数据库")
	}

	// 设置数据库实例
	appState.DB = repo
	if appState.ServerConfig.Config.Monitoring.Enabled {
		appState.TenantActivity.SetSource(appState.DB)
	}

	// 设置调试处理器
	setupDebugHandlers(appState)
	// 设置 Go 性能分析
	setupGoProfiling(appState.ServerConfig.Config, appState.Logger)

	// 创建迁移器
	migrator := db.NewMigrator(repo, appState.Logger, appState.Cluster.LocalName())
	migrator.SetNode(appState.Cluster.LocalName())
	// TODO-offload: 当启用超过 S3 的模块时，"offload-s3" 必须来自配置
	migrator.SetOffloadProvider(appState.Modules, "offload-s3")
	appState.Migrator = migrator

	// 设置向量仓库
	vectorRepo = repo
	// migrator = vectorMigrator
	// 创建探索器
	explorer := traverser.NewExplorer(repo, appState.Logger, appState.Modules, traverser.NewMetrics(appState.Metrics), appState.ServerConfig.Config)
	// 创建模式仓库
	schemaRepo := schemarepo.NewStore(appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err = schemaRepo.Open(); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("无法初始化模式仓库")
		os.Exit(1)
	}

	// 创建本地分类器仓库
	localClassifierRepo, err := classifications.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("无法初始化分类器仓库")
		os.Exit(1)
	}

	// TODO: 配置 HTTP 传输以实现高效的集群内通信
	// 创建集群分类客户端
	classificationsTxClient := clients.NewClusterClassifications(appState.ClusterHttpClient)
	// 创建分布式分类仓库
	classifierRepo := classifications.NewDistributeRepo(classificationsTxClient,
		appState.Cluster, localClassifierRepo, appState.Logger)
	appState.ClassificationRepo = classifierRepo

	// 解析节点到端口的映射
	server2port, err := parseNode2Port(appState)
	if len(server2port) == 0 || err != nil {
		appState.Logger.
			WithField("action", "startup").
			WithField("raft-join", appState.ServerConfig.Config.Raft.Join).
			WithError(err).
			Fatal("解析 raft-join 失败")
		os.Exit(1)
	}

	// 获取节点名称和数据路径
	nodeName := appState.Cluster.LocalName()
	dataPath := appState.ServerConfig.Config.Persistence.DataPath

	// 创建模式解析器
	schemaParser := schema.NewParser(appState.Cluster, vectorIndex.ParseAndValidateConfig, migrator, appState.Modules, appState.ServerConfig.Config.DefaultQuantization)

	// 获取 gRPC 和认证配置
	grpcConfig := appState.ServerConfig.Config.GRPC
	authConfig := appState.ServerConfig.Config.Cluster.AuthConfig

	var creds credentials.TransportCredentials

	// 判断是否使用 TLS
	useTLS := len(grpcConfig.CertFile) > 0

	if useTLS {
		creds = credentials.NewClientTLSFromCert(nil, "")
	} else {
		creds = insecure.NewCredentials() // 测试时使用不安全的凭证
	}

	// 设置 gRPC 拨号选项
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}

	// 如果启用了基本认证，则添加认证拦截器
	if authConfig.BasicAuth.Enabled() {
		authHeader := grpcconn.BasicAuthHeader(authConfig.BasicAuth.Username, authConfig.BasicAuth.Password)
		opts = append(opts,
			grpc.WithUnaryInterceptor(grpcconn.BasicAuthUnaryInterceptor(authHeader)),
			grpc.WithStreamInterceptor(grpcconn.BasicAuthStreamInterceptor(authHeader)),
		)
	}

	// 计算最大消息大小和初始连接窗口大小
	maxSize := clusterapigrpc.GetMaxMessageSize(appState.ServerConfig.Config.ReplicationEngineFileCopyChunkSize)
	initialConnWindowSize := clusterapigrpc.GetInitialConnWindowSize(
		appState.ServerConfig.Config.ReplicationEngineFileCopyChunkSize,
		appState.ServerConfig.Config.ReplicationEngineFileCopyWorkers,
	)

	// 添加 gRPC 调用选项
	opts = append(opts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize),
		),
		grpc.WithInitialWindowSize(int32(maxSize)),
		grpc.WithInitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.WithReadBufferSize(clusterapigrpc.READ_BUFFER_SIZE),
		grpc.WithWriteBufferSize(clusterapigrpc.WRITE_BUFFER_SIZE),
	)

	// 获取 gRPC 连接配置
	grpcMaxOpenConns := appState.ServerConfig.Config.GRPC.MaxOpenConns
	grpcIddleConnTimeout := appState.ServerConfig.Config.GRPC.IdleConnTimeout

	// 创建 gRPC 连接管理器
	appState.GRPCConnManager = grpcconn.NewConnManager(grpcMaxOpenConns, grpcIddleConnTimeout,
		metricsRegisterer, appState.Logger, opts...)

	// 创建远程客户端工厂函数
	remoteClientFactory := func(ctx context.Context, address string) (copier.FileReplicationServiceClient, error) {
		clientConn, err := appState.GRPCConnManager.GetConn(address)
		if err != nil {
			return nil, fmt.Errorf("获取 gRPC 连接失败: %w", err)
		}
		return protocol.NewFileReplicationServiceClient(clientConn), nil
	}

	// 设置节点选择器
	var nodeSelector cluster.NodeSelector = appState.Cluster

	// 创建副本复制器
	replicaCopier := copier.New(remoteClientFactory, remoteIndexClient, nodeSelector,
		appState.ServerConfig.Config.ReplicationEngineFileCopyWorkers, dataPath, appState.DB, nodeName, appState.Logger)

	// 创建 Raft 集群配置
	rConfig := rCluster.Config{
		// 工作目录和节点标识
		WorkDir:                         filepath.Join(dataPath, config.DefaultRaftDir),
		NodeID:                          nodeName,
		Host:                            appState.Cluster.LocalAddr(),
		BindAddr:                        appState.Cluster.LocalBindAddr(),
		// 端口配置
		RaftPort:                        appState.ServerConfig.Config.Raft.Port,
		RPCPort:                         appState.ServerConfig.Config.Raft.InternalRPCPort,
		RaftRPCMessageMaxSize:           appState.ServerConfig.Config.Raft.RPCMessageMaxSize,
		// 超时配置
		BootstrapTimeout:                appState.ServerConfig.Config.Raft.BootstrapTimeout,
		BootstrapExpect:                 appState.ServerConfig.Config.Raft.BootstrapExpect,
		HeartbeatTimeout:                appState.ServerConfig.Config.Raft.HeartbeatTimeout,
		ElectionTimeout:                 appState.ServerConfig.Config.Raft.ElectionTimeout,
		LeaderLeaseTimeout:              appState.ServerConfig.Config.Raft.LeaderLeaseTimeout,
		TimeoutsMultiplier:              appState.ServerConfig.Config.Raft.TimeoutsMultiplier.Get(),
		// 快照配置
		SnapshotInterval:                appState.ServerConfig.Config.Raft.SnapshotInterval,
		SnapshotThreshold:               appState.ServerConfig.Config.Raft.SnapshotThreshold,
		TrailingLogs:                    appState.ServerConfig.Config.Raft.TrailingLogs,
		ConsistencyWaitTimeout:          appState.ServerConfig.Config.Raft.ConsistencyWaitTimeout,
		// 元数据配置
		MetadataOnlyVoters:              appState.ServerConfig.Config.Raft.MetadataOnlyVoters,
		EnableOneNodeRecovery:           appState.ServerConfig.Config.Raft.EnableOneNodeRecovery,
		ForceOneNodeRecovery:            appState.ServerConfig.Config.Raft.ForceOneNodeRecovery,
		// 组件配置
		DB:                              nil,
		Parser:                          schemaParser,
		NodeNameToPortMap:               server2port,
		NodeSelector:                    appState.Cluster,
		Logger:                          appState.Logger,
		IsLocalHost:                     appState.ServerConfig.Config.Cluster.Localhost,
		LoadLegacySchema:                schemaRepo.LoadLegacySchema,
		SentryEnabled:                   appState.ServerConfig.Config.Sentry.Enabled,
		// 权限和认证配置
		AuthzController:                 appState.AuthzController,
		RBAC:                            appState.RBAC,
		DynamicUserController:           appState.APIKey.Dynamic,
		ReplicaCopier:                   replicaCopier,
		AuthNConfig:                     appState.ServerConfig.Config.Authentication,
		// 复制引擎配置
		ReplicationEngineMaxWorkers:     appState.ServerConfig.Config.ReplicationEngineMaxWorkers,
		DistributedTasks:                appState.ServerConfig.Config.DistributedTasks,
		ReplicaMovementEnabled:          appState.ServerConfig.Config.ReplicaMovementEnabled,
		ReplicaMovementMinimumAsyncWait: appState.ServerConfig.Config.ReplicaMovementMinimumAsyncWait,
		DrainSleep:                      appState.ServerConfig.Config.Raft.DrainSleep.Get(),
	}
	// 检查当前节点是否为投票者
	for _, name := range appState.ServerConfig.Config.Raft.Join[:rConfig.BootstrapExpect] {
		if strings.Contains(name, rConfig.NodeID) {
			rConfig.Voter = true
			break
		}
	}

	// 创建集群服务
	appState.ClusterService = rCluster.New(rConfig, appState.AuthzController, appState.AuthzSnapshotter, appState.GRPCServerMetrics)
	// 设置迁移器的集群引用
	migrator.SetCluster(appState.ClusterService.Raft)

	// 创建模式执行器
	executor := schema.NewExecutor(migrator,
		appState.ClusterService.SchemaReader(),
		appState.Logger, backup.RestoreClassDir(dataPath),
	)

	// 获取卸载后端模块
	offloadmod, _ := appState.Modules.OffloadBackend("offload-s3")

	// 创建集合检索策略配置标志
	collectionRetrievalStrategyConfigFlag := configRuntime.NewFeatureFlag(
		configRuntime.CollectionRetrievalStrategyLDKey,
		string(configRuntime.LeaderOnly),
		appState.LDIntegration,
		configRuntime.CollectionRetrievalStrategyEnvVariable,
		appState.Logger,
	)

	// 创建模式管理器
	schemaManager, err := schema.NewManager(migrator,
		appState.ClusterService.Raft,
		appState.ClusterService.SchemaReader(),
		schemaRepo,
		appState.Logger, appState.Authorizer, &appState.ServerConfig.Config.SchemaHandlerConfig, appState.ServerConfig.Config,
		vectorIndex.ParseAndValidateConfig, appState.Modules, inverted.ValidateConfig,
		appState.Modules, appState.Cluster,
		offloadmod, *schemaParser,
		collectionRetrievalStrategyConfigFlag,
	)
	if err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("无法初始化模式管理器")
		os.Exit(1)
	}

	// 设置应用状态和仓库的相关组件
	appState.SchemaManager = schemaManager
	repo.SetNodeSelector(appState.ClusterService.NodeSelector())
	repo.SetSchemaReader(appState.ClusterService.SchemaReader())
	repo.SetReplicationFSM(appState.ClusterService.ReplicationFsm())
	repo.SetSchemaGetter(appState.SchemaManager)

	// 在所有组件准备就绪后初始化所需的服务
	postInitModules(appState)

	// 创建远程索引和节点接收器
	appState.RemoteIndexIncoming = sharding.NewRemoteIndexIncoming(repo, appState.ClusterService.SchemaReader(), appState.Modules)
	appState.RemoteNodeIncoming = sharding.NewRemoteNodeIncoming(repo)

	// 创建备份管理器
	backupManager := backup.NewHandler(appState.Logger, appState.ServerConfig.Config.Backup, appState.Authorizer,
		schemaManager, repo, appState.Modules, appState.RBAC, appState.APIKey.Dynamic)
	appState.BackupManager = backupManager

	// 创建内部服务器并启动
	appState.InternalServer = clusterapi.NewServer(appState)
	enterrors.GoWrapper(func() { appState.InternalServer.Serve() }, appState.Logger)

	// 设置各个组件的模式获取器
	vectorRepo.SetSchemaGetter(schemaManager)
	explorer.SetSchemaGetter(schemaManager)
	appState.Modules.SetSchemaGetter(schemaManager)

	// 创建遍历器
	appState.Traverser = traverser.NewTraverser(appState.ServerConfig,
		appState.Logger, appState.Authorizer, vectorRepo, explorer, schemaManager,
		appState.Modules, traverser.NewMetrics(appState.Metrics),
		appState.ServerConfig.Config.MaximumConcurrentGetRequests)

	// 注册模式更新回调
	updateSchemaCallback := makeUpdateSchemaCall(appState)
	executor.RegisterSchemaUpdateCallback(updateSchemaCallback)

	// 配置位图缓冲池
	bitmapBufPool, bitmapBufPoolClose := configureBitmapBufPool(appState)
	repo.WithBitmapBufPool(bitmapBufPool, bitmapBufPoolClose)

	// 配置重新索引器
	var reindexCtx context.Context
	reindexCtx, appState.ReindexCtxCancel = context.WithCancelCause(serverShutdownCtx)
	reindexer := configureReindexer(appState, reindexCtx)
	repo.WithReindexer(reindexer)

	// 创建元存储就绪状态管理器
	metaStoreReady := newMetaStoreReady()
	enterrors.GoWrapper(func() {
		// 打开集群服务和云元存储
		if err := appState.ClusterService.Open(context.Background(), executor); err != nil {
			appState.Logger.
				WithField("action", "startup").
				WithError(err).
				Fatal("无法打开云元存储")
			metaStoreReady.failure(err)
		} else {
			metaStoreReady.success()
		}
	}, appState.Logger)

	// TODO-RAFT: refactor remove this sleep
	// this sleep was used to block GraphQL and give time to RAFT to start.
	time.Sleep(2 * time.Second)

	appState.AutoSchemaManager = objects.NewAutoSchemaManager(schemaManager, vectorRepo, appState.ServerConfig, appState.Authorizer,
		appState.Logger, prometheus.DefaultRegisterer)
	batchManager := objects.NewBatchManager(vectorRepo, appState.Modules,
		schemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.Metrics, appState.AutoSchemaManager)
	appState.BatchManager = batchManager

	err = migrator.AdjustFilterablePropSettings(ctx)
	if err != nil {
		appState.Logger.
			WithError(err).
			WithField("action", "adjustFilterablePropSettings").
			Fatal("migration failed")
		os.Exit(1)
	}

	// FIXME to avoid import cycles, tasks are passed as strings
	reindexTaskNamesWithArgs := map[string]any{}
	reindexFinished := make(chan error, 1)

	if appState.ServerConfig.Config.ReindexSetToRoaringsetAtStartup {
		reindexTaskNamesWithArgs["ShardInvertedReindexTaskSetToRoaringSet"] = nil
	}
	if appState.ServerConfig.Config.IndexMissingTextFilterableAtStartup {
		reindexTaskNamesWithArgs["ShardInvertedReindexTaskMissingTextFilterable"] = nil
	}
	if len(appState.ServerConfig.Config.ReindexIndexesAtStartup) > 0 {
		reindexTaskNamesWithArgs["ShardInvertedReindexTask_SpecifiedIndex"] = appState.ServerConfig.Config.ReindexIndexesAtStartup
	}

	if len(reindexTaskNamesWithArgs) > 0 {
		// start reindexing inverted indexes (if requested by user) in the background
		// allowing db to complete api configuration and start handling requests
		enterrors.GoWrapper(func() {
			l := appState.Logger.WithField("action", "startup")
			if err := metaStoreReady.waitForMetaStore(); err != nil {
				l.WithError(err).Error("Reindexing inverted indexes skipped")
				return
			}
			l.Info("Reindexing inverted indexes")
			reindexFinished <- migrator.InvertedReindex(reindexCtx, reindexTaskNamesWithArgs)
		}, appState.Logger)
	}

	enterrors.GoWrapper(func() {
		l := appState.Logger.WithField("action", "startup")
		if err := metaStoreReady.waitForMetaStore(); err != nil {
			l.WithError(err).Error("Configuring crons skipped")
			return
		}
		l.Info("Configuring crons")
		appState.Crons.Init(appState.ClusterService, appState.ObjectTTLCoordinator)
	}, appState.Logger)

	configureServer = makeConfigureServer(appState)

	// Add dimensions to all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.ReindexVectorDimensionsAtStartup && repo.GetConfig().TrackVectorDimensions {
		appState.Logger.
			WithField("action", "startup").
			Info("Reindexing dimensions")
		migrator.RecalculateVectorDimensions(ctx)
	}

	// Add recount properties of all the objects in the database, if requested by the user
	if appState.ServerConfig.Config.RecountPropertiesAtStartup {
		migrator.RecountProperties(ctx)
	}

	if appState.ServerConfig.Config.DistributedTasks.Enabled {
		appState.DistributedTaskScheduler = distributedtask.NewScheduler(distributedtask.SchedulerParams{
			CompletionRecorder: appState.ClusterService.Raft,
			TasksLister:        appState.ClusterService.Raft,
			Providers:          map[string]distributedtask.Provider{},
			Logger:             appState.Logger,
			MetricsRegisterer:  metricsRegisterer,
			LocalNode:          appState.Cluster.LocalName(),
			TickInterval:       appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval,

			// Using a single global value for now to keep it simple. If there is a need
			// this can be changed to provide a value per provider.
			CompletedTaskTTL: appState.ServerConfig.Config.DistributedTasks.CompletedTaskTTL,
		})
		enterrors.GoWrapper(func() {
			// Do not launch scheduler until the full RAFT state is restored to avoid needlessly starting
			// and stopping tasks.
			// Additionally, not-ready RAFT state could lead to lose of local task metadata.
			if metaStoreReady.waitForMetaStore() != nil {
				return
			}
			if err = appState.DistributedTaskScheduler.Start(ctx); err != nil {
				appState.Logger.WithError(err).WithField("action", "startup").
					Error("failed to start distributed task scheduler")
			}
		}, appState.Logger)
	}

	return appState
}

func configureBitmapBufPool(appState *state.State) (pool roaringset.BitmapBufPool, close func()) {
	return roaringset.NewBitmapBufPoolDefault(appState.Logger, appState.Metrics,
		appState.ServerConfig.Config.QueryBitmapBufsMaxBufSize,
		appState.ServerConfig.Config.QueryBitmapBufsMaxMemory)
}

func configureReindexer(appState *state.State, reindexCtx context.Context) db.ShardReindexerV3 {
	tasks := []db.ShardReindexTaskV3{}
	logger := appState.Logger.WithField("action", "reindexV3")
	cfg := appState.ServerConfig.Config
	concurrency := concurrency.TimesFloatGOMAXPROCS(cfg.ReindexerGoroutinesFactor)

	if cfg.ReindexMapToBlockmaxAtStartup {
		tasks = append(tasks, db.NewShardInvertedReindexTaskMapToBlockmax(
			logger,
			cfg.ReindexMapToBlockmaxConfig.SwapBuckets,
			cfg.ReindexMapToBlockmaxConfig.UnswapBuckets,
			cfg.ReindexMapToBlockmaxConfig.TidyBuckets,
			cfg.ReindexMapToBlockmaxConfig.ReloadShards,
			cfg.ReindexMapToBlockmaxConfig.Rollback,
			cfg.ReindexMapToBlockmaxConfig.ConditionalStart,
			time.Second*time.Duration(cfg.ReindexMapToBlockmaxConfig.ProcessingDurationSeconds),
			time.Second*time.Duration(cfg.ReindexMapToBlockmaxConfig.PauseDurationSeconds),
			time.Millisecond*time.Duration(cfg.ReindexMapToBlockmaxConfig.PerObjectDelayMilliseconds),
			concurrency, cfg.ReindexMapToBlockmaxConfig.Selected, appState.SchemaManager,
		))
	}

	if len(tasks) == 0 {
		return db.NewShardReindexerV3Noop()
	}

	reindexer := db.NewShardReindexerV3(reindexCtx, logger, appState.DB.GetIndex, concurrency)
	for i := range tasks {
		reindexer.RegisterTask(tasks[i])
	}
	reindexer.Init()
	return reindexer
}

type metaStoreReady struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func newMetaStoreReady() *metaStoreReady {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &metaStoreReady{ctx: ctx, cancel: cancel}
}

func (msr *metaStoreReady) success() {
	msr.cancel(nil)
}

func (msr *metaStoreReady) failure(err error) {
	msr.cancel(err)
}

func (msr *metaStoreReady) waitForMetaStore() error {
	<-msr.ctx.Done()
	if err := context.Cause(msr.ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func parseNode2Port(appState *state.State) (m map[string]int, err error) {
	m = make(map[string]int, len(appState.ServerConfig.Config.Raft.Join))
	for _, raftNamePort := range appState.ServerConfig.Config.Raft.Join {
		np := strings.Split(raftNamePort, ":")
		if np[0] == appState.Cluster.LocalName() {
			m[np[0]] = appState.ServerConfig.Config.Raft.Port
			continue
		}
		if m[np[0]], err = strconv.Atoi(np[1]); err != nil {
			return m, fmt.Errorf("expect integer as raft port: got %s:: %w", raftNamePort, err)
		}
	}

	return m, nil
}

// parseVotersNames parses names of all voters.
// If we reach this point, we assume that the configuration is valid
func parseVotersNames(cfg config.Raft) (m map[string]struct{}) {
	m = make(map[string]struct{}, cfg.BootstrapExpect)
	for _, raftNamePort := range cfg.Join[:cfg.BootstrapExpect] {
		m[strings.Split(raftNamePort, ":")[0]] = struct{}{}
	}
	return m
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	serverShutdownCtx, serverShutdownCancel := context.WithCancelCause(context.Background())
	appState := MakeAppState(ctx, serverShutdownCtx, connectorOptionGroup)

	appState.Logger.WithFields(logrus.Fields{
		"server_version": config.ServerVersion,
		"version":        build.Version,
	}).Infof("configured versions")

	api.ServeError = openapierrors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = composer.New(
		appState.ServerConfig.Config.Authentication,
		appState.APIKey, appState.OIDC)

	api.Logger = func(msg string, args ...interface{}) {
		appState.Logger.WithFields(logrus.Fields{"action": "restapi_management", "version": build.Version}).Infof(msg, args...)
	}

	classifier := classification.New(appState.SchemaManager, appState.ClassificationRepo, appState.DB, // the DB is the vectorrepo
		appState.Authorizer,
		appState.Logger, appState.Modules)

	setupAuthnHandlers(api,
		appState.ClusterService.Raft,
		appState.ServerConfig.Config.Authorization.Rbac,
		appState.Logger)
	authz.SetupHandlers(api,
		appState.ClusterService.Raft,
		appState.SchemaManager,
		appState.ServerConfig.Config.Authentication.APIKey,
		appState.ServerConfig.Config.Authentication.OIDC,
		appState.ServerConfig.Config.Authorization.Rbac,
		appState.Metrics,
		appState.Authorizer,
		appState.Logger)

	replicationHandlers.SetupHandlers(appState.ServerConfig.Config.ReplicaMovementEnabled, api, appState.ClusterService.Raft, appState.Metrics, appState.Authorizer, appState.Logger)

	remoteDbUsers := clients.NewRemoteUser(appState.ClusterHttpClient, appState.Cluster)
	db_users.SetupHandlers(api, appState.ClusterService.Raft, appState.Authorizer, appState.ServerConfig.Config.Authentication, appState.ServerConfig.Config.Authorization, remoteDbUsers, appState.SchemaManager, appState.Logger)
	appState.ObjectTTLCoordinator = objectttl.NewCoordinator(appState.ClusterService.SchemaReader(), appState.SchemaManager, appState.DB, appState.Logger, appState.ClusterHttpClient, appState.Cluster)

	setupSchemaHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger)
	setupAliasesHandlers(api, appState.SchemaManager, appState.Metrics, appState.Logger)
	objectsManager := objects.NewManager(appState.SchemaManager, appState.ServerConfig, appState.Logger,
		appState.Authorizer, appState.DB, appState.Modules,
		objects.NewMetrics(appState.Metrics), appState.MemWatch, appState.AutoSchemaManager)
	setupObjectHandlers(api, objectsManager, appState.ServerConfig.Config, appState.Logger,
		appState.Modules, appState.Metrics)
	setupObjectBatchHandlers(api, appState.BatchManager, appState.Metrics, appState.Logger)
	setupGraphQLHandlers(api, appState, appState.SchemaManager, appState.ServerConfig.Config.DisableGraphQL,
		appState.Metrics, appState.Logger)
	setupMiscHandlers(api, appState.ServerConfig, appState.Modules,
		appState.Metrics, appState.Logger)
	setupClassificationHandlers(api, classifier, appState.Metrics, appState.Logger)
	backupScheduler := startBackupScheduler(appState)
	setupBackupHandlers(api, backupScheduler, appState.Metrics, appState.Logger)
	setupNodesHandlers(api, appState.SchemaManager, appState.DB, appState)
	if appState.ServerConfig.Config.DistributedTasks.Enabled {
		setupDistributedTasksHandlers(api, appState.Authorizer, appState.ClusterService.Raft)
	}

	var grpcInstrument []grpc.ServerOption
	if appState.ServerConfig.Config.Monitoring.Enabled {
		grpcInstrument = monitoring.InstrumentGrpc(appState.GRPCServerMetrics)
	}

	grpcServer, batchDrain := createGrpcServer(appState, grpcInstrument...)

	telemeter := telemetry.New(
		appState.DB,
		appState.SchemaManager,
		appState.Logger,
		getTelemetryURL(appState),
		appState.ServerConfig.Config.TelemetryPushInterval,
	)

	setupMiddlewares := makeSetupMiddlewares(appState)
	setupGlobalMiddleware := makeSetupGlobalMiddleware(appState, api.Context(), telemeter)
	if telemetryEnabled(appState) {
		enterrors.GoWrapper(func() {
			if err := telemeter.Start(context.Background()); err != nil {
				appState.Logger.
					WithField("action", "startup").
					Errorf("telemetry failed to start: %s", err.Error())
			}
		}, appState.Logger)
	}
	if entconfig.Enabled(os.Getenv("ENABLE_CLEANUP_UNFINISHED_BACKUPS")) {
		enterrors.GoWrapper(
			func() {
				// cleanup unfinished backups on startup
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				backupScheduler.CleanupUnfinishedBackups(ctx)
			}, appState.Logger)
	}

	api.PreServerShutdown = func() {
		batchDrain()
	}

	api.ServerShutdown = func() {
		// leave memberlist first to announce node graceful departure
		if err := appState.Cluster.Leave(); err != nil {
			appState.Logger.WithError(err).Error("leave node from cluster")
		}

		// drain any ongoing operations
		time.Sleep(appState.ServerConfig.Config.Raft.DrainSleep.Get())

		if telemetryEnabled(appState) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// must be shutdown before the db, to ensure the
			// termination payload contains the correct
			// object count
			if err := telemeter.Stop(ctx); err != nil {
				appState.Logger.WithField("action", "stop_telemetry").
					Errorf("failed to stop telemetry: %s", err.Error())
			}
		}

		// Shutdown OTEL tracing
		if err := opentelemetry.Shutdown(ctx); err != nil {
			appState.Logger.WithField("action", "stop_opentelemetry").
				Errorf("failed to stop opentelemetry: %s", err.Error())
		}

		serverShutdownCancel(fmt.Errorf("server shutdown"))

		if appState.DistributedTaskScheduler != nil {
			appState.DistributedTaskScheduler.Close()
		}

		// close grpc client connections
		appState.GRPCConnManager.Close()

		// gracefully stop gRPC server
		grpcServer.GracefulStop()

		if appState.ServerConfig.Config.Sentry.Enabled {
			sentry.Flush(2 * time.Second)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := appState.InternalServer.Close(ctx); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown internal server").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.ClusterService.Close(ctx); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown cluster service").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.APIKey.Dynamic.Close(); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown db users").
				Errorf("failed to gracefully shutdown")
		}

		if err := appState.Modules.Close(); err != nil {
			appState.Logger.
				WithError(err).
				WithField("action", "shutdown modules").
				Errorf("failed to gracefully shutdown")
		}
	}

	startGrpcServer(grpcServer, appState)

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func startBackupScheduler(appState *state.State) *backup.Scheduler {
	backupScheduler := backup.NewScheduler(
		appState.Authorizer,
		clients.NewClusterBackups(appState.ClusterHttpClient),
		appState.DB, appState.Modules,
		membership{appState.Cluster, appState.ClusterService},
		appState.SchemaManager,
		appState.Logger)
	return backupScheduler
}

// TODO: Split up and don't write into global variables. Instead return an appState
func startupRoutine(ctx, serverShutdownCtx context.Context, options *swag.CommandLineOptionsGroup) *state.State {
	appState := &state.State{}

	logger := logger()
	appState.Logger = logger

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("created startup context, nothing done so far")

	ldInteg, err := configRuntime.ConfigureLDIntegration()
	if err != nil {
		logger.WithField("action", "startup").Infof("Feature flag LD integration disabled: %s", err)
	}
	appState.LDIntegration = ldInteg
	// Load the config using the flags
	serverConfig := &config.WeaviateConfig{}
	appState.ServerConfig = serverConfig
	err = serverConfig.LoadConfig(options, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).Error("could not load config")
		logger.Exit(1)
	}
	// Initialize runtime config and load overridden config
	runtimeConfigManager := initRuntimeOverrides(appState)
	dataPath := serverConfig.Config.Persistence.DataPath
	if err := os.MkdirAll(dataPath, 0o777); err != nil {
		logger.WithField("action", "startup").
			WithField("path", dataPath).Error("cannot create data directory")
		logger.Exit(1)
	}

	monitoring.InitConfig(serverConfig.Config.Monitoring)

	if serverConfig.Config.DisableGraphQL {
		logger.WithFields(logrus.Fields{
			"action":          "startup",
			"disable_graphql": true,
		}).Warnf("GraphQL API disabled, relying only on gRPC API for querying. " +
			"This is considered experimental and will likely experience breaking changes " +
			"before reaching general availability")
	}

	logger.WithFields(logrus.Fields{
		"action":                    "startup",
		"default_vectorizer_module": serverConfig.Config.DefaultVectorizerModule,
	}).Infof("the default vectorizer modules is set to %q, as a result all new "+
		"schema classes without an explicit vectorizer setting, will use this "+
		"vectorizer", serverConfig.Config.DefaultVectorizerModule)

	logger.WithFields(logrus.Fields{
		"action":              "startup",
		"auto_schema_enabled": serverConfig.Config.AutoSchema.Enabled,
	}).Infof("auto schema enabled setting is set to \"%v\"", serverConfig.Config.AutoSchema.Enabled)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("config loaded")

	appState.OIDC = configureOIDC(appState)
	appState.APIKey = configureAPIKey(appState)
	appState.APIKeyRemote = apikey.NewRemoteApiKey(appState.APIKey)
	appState.AnonymousAccess = configureAnonymousAccess(appState)
	if err = configureAuthorizer(appState); err != nil {
		logger.WithField("action", "startup").WithField("error", err).Error("cannot configure authorizer")
		logger.Exit(1)
	}
	appState.Crons = configureCrons(appState, serverShutdownCtx)

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("configured OIDC and anonymous access client")

	logger.WithField("action", "startup").WithField("startup_time_left", timeTillDeadline(ctx)).
		Debug("initialized schema")

	var nonStorageNodes map[string]struct{}
	if cfg := serverConfig.Config.Raft; cfg.MetadataOnlyVoters {
		nonStorageNodes = parseVotersNames(cfg)
	}

	clusterState, err := cluster.Init(serverConfig.Config.Cluster, serverConfig.Config.Raft.TimeoutsMultiplier.Get(), dataPath, nonStorageNodes, logger)
	if err != nil {
		logger.WithField("action", "startup").WithError(err).
			Error("could not init cluster state")
		logger.Exit(1)
	}

	appState.Cluster = clusterState
	appState.Logger.
		WithField("action", "startup").
		Debug("startup routine complete")

	// Register enabled modules
	if err := registerModules(appState); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't load")
	}
	// while we accept an overall longer startup, e.g. due to a recovery, we
	// still want to limit the module startup context, as that's mostly service
	// discovery / dependency checking
	moduleCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if err := initModules(moduleCtx, appState); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("modules didn't initialize")
	}
	// now that modules are loaded we can run the remaining config validation
	// which is module dependent
	if err := appState.ServerConfig.Config.ValidateModules(appState.Modules); err != nil {
		appState.Logger.
			WithField("action", "startup").WithError(err).
			Fatal("invalid config")
	}

	// Initialize runtime config hooks and start runtime config background process
	postInitRuntimeOverrides(appState, runtimeConfigManager)

	return appState
}

// logger does not parse the regular config object, as logging needs to be
// configured before the configuration is even loaded/parsed. We are thus
// "manually" reading the desired env vars and set reasonable defaults if they
// are not set.
//
// Defaults to log level info and json format
func logger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(NewWeaviateTextFormatter())

	if os.Getenv("LOG_FORMAT") != "text" {
		logger.SetFormatter(NewWeaviateJSONFormatter())
	}
	logLevelStr := os.Getenv("LOG_LEVEL")
	level, err := logLevelFromString(logLevelStr)
	if errors.Is(err, errlogLevelNotRecognized) {
		logger.WithField("log_level_env", logLevelStr).Warn("log level not recognized, defaulting to info")
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	return logger
}

// everything hard-coded right now, to be made dynamic (from go plugins later)
func registerModules(appState *state.State) error {
	appState.Logger.
		WithField("action", "startup").
		Debug("start registering modules")

	appState.Modules = modules.NewProvider(appState.Logger, appState.ServerConfig.Config)

	// Default modules
	defaultVectorizers := []string{
		modtext2vecaws.Name,
		modmulti2veccohere.Name,
		modcohere.Name,
		moddatabricks.Name,
		modtext2vecgoogle.Name,
		modmulti2vecgoogle.Name,
		modhuggingface.Name,
		modjinaai.Name,
		modmulti2vecjinaai.Name,
		modmistral.Name,
		modtext2vecoctoai.Name,
		modopenai.Name,
		modmorph.Name,
		modvoyageai.Name,
		modmulti2vecvoyageai.Name,
		modweaviateembed.Name,
		modtext2multivecjinaai.Name,
		modnvidia.Name,
		modmulti2vecnvidia.Name,
		modmulti2multivecjinaai.Name,
		modmulti2multivecweaviate.Name,
		modmulti2vecaws.Name,
	}
	defaultGenerative := []string{
		modgenerativeanthropic.Name,
		modgenerativeanyscale.Name,
		modgenerativeaws.Name,
		modgenerativecohere.Name,
		modgenerativecontextualai.Name,
		modgenerativedatabricks.Name,
		modgenerativefriendliai.Name,
		modgenerativegoogle.Name,
		modgenerativemistral.Name,
		modgenerativenvidia.Name,
		modgenerativeoctoai.Name,
		modgenerativeopenai.Name,
		modgenerativexai.Name,
	}
	defaultOthers := []string{
		modrerankercohere.Name,
		modrerankercontextualai.Name,
		modrerankervoyageai.Name,
		modrerankerjinaai.Name,
		modrerankernvidia.Name,
	}

	defaultModules := append(defaultVectorizers, defaultGenerative...)
	defaultModules = append(defaultModules, defaultOthers...)

	var modules []string

	if len(appState.ServerConfig.Config.EnableModules) > 0 {
		modules = strings.Split(appState.ServerConfig.Config.EnableModules, ",")
	}

	if appState.ServerConfig.Config.EnableApiBasedModules {
		// Concatenate modules with default modules
		modules = append(modules, defaultModules...)
	}

	enabledModules := map[string]bool{}
	for _, module := range modules {
		enabledModules[strings.TrimSpace(module)] = true
	}

	if _, ok := enabledModules[modt2vbigram.Name]; ok {
		appState.Modules.Register(modt2vbigram.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modt2vbigram.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcontextionary.Name]; ok {
		appState.Modules.Register(modcontextionary.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcontextionary.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modt2vmodel2vec.Name]; ok {
		appState.Modules.Register(modt2vmodel2vec.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modt2vmodel2vec.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtransformers.Name]; ok {
		appState.Modules.Register(modtransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtransformers.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgpt4all.Name]; ok {
		appState.Modules.Register(modgpt4all.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgpt4all.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankervoyageai.Name]; ok {
		appState.Modules.Register(modrerankervoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankervoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankertransformers.Name]; ok {
		appState.Modules.Register(modrerankertransformers.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankertransformers.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankercohere.Name]; ok {
		appState.Modules.Register(modrerankercohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankercohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankercontextualai.Name]; ok {
		appState.Modules.Register(modrerankercontextualai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankercontextualai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankerdummy.Name]; ok {
		appState.Modules.Register(modrerankerdummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankerdummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankerjinaai.Name]; ok {
		appState.Modules.Register(modrerankerjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankerjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modrerankernvidia.Name]; ok {
		appState.Modules.Register(modrerankernvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modrerankernvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modqna.Name]; ok {
		appState.Modules.Register(modqna.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modqna.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modsum.Name]; ok {
		appState.Modules.Register(modsum.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modsum.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modimage.Name]; ok {
		appState.Modules.Register(modimage.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modimage.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modner.Name]; ok {
		appState.Modules.Register(modner.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modner.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modspellcheck.Name]; ok {
		appState.Modules.Register(modspellcheck.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modspellcheck.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modclip.Name]; ok {
		appState.Modules.Register(modclip.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modclip.Name).
			Debug("enabled module")
	}

	_, enabledMulti2VecGoogle := enabledModules[modmulti2vecgoogle.Name]
	_, enabledMulti2VecPaLM := enabledModules[modmulti2vecgoogle.LegacyName]
	if enabledMulti2VecGoogle || enabledMulti2VecPaLM {
		appState.Modules.Register(modmulti2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2veccohere.Name]; ok {
		appState.Modules.Register(modmulti2veccohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2veccohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecjinaai.Name]; ok {
		appState.Modules.Register(modmulti2vecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2multivecjinaai.Name]; ok {
		appState.Modules.Register(modmulti2multivecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2multivecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2multivecweaviate.Name]; ok {
		appState.Modules.Register(modmulti2multivecweaviate.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2multivecweaviate.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecnvidia.Name]; ok {
		appState.Modules.Register(modmulti2vecnvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecnvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modnvidia.Name]; ok {
		appState.Modules.Register(modnvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modnvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecvoyageai.Name]; ok {
		appState.Modules.Register(modmulti2vecvoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecvoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modopenai.Name]; ok {
		appState.Modules.Register(modopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmorph.Name]; ok {
		appState.Modules.Register(modmorph.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmorph.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[moddatabricks.Name]; ok {
		appState.Modules.Register(moddatabricks.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", moddatabricks.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modqnaopenai.Name]; ok {
		appState.Modules.Register(modqnaopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modqnaopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativecohere.Name]; ok {
		appState.Modules.Register(modgenerativecohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativecohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativecontextualai.Name]; ok {
		appState.Modules.Register(modgenerativecontextualai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativecontextualai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativefriendliai.Name]; ok {
		appState.Modules.Register(modgenerativefriendliai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativefriendliai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativenvidia.Name]; ok {
		appState.Modules.Register(modgenerativenvidia.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativenvidia.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativemistral.Name]; ok {
		appState.Modules.Register(modgenerativemistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativemistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeopenai.Name]; ok {
		appState.Modules.Register(modgenerativeopenai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeopenai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativexai.Name]; ok {
		appState.Modules.Register(modgenerativexai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativexai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedatabricks.Name]; ok {
		appState.Modules.Register(modgenerativedatabricks.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedatabricks.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeollama.Name]; ok {
		appState.Modules.Register(modgenerativeollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativedummy.Name]; ok {
		appState.Modules.Register(modgenerativedummy.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativedummy.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeaws.Name]; ok {
		appState.Modules.Register(modgenerativeaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modhuggingface.Name]; ok {
		appState.Modules.Register(modhuggingface.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modhuggingface.Name).
			Debug("enabled module")
	}

	_, enabledGenerativeGoogle := enabledModules[modgenerativegoogle.Name]
	_, enabledGenerativePaLM := enabledModules[modgenerativegoogle.LegacyName]
	if enabledGenerativeGoogle || enabledGenerativePaLM {
		appState.Modules.Register(modgenerativegoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativegoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanyscale.Name]; ok {
		appState.Modules.Register(modgenerativeanyscale.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanyscale.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeanthropic.Name]; ok {
		appState.Modules.Register(modgenerativeanthropic.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeanthropic.Name).
			Debug("enabled module")
	}

	_, enabledText2vecGoogle := enabledModules[modtext2vecgoogle.Name]
	_, enabledText2vecPaLM := enabledModules[modtext2vecgoogle.LegacyName]
	if enabledText2vecGoogle || enabledText2vecPaLM {
		appState.Modules.Register(modtext2vecgoogle.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecgoogle.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecaws.Name]; ok {
		appState.Modules.Register(modtext2vecaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmulti2vecaws.Name]; ok {
		appState.Modules.Register(modmulti2vecaws.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmulti2vecaws.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgfs.Name]; ok {
		appState.Modules.Register(modstgfs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgfs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgs3.Name]; ok {
		appState.Modules.Register(modstgs3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgs3.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modsloads3.Name]; ok {
		appState.Modules.Register(modsloads3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modsloads3.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstggcs.Name]; ok {
		appState.Modules.Register(modstggcs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstggcs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modstgazure.Name]; ok {
		appState.Modules.Register(modstgazure.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modstgazure.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcentroid.Name]; ok {
		appState.Modules.Register(modcentroid.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcentroid.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modcohere.Name]; ok {
		appState.Modules.Register(modcohere.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modcohere.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modvoyageai.Name]; ok {
		appState.Modules.Register(modvoyageai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modvoyageai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modmistral.Name]; ok {
		appState.Modules.Register(modmistral.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modmistral.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modbind.Name]; ok {
		appState.Modules.Register(modbind.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modbind.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modjinaai.Name]; ok {
		appState.Modules.Register(modjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modollama.Name]; ok {
		appState.Modules.Register(modollama.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modollama.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modweaviateembed.Name]; ok {
		appState.Modules.Register(modweaviateembed.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modweaviateembed.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modgenerativeoctoai.Name]; ok {
		appState.Modules.Register(modgenerativeoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modgenerativeoctoai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modtext2vecoctoai.Name]; ok {
		appState.Modules.Register(modtext2vecoctoai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2vecoctoai.Name).
			Debug("enabled module")
	}

	_, enabledText2MultivecJinaAI := enabledModules[modtext2multivecjinaai.Name]
	_, enabledText2ColBERTJinaAI := enabledModules[modtext2multivecjinaai.LegacyName]
	if enabledText2MultivecJinaAI || enabledText2ColBERTJinaAI {
		appState.Modules.Register(modtext2multivecjinaai.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modtext2multivecjinaai.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modusagegcs.Name]; ok {
		appState.Modules.Register(modusagegcs.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modusagegcs.Name).
			Debug("enabled module")
	}

	if _, ok := enabledModules[modusages3.Name]; ok {
		appState.Modules.Register(modusages3.New())
		appState.Logger.
			WithField("action", "startup").
			WithField("module", modusages3.Name).
			Debug("enabled module")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("completed registering modules")

	return nil
}

func postInitModules(appState *state.State) {
	// Initialize usage service after all components are ready
	if appState.Modules.UsageEnabled() {
		appState.Logger.WithField("action", "startup").Debug("initializing usage service")

		// Initialize usage service for GCS
		if usageGCSModule := appState.Modules.GetByName(modusagegcs.Name); usageGCSModule != nil {
			if usageModuleWithService, ok := usageGCSModule.(modulecapabilities.ModuleWithUsageService); ok {
				usageService := usage.NewService(appState.SchemaManager, appState.DB, appState.Modules, usageModuleWithService.Logger())
				usageModuleWithService.SetUsageService(usageService)
			}
		}
		// Initialize usage service for S3
		if usageS3Module := appState.Modules.GetByName(modusages3.Name); usageS3Module != nil {
			if usageModuleWithService, ok := usageS3Module.(modulecapabilities.ModuleWithUsageService); ok {
				usageService := usage.NewService(appState.SchemaManager, appState.DB, appState.Modules, usageModuleWithService.Logger())
				usageModuleWithService.SetUsageService(usageService)
			}
		}
	}
}

func initModules(ctx context.Context, appState *state.State) error {
	storageProvider, err := modulestorage.NewRepo(
		appState.ServerConfig.Config.Persistence.DataPath, appState.Logger)
	if err != nil {
		return errors.Wrap(err, "init storage provider")
	}

	// TODO: gh-1481 don't pass entire appState in, but only what's needed. Probably only
	// config?
	moduleParams := moduletools.NewInitParams(storageProvider, appState,
		&appState.ServerConfig.Config, appState.Logger, prometheus.DefaultRegisterer)

	appState.Logger.
		WithField("action", "startup").
		Debug("start initializing modules")
	if err := appState.Modules.Init(ctx, moduleParams, appState.Logger); err != nil {
		return errors.Wrap(err, "init modules")
	}

	appState.Logger.
		WithField("action", "startup").
		Debug("finished initializing modules")

	return nil
}

type clientWithAuth struct {
	r         http.RoundTripper
	basicAuth cluster.BasicAuth
}

func (c clientWithAuth) RoundTrip(r *http.Request) (*http.Response, error) {
	r.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	return c.r.RoundTrip(r)
}

func reasonableHttpClient(authConfig cluster.AuthConfig, minimumInternalTimeout time.Duration) *http.Client {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   minimumInternalTimeout,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Wrap with OpenTelemetry tracing (only has an effect if tracing is enabled)
	transport := monitoring.NewTracingTransport(t)

	if authConfig.BasicAuth.Enabled() {
		return &http.Client{Transport: clientWithAuth{r: transport, basicAuth: authConfig.BasicAuth}}
	}
	return &http.Client{Transport: transport}
}

func setupGoProfiling(config config.Config, logger logrus.FieldLogger) {
	if config.Profiling.Disabled {
		return
	}

	functionsToIgnoreInProfiling := []string{
		"raft",
		"http2",
		"memberlist",
		"selectgo", // various tickers
		"cluster",
		"rest",
		"signal_recv",
		"backgroundRead",
		"SetupGoProfiling",
		"serve",
		"Serve",
		"batchWorker",
	}
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler(functionsToIgnoreInProfiling...))
	enterrors.GoWrapper(func() {
		portNumber := config.Profiling.Port
		if portNumber == 0 {
			if err := http.ListenAndServe(":6060", nil); err != nil {
				logger.Error("error listinening and serve :6060 : %w", err)
			}
		} else {
			http.ListenAndServe(fmt.Sprintf(":%d", portNumber), nil)
		}
	}, logger)

	if config.Profiling.BlockProfileRate > 0 {
		goruntime.SetBlockProfileRate(config.Profiling.BlockProfileRate)
	}

	if config.Profiling.MutexProfileFraction > 0 {
		goruntime.SetMutexProfileFraction(config.Profiling.MutexProfileFraction)
	}
}

func ParseVersionFromSwaggerSpec() string {
	spec := struct {
		Info struct {
			Version string `json:"version"`
		} `json:"info"`
	}{}

	err := json.Unmarshal(SwaggerJSON, &spec)
	if err != nil {
		panic(err)
	}

	return spec.Info.Version
}

func limitResources(appState *state.State) {
	if os.Getenv("LIMIT_RESOURCES") == "true" {
		appState.Logger.Info("Limiting resources:  memory: 80%, cores: all but one")
		if os.Getenv("GOMAXPROCS") == "" {
			// Fetch the number of cores from the cgroups cpuset
			// and parse it into an int
			cores, err := getCores()
			if err == nil {
				appState.Logger.WithField("cores", cores).
					Warn("GOMAXPROCS not set, and unable to read from cgroups, setting to number of cores")
				goruntime.GOMAXPROCS(cores)
			} else {
				cores = goruntime.NumCPU() - 1
				if cores > 0 {
					appState.Logger.WithField("cores", cores).
						Warnf("Unable to read from cgroups: %v, setting to max cores to: %v", err, cores)
					goruntime.GOMAXPROCS(cores)
				}
			}
		}

		limit, err := memlimit.SetGoMemLimit(0.8)
		if err != nil {
			appState.Logger.WithError(err).Warnf("Unable to set memory limit from cgroups: %v", err)
			// Set memory limit to 90% of the available memory
			limit := int64(float64(memory.TotalMemory()) * 0.8)
			debug.SetMemoryLimit(limit)
			appState.Logger.WithField("limit", limit).Info("Set memory limit based on available memory")
		} else {
			appState.Logger.WithField("limit", limit).Info("Set memory limit")
		}
	} else {
		appState.Logger.Info("No resource limits set, weaviate will use all available memory and CPU. " +
			"To limit resources, set LIMIT_RESOURCES=true")
	}
}

func telemetryEnabled(state *state.State) bool {
	return !state.ServerConfig.Config.DisableTelemetry
}

// getTelemetryURL returns the telemetry consumer URL from config.
// If a custom URL is set, it's base64-encoded to match the expected format.
// Returns empty string if no custom URL is set (telemetry.New will use default).
func getTelemetryURL(state *state.State) string {
	url := state.ServerConfig.Config.TelemetryURL
	if url == "" {
		return ""
	}
	// The telemetry package expects base64-encoded URLs
	return base64.StdEncoding.EncodeToString([]byte(url))
}

type membership struct {
	*cluster.State
	raft *rCluster.Service
}

func (m membership) LeaderID() string {
	_, id := m.raft.LeaderWithID()
	return id
}

// initRuntimeOverrides assumes, Configs from envs are loaded before
// initializing runtime overrides.
func initRuntimeOverrides(appState *state.State) *configRuntime.ConfigManager[config.WeaviateRuntimeConfig] {
	// Enable runtime config manager
	if appState.ServerConfig.Config.RuntimeOverrides.Enabled {
		// Runtimeconfig manager takes of keeping the `registered` config values upto date
		registered := &config.WeaviateRuntimeConfig{}
		registered.MaximumAllowedCollectionsCount = appState.ServerConfig.Config.SchemaHandlerConfig.MaximumAllowedCollectionsCount
		registered.AsyncReplicationDisabled = appState.ServerConfig.Config.Replication.AsyncReplicationDisabled
		registered.AsyncReplicationClusterMaxWorkers = appState.ServerConfig.Config.Replication.AsyncReplicationClusterMaxWorkers
		registered.AutoschemaEnabled = appState.ServerConfig.Config.AutoSchema.Enabled
		registered.ReplicaMovementMinimumAsyncWait = appState.ServerConfig.Config.ReplicaMovementMinimumAsyncWait
		registered.TenantActivityReadLogLevel = appState.ServerConfig.Config.TenantActivityReadLogLevel
		registered.TenantActivityWriteLogLevel = appState.ServerConfig.Config.TenantActivityWriteLogLevel
		registered.RevectorizeCheckDisabled = appState.ServerConfig.Config.RevectorizeCheckDisabled
		registered.QuerySlowLogEnabled = appState.ServerConfig.Config.QuerySlowLogEnabled
		registered.QuerySlowLogThreshold = appState.ServerConfig.Config.QuerySlowLogThreshold
		registered.InvertedSorterDisabled = appState.ServerConfig.Config.InvertedSorterDisabled
		registered.DefaultQuantization = appState.ServerConfig.Config.DefaultQuantization
		registered.ReplicatedIndicesRequestQueueEnabled = appState.ServerConfig.Config.Cluster.RequestQueueConfig.IsEnabled
		registered.RaftDrainSleep = appState.ServerConfig.Config.Raft.DrainSleep
		registered.RaftTimoutsMultiplier = appState.ServerConfig.Config.Raft.TimeoutsMultiplier
		registered.ReplicatedIndicesRequestQueueEnabled = appState.ServerConfig.Config.Cluster.RequestQueueConfig.IsEnabled
		registered.OperationalMode = appState.ServerConfig.Config.OperationalMode
		registered.ObjectsTTLDeleteSchedule = appState.ServerConfig.Config.ObjectsTTLDeleteSchedule
		registered.ObjectsTTLBatchSize = appState.ServerConfig.Config.ObjectsTTLBatchSize
		registered.ObjectsTTLPauseEveryNoBatches = appState.ServerConfig.Config.ObjectsTTLPauseEveryNoBatches
		registered.ObjectsTTLPauseDuration = appState.ServerConfig.Config.ObjectsTTLPauseDuration
		registered.ObjectsTTLConcurrencyFactor = appState.ServerConfig.Config.ObjectsTTLConcurrencyFactor

		if appState.ServerConfig.Config.Authentication.OIDC.Enabled {
			registered.OIDCIssuer = appState.ServerConfig.Config.Authentication.OIDC.Issuer
			registered.OIDCClientID = appState.ServerConfig.Config.Authentication.OIDC.ClientID
			registered.OIDCSkipClientIDCheck = appState.ServerConfig.Config.Authentication.OIDC.SkipClientIDCheck
			registered.OIDCUsernameClaim = appState.ServerConfig.Config.Authentication.OIDC.UsernameClaim
			registered.OIDCGroupsClaim = appState.ServerConfig.Config.Authentication.OIDC.GroupsClaim
			registered.OIDCScopes = appState.ServerConfig.Config.Authentication.OIDC.Scopes
			registered.OIDCCertificate = appState.ServerConfig.Config.Authentication.OIDC.Certificate
			registered.OIDCJWKSUrl = appState.ServerConfig.Config.Authentication.OIDC.JWKSUrl
		}

		cm, err := configRuntime.NewConfigManager(
			appState.ServerConfig.Config.RuntimeOverrides.Path,
			config.ParseRuntimeConfig,
			config.UpdateRuntimeConfig,
			registered,
			appState.ServerConfig.Config.RuntimeOverrides.LoadInterval,
			appState.Logger,
			prometheus.DefaultRegisterer)
		if err != nil {
			appState.Logger.WithField("action", "startup").Errorf("could not create runtime config manager: %v", err)
		}
		return cm
	}
	return nil
}

// postInitRuntimeOverrides registers hooks and starts runtime config background process
func postInitRuntimeOverrides(appState *state.State, cm *configRuntime.ConfigManager[config.WeaviateRuntimeConfig]) {
	if appState.ServerConfig.Config.RuntimeOverrides.Enabled && cm != nil {
		// register any additional runtime configs
		if appState.Modules.UsageEnabled() {
			cm.RegisterAdditional(func(registered *config.WeaviateRuntimeConfig) {
				// gcs config
				registered.UsageGCSBucket = appState.ServerConfig.Config.Usage.GCSBucket
				registered.UsageGCSPrefix = appState.ServerConfig.Config.Usage.GCSPrefix
				// s3 config
				registered.UsageS3Bucket = appState.ServerConfig.Config.Usage.S3Bucket
				registered.UsageS3Prefix = appState.ServerConfig.Config.Usage.S3Prefix
				// common config
				registered.UsageScrapeInterval = appState.ServerConfig.Config.Usage.ScrapeInterval
				registered.UsageShardJitterInterval = appState.ServerConfig.Config.Usage.ShardJitterInterval
				registered.UsagePolicyVersion = appState.ServerConfig.Config.Usage.PolicyVersion
				registered.UsageVerifyPermissions = appState.ServerConfig.Config.Usage.VerifyPermissions
			})
		}
		// register hooks
		hooks := make(map[string]func() error)
		if appState.ServerConfig.Config.Authentication.OIDC.Enabled {
			hooks["OIDC"] = appState.OIDC.Init
		}
		maps.Copy(hooks, appState.Crons.RuntimeConfigHooks())

		appState.Logger.Log(logrus.InfoLevel, "registereing OIDC runtime overrides hooks")
		cm.RegisterHooks(hooks)
		// reload current overrides file to take into account additional settings
		if err := cm.ReloadConfig(); err != nil {
			appState.Logger.WithField("action", "startup").Errorf("could not reload config: %v", err)
		}
		// start runtime config background check
		enterrors.GoWrapper(func() {
			// NOTE: Not using parent `ctx` because that is getting cancelled in the caller even during startup.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := cm.Run(ctx); err != nil {
				appState.Logger.WithField("action", "runtime config manager startup").Fatalf("runtime config manager stopped: %v", err)
			}
		}, appState.Logger)
	}
}
