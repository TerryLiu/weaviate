//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	golangSort "sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/usecases/dynsemaphore"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/loadlimiter"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/cluster/router/executor"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/entities/tokenizer"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/multitenancy"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// 使用runtime.GOMAXPROCS而非runtime.NumCPU，因为NumCPU返回的是物理CPU核心数。
// 然而在容器化环境中，这可能不是我们想要的。物理节点可能有128个核心，但我们可能
// 被cgroup限制为2个核心。在这种情况下，我们希望限制为2而不是128。
// MAXPROCS不一定能反映cgroup限制，但至少有机会被正确设置。
// 如果没有，则默认为NumCPU，所以我们不会变得更糟。
var _NUMCPU = runtime.GOMAXPROCS(0)

// shardMap是一个专门用于存储分片的sync.Map
type shardMap sync.Map

// Range按顺序为map中存在的每个键值对调用f函数。
// 如果f返回错误，则停止迭代
func (m *shardMap) Range(f func(name string, shard ShardLike) error) (err error) {
	(*sync.Map)(m).Range(func(key, value any) bool {
		// 对键进行安全的类型断言
		name, ok := key.(string)
		if !ok {
			// 跳过无效的键
			return true
		}

		// Safe type assertion for value
		shard, ok := value.(ShardLike)
		if !ok || shard == nil {
			// Skip invalid or nil shards
			return true
		}

		err = f(name, shard)
		return err == nil
	})
	return err
}

// RangeConcurrently并发地为map中存在的每个键值对调用f函数，最多使用_NUMCPU个执行器并行运行。
// 与[Range]不同，它不保证在第一个错误时退出。
func (m *shardMap) RangeConcurrently(logger logrus.FieldLogger, f func(name string, shard ShardLike) error) (err error) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU)
	(*sync.Map)(m).Range(func(key, value any) bool {
		name, ok := key.(string)
		if !ok {
			// 跳过无效的键
			return true
		}

		// Safe type assertion for value
		shard, ok := value.(ShardLike)
		if !ok || shard == nil {
			// Skip invalid or nil shards
			return true
		}

		eg.Go(func() error {
			return f(name, shard)
		}, name, shard)
		return true
	})

	return eg.Wait()
}

// Load返回分片，如果不存在则返回nil。
// 注意：此方法不检查分片是否已加载，可能返回未加载的懒加载分片，
// 如果使用返回的分片可能会导致其被加载。
// 如果想在不加载的情况下检查分片是否已加载，请使用Loaded。
func (m *shardMap) Load(name string) ShardLike {
	v, ok := (*sync.Map)(m).Load(name)
	if !ok {
		return nil
	}

	shard, ok := v.(ShardLike)
	if !ok {
		return nil
	}
	return shard
}

// Loaded返回分片，如果不存在则返回nil。
// 如果是懒加载分片，只有在已加载时才返回。
func (m *shardMap) Loaded(name string) ShardLike {
	v, ok := (*sync.Map)(m).Load(name)
	if !ok {
		return nil
	}

	shard, ok := v.(ShardLike)
	if !ok {
		return nil
	}

	// 如果是懒加载分片，只有在已加载时才返回
	if lazyShard, ok := shard.(*LazyLoadShard); ok {
		if !lazyShard.isLoaded() {
			return nil
		}
	}

	return shard
}

// Store设置分片，给出其名称和值
func (m *shardMap) Store(name string, shard ShardLike) {
	(*sync.Map)(m).Store(name, shard)
}

// Swap交换键的分片并返回之前的值（如果有）。
// loaded结果报告键是否存在。
func (m *shardMap) Swap(name string, shard ShardLike) (previous ShardLike, loaded bool) {
	v, ok := (*sync.Map)(m).Swap(name, shard)
	if v == nil || !ok {
		return nil, ok
	}
	return v.(ShardLike), ok
}

// CompareAndSwap比较并交换分片，如果map中存储的值等于old，则将其交换为new
// name: 分片名称
// old: 旧的分片值
// new: 新的分片值
// 返回值: 如果交换成功返回true，否则返回false
func (m *shardMap) CompareAndSwap(name string, old, new ShardLike) bool {
	return (*sync.Map)(m).CompareAndSwap(name, old, new)
}

// LoadAndDelete删除键对应的分片值，如果存在则返回之前的值
// name: 分片名称
// 返回值: 之前的分片值和是否存在的标志
func (m *shardMap) LoadAndDelete(name string) (ShardLike, bool) {
	v, ok := (*sync.Map)(m).LoadAndDelete(name)
	if v == nil || !ok {
		return nil, ok
	}
	return v.(ShardLike), ok
}

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
// Index表示一个索引，是包含特定类的所有数据的逻辑单元。
// 一个索引可以进一步分解为独立的单元（称为分片），以便在节点间轻松分布
type Index struct {
	classSearcher           inverted.ClassSearcher // 允许嵌套的按引用搜索
	shards                  shardMap               // 分片映射
	Config                  IndexConfig            // 索引配置
	globalreplicationConfig *replication.GlobalConfig // 全局复制配置

	getSchema    schemaUC.SchemaGetter     // Schema获取器
	schemaReader schemaUC.SchemaReader    // Schema读取器
	logger       logrus.FieldLogger        // 日志记录器
	remote       *sharding.RemoteIndex     // 远程索引客户端
	stopwords    *stopwords.Detector       // 停用词检测器
	replicator   *replica.Replicator       // 复制器

	vectorIndexUserConfigLock sync.Mutex                            // 向量索引用户配置锁
	vectorIndexUserConfig     schemaConfig.VectorIndexConfig        // 向量索引用户配置
	vectorIndexUserConfigs    map[string]schemaConfig.VectorIndexConfig // 多向量索引用户配置
	HFreshEnabled             bool                                    // HNSW刷新启用标志

	partitioningEnabled  bool // 分区启用标志
	AsyncIndexingEnabled bool // 异步索引启用标志

	invertedIndexConfig     schema.InvertedIndexConfig // 倒排索引配置
	invertedIndexConfigLock sync.Mutex                 // 倒排索引配置锁

	// 此锁应与db indexLock一起使用。
	//
	// db indexlock锁定包含所有索引的map以防止更改，应在迭代时使用。
	// 此锁保护此特定索引在使用时不被删除。使用Rlock表示正在使用。
	// 这样多个goroutine可以并行使用特定索引。删除例程将尝试获取RWlock。
	//
	// 使用方法：
	// 使用db.indexLock锁定整个db
	// 选择你想要的索引并对它们加Rlock
	// 解锁db.indexLock
	// 使用这些索引
	// 对所有选中的索引执行RUnlock
	dropIndex sync.RWMutex

	// 索引中的其他锁应始终按给定顺序调用以防止死锁：
	// 1. closeLock
	// 2. backupLock（针对特定分片）
	// 3. shardCreateLocks（针对特定分片）
	closeLock  sync.RWMutex       // 在执行操作时防止关闭
	backupLock *esync.KeyRWLocker // 在备份运行时防止写入
	// 防止并发的分片状态更改。使用.Rlock来防止状态更改，使用.Lock来更改状态
	// 尽量减少持有RW锁的时间，因为它会阻塞同一分片上的其他操作如搜索或写入。
	shardCreateLocks *esync.KeyRWLocker

	metrics          *Metrics                    // 指标收集器
	centralJobQueue  chan job                    // 中央作业队列
	scheduler        *queue.Scheduler            // 调度器
	indexCheckpoints *indexcheckpoint.Checkpoints // 索引检查点

	cycleCallbacks *indexCycleCallbacks // 索引周期回调

	lastBackup atomic.Pointer[BackupState] // 最后一次备份状态

	// 在Shutdown或Drop被调用时取消
	closingCtx    context.Context      // 关闭上下文
	closingCancel context.CancelFunc   // 关闭取消函数

	// 如果懒加载分片关闭则始终为true，在懒加载分片的情况下
	// 当最后一个分片加载完成后设置为true。
	allShardsReady atomic.Bool          // 所有分片就绪标志
	allocChecker   memwatch.AllocChecker // 内存分配检查器

	replicationConfigLock          sync.RWMutex                // 复制配置锁
	asyncReplicationWorkersLimiter *dynsemaphore.DynamicWeighted // 异步复制工作者限流器

	shardLoadLimiter  *loadlimiter.LoadLimiter  // 分片加载限流器
	bucketLoadLimiter *loadlimiter.LoadLimiter  // 存储桶加载限流器

	closed bool // 关闭标志

	shardReindexer ShardReindexerV3 // 分片重新索引器

	router        routerTypes.Router        // 路由器
	shardResolver *resolver.ShardResolver   // 分片解析器
	bitmapBufPool roaringset.BitmapBufPool  // 位图缓冲池
}

// ID返回索引的唯一标识符
func (i *Index) ID() string {
	return indexID(i.Config.ClassName)
}

// path返回索引的文件系统路径
func (i *Index) path() string {
	return path.Join(i.Config.RootPath, i.ID())
}

// NewIndex创建一个具有指定分片数量的索引，仅使用本地节点的分片
func NewIndex(
	ctx context.Context,
	cfg IndexConfig,
	invertedIndexConfig schema.InvertedIndexConfig,
	vectorIndexUserConfig schemaConfig.VectorIndexConfig,
	vectorIndexUserConfigs map[string]schemaConfig.VectorIndexConfig,
	router routerTypes.Router,
	shardResolver *resolver.ShardResolver,
	sg schemaUC.SchemaGetter,
	schemaReader schemaUC.SchemaReader,
	cs inverted.ClassSearcher,
	logger logrus.FieldLogger,
	nodeResolver cluster.NodeResolver,
	remoteClient sharding.RemoteIndexClient,
	replicaClient replica.Client,
	globalReplicationConfig *replication.GlobalConfig,
	promMetrics *monitoring.PrometheusMetrics,
	class *models.Class,
	jobQueueCh chan job,
	scheduler *queue.Scheduler,
	indexCheckpoints *indexcheckpoint.Checkpoints,
	allocChecker memwatch.AllocChecker,
	shardReindexer ShardReindexerV3,
	bitmapBufPool roaringset.BitmapBufPool,
	asyncIndexingEnabled bool,
) (*Index, error) {
	// 添加自定义词典
	err := tokenizer.AddCustomDict(class.Class, invertedIndexConfig.TokenizerUserDict)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

	// 创建停用词检测器
	sd, err := stopwords.NewDetectorFromConfig(invertedIndexConfig.Stopwords)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

	// 设置嵌套引用查询限制的默认值
	if cfg.QueryNestedRefLimit == 0 {
		cfg.QueryNestedRefLimit = config.DefaultQueryNestedCrossReferenceLimit
	}

	// 初始化向量索引用户配置映射
	if vectorIndexUserConfigs == nil {
		vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{}
	}

	// 创建指标收集器
	metrics, err := NewMetrics(logger, promMetrics, cfg.ClassName.String(), "n/a")
	if err != nil {
		return nil, fmt.Errorf("create metrics for index %q: %w", cfg.ClassName.String(), err)
	}

	index := &Index{
		Config:                  cfg,
		globalreplicationConfig: globalReplicationConfig,
		getSchema:               sg,
		schemaReader:            schemaReader,
		logger:                  logger,
		classSearcher:           cs,
		vectorIndexUserConfig:   vectorIndexUserConfig,
		invertedIndexConfig:     invertedIndexConfig,
		vectorIndexUserConfigs:  vectorIndexUserConfigs,
		stopwords:               sd,
		partitioningEnabled:     multitenancy.IsMultiTenant(class.MultiTenancyConfig),
		AsyncIndexingEnabled:    asyncIndexingEnabled,
		remote:                  sharding.NewRemoteIndex(cfg.ClassName.String(), sg, nodeResolver, remoteClient),
		metrics:                 metrics,
		centralJobQueue:         jobQueueCh,
		backupLock:              esync.NewKeyRWLocker(),
		scheduler:               scheduler,
		indexCheckpoints:        indexCheckpoints,
		allocChecker:            allocChecker,
		shardCreateLocks:        esync.NewKeyRWLocker(),
		shardLoadLimiter:        cfg.ShardLoadLimiter,
		bucketLoadLimiter:       cfg.BucketLoadLimiter,
		shardReindexer:          shardReindexer,
		router:                  router,
		shardResolver:           shardResolver,
		bitmapBufPool:           bitmapBufPool,
		HFreshEnabled:           cfg.HFreshEnabled,
	}

	// 获取删除策略的闭包函数
	getDeletionStrategy := func() string {
		return index.DeletionStrategy()
	}

	// TODO: 修复副本路由器实例化，应该在顶层进行
	// 创建复制器
	index.replicator, err = replica.NewReplicator(cfg.ClassName.String(), router, nodeResolver, sg.NodeName(), getDeletionStrategy, replicaClient, promMetrics, logger)
	if err != nil {
		return nil, fmt.Errorf("create replicator for index %q: %w", index.ID(), err)
	}

	index.asyncReplicationWorkersLimiter = dynsemaphore.NewDynamicWeightedWithParent(index.Config.AsyncReplicationWorkersLimiter,
		func() int64 {
			index.replicationConfigLock.RLock()
			defer index.replicationConfigLock.RUnlock()
			return int64(index.Config.AsyncReplicationConfig.maxWorkers)
		})

	index.closingCtx, index.closingCancel = context.WithCancel(context.Background())

	index.initCycleCallbacks()

	if err := index.checkSingleShardMigration(); err != nil {
		return nil, errors.Wrap(err, "migrating sharding state from previous version")
	}

	if err := os.MkdirAll(index.path(), os.ModePerm); err != nil {
		return nil, fmt.Errorf("init index %q: %w", index.ID(), err)
	}

	if err := index.initAndStoreShards(ctx, class, promMetrics); err != nil {
		return nil, err
	}

	index.cycleCallbacks.compactionCycle.Start()
	index.cycleCallbacks.compactionAuxCycle.Start()
	index.cycleCallbacks.flushCycle.Start()

	return index, nil
}

// since called in Index's constructor there is no risk same shard will be inited/created in parallel,
// therefore shardCreateLocks are not used here
// initAndStoreShards初始化并存储本地分片
// ctx: 上下文
// class: 类模型
// promMetrics: Prometheus指标
// 返回值: 可能的错误
func (i *Index) initAndStoreShards(ctx context.Context, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics,
) error {
	// 分片信息结构体
	type shardInfo struct {
		name           string  // 分片名称
		activityStatus string  // 活动状态
	}

	var localShards []shardInfo  // 本地分片列表
	className := i.Config.ClassName.String()  // 类名

	// 读取分片状态并收集本地分片信息
	err := i.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, physical := range state.Physical {
			if state.IsLocalShard(shardName) {
				localShards = append(localShards, shardInfo{
					name:           shardName,
					activityStatus: physical.ActivityStatus(),
				})
			}
		}

		return nil
	})
	// 检查分片状态读取是否成功
	if err != nil {
		return fmt.Errorf("failed to read sharding state: %w", err)
	}

	// 如果禁用了懒加载分片，则立即加载所有活跃的本地分片
	if i.Config.DisableLazyLoadShards {
		eg := enterrors.NewErrorGroupWrapper(i.logger)  // 创建错误组
		eg.SetLimit(_NUMCPU)  // 设置并发限制

		// 遍历本地分片，只加载活跃状态的分片
		for _, shard := range localShards {
			if shard.activityStatus != models.TenantActivityStatusHOT {
				continue  // 跳过非活跃分片
			}

			shardName := shard.name
			eg.Go(func() error {
				// 获取分片加载许可
				if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
					return fmt.Errorf("acquiring permit to load shard: %w", err)
				}
				defer i.shardLoadLimiter.Release()  // 确保释放许可

				// 创建新的分片实例
				newShard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler,
					i.indexCheckpoints, i.shardReindexer, false, i.bitmapBufPool)
				if err != nil {
					return fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
				}

				// 存储分片到索引中
				i.shards.Store(shardName, newShard)
				return nil
			}, shardName)
		}

		// 等待所有分片初始化完成
		if err := eg.Wait(); err != nil {
			return err
		}

		i.allShardsReady.Store(true)
		return nil
	}

	activeShardNames := make([]string, 0, len(localShards))

	for _, shard := range localShards {
		if shard.activityStatus != models.TenantActivityStatusHOT {
			continue
		}

		activeShardNames = append(activeShardNames, shard.name)

		lazyShard := NewLazyLoadShard(ctx, promMetrics, shard.name, i, class, i.centralJobQueue, i.indexCheckpoints,
			i.allocChecker, i.shardLoadLimiter, i.shardReindexer, true, i.bitmapBufPool)
		i.shards.Store(shard.name, lazyShard)
	}

	// NOTE(dyma):
	// 1. So "lazy-loaded" shards are actually loaded "half-eagerly"?
	// 2. If <-ctx.Done or we fail to load a shard, should allShardsReady still report true?
	initLazyShardsInBackground := func() {
		defer i.allShardsReady.Store(true)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		now := time.Now()

		for _, shardName := range activeShardNames {
			select {
			case <-i.closingCtx.Done():
				i.logger.
					WithField("action", "load_all_shards").
					Errorf("failed to load all shards: %v", i.closingCtx.Err())
				return
			case <-ticker.C:
				select {
				case <-i.closingCtx.Done():
					i.logger.
						WithField("action", "load_all_shards").
						Errorf("failed to load all shards: %v", i.closingCtx.Err())
					return
				default:
					err := i.loadLocalShardIfActive(shardName)
					if err != nil {
						i.logger.
							WithField("action", "load_shard").
							WithField("shard_name", shardName).
							Errorf("failed to load shard: %v", err)
						return
					}
				}
			}
		}

		i.logger.
			WithField("action", "load_all_shards").
			WithField("took", time.Since(now).String()).
			Debug("finished loading all shards")
	}

	enterrors.GoWrapper(initLazyShardsInBackground, i.logger)

	return nil
}

func (i *Index) loadLocalShardIfActive(shardName string) error {
	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	// check if set to inactive in the meantime by concurrent call
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil
	}

	lazyShard, ok := shard.(*LazyLoadShard)
	if ok {
		return lazyShard.Load(context.Background())
	}

	return nil
}

// used to init/create shard in different moments of index's lifecycle, therefore it needs to be called
// within shardCreateLocks to prevent parallel create/init of the same shard
func (i *Index) initShard(ctx context.Context, shardName string, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics, disableLazyLoad bool, implicitShardLoading bool,
) (ShardLike, error) {
	if disableLazyLoad {
		if err := i.allocChecker.CheckMappingAndReserve(3, int(lsmkv.FlushAfterDirtyDefault.Seconds())); err != nil {
			return nil, errors.Wrap(err, "memory pressure: cannot init shard")
		}

		if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("acquiring permit to load shard: %w", err)
		}
		defer i.shardLoadLimiter.Release()

		shard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler,
			i.indexCheckpoints, i.shardReindexer, false, i.bitmapBufPool)
		if err != nil {
			return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
		}

		return shard, nil
	}

	shard := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.indexCheckpoints,
		i.allocChecker, i.shardLoadLimiter, i.shardReindexer, implicitShardLoading, i.bitmapBufPool)
	return shard, nil
}

func (i *Index) maintenanceModeEnabled() bool {
	return i.Config.MaintenanceModeEnabled != nil && i.Config.MaintenanceModeEnabled()
}

// Iterate over all objects in the index, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateObjects(ctx context.Context, cb func(index *Index, shard ShardLike, object *storobj.Object) error) (err error) {
	return i.ForEachShard(func(_ string, shard ShardLike) error {
		wrapper := func(object *storobj.Object) error {
			return cb(i, shard, object)
		}
		bucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
		return bucket.IterateObjects(ctx, wrapper)
	})
}

// ForEachShard applies func f on each shard in the index.
//
// WARNING: only use this if you expect all LazyLoadShards to be loaded!
// Calling this method may lead to shards being force-loaded, causing
// unexpected CPU spikes. If you only want to apply f on loaded shards,
// call ForEachLoadedShard instead.
// Note: except Dropping and Shutting Down
func (i *Index) ForEachShard(f func(name string, shard ShardLike) error) error {
	// Check if the index is being dropped or shut down to avoid panics when the index is being deleted
	if i.closingCtx.Err() != nil {
		i.logger.WithField("action", "for_each_shard").Debug("index is being dropped or shut down")
		return nil
	}

	return i.shards.Range(f)
}

func (i *Index) ForEachLoadedShard(f func(name string, shard ShardLike) error) error {
	return i.shards.Range(func(name string, shard ShardLike) error {
		// Skip lazy loaded shard which are not loaded
		if asLazyLoadShard, ok := shard.(*LazyLoadShard); ok {
			if !asLazyLoadShard.isLoaded() {
				return nil
			}
		}
		return f(name, shard)
	})
}

func (i *Index) ForEachShardConcurrently(f func(name string, shard ShardLike) error) error {
	// Check if the index is being dropped or shut down to avoid panics when the index is being deleted
	if i.closingCtx.Err() != nil {
		i.logger.WithField("action", "for_each_shard_concurrently").Debug("index is being dropped or shut down")
		return nil
	}
	return i.shards.RangeConcurrently(i.logger, f)
}

func (i *Index) ForEachLoadedShardConcurrently(f func(name string, shard ShardLike) error) error {
	return i.shards.RangeConcurrently(i.logger, func(name string, shard ShardLike) error {
		// Skip lazy loaded shard which are not loaded
		if asLazyLoadShard, ok := shard.(*LazyLoadShard); ok {
			if !asLazyLoadShard.isLoaded() {
				return nil
			}
		}
		return f(name, shard)
	})
}

// Iterate over all objects in the shard, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateShards(ctx context.Context, cb func(index *Index, shard ShardLike) error) (err error) {
	return i.ForEachShard(func(key string, shard ShardLike) error {
		return cb(i, shard)
	})
}

func (i *Index) addProperty(ctx context.Context, props ...*models.Property) error {
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)

	i.ForEachShard(func(key string, shard ShardLike) error {
		shard.initPropertyBuckets(ctx, eg, false, props...)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "extend idx '%s' with properties '%v", i.ID(), props)
	}
	return nil
}

func (i *Index) updateVectorIndexConfig(ctx context.Context,
	updated schemaConfig.VectorIndexConfig,
) error {
	// an updated is not specific to one shard, but rather all
	err := i.ForEachShard(func(name string, shard ShardLike) error {
		// At the moment, we don't do anything in an update that could fail, but
		// technically this should be part of some sort of a two-phase commit  or
		// have another way to rollback if we have updates that could potentially
		// fail in the future. For now that's not a realistic risk.
		if err := shard.UpdateVectorIndexConfig(ctx, updated); err != nil {
			return errors.Wrapf(err, "shard %s", name)
		}
		return nil
	})
	if err != nil {
		return err
	}
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	i.vectorIndexUserConfig = updated

	return nil
}

func (i *Index) updateVectorIndexConfigs(ctx context.Context,
	updated map[string]schemaConfig.VectorIndexConfig,
) error {
	err := i.ForEachShard(func(name string, shard ShardLike) error {
		if err := shard.UpdateVectorIndexConfigs(ctx, updated); err != nil {
			return fmt.Errorf("shard %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	for targetName, targetCfg := range updated {
		i.vectorIndexUserConfigs[targetName] = targetCfg
	}

	return nil
}

func (i *Index) GetInvertedIndexConfig() schema.InvertedIndexConfig {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	return i.invertedIndexConfig
}

func (i *Index) updateInvertedIndexConfig(ctx context.Context,
	updated schema.InvertedIndexConfig,
) error {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	i.invertedIndexConfig = updated

	err := i.stopwords.ReplaceDetectorFromConfig(updated.Stopwords)
	if err != nil {
		return fmt.Errorf("update inverted index config: %w", err)
	}

	err = tokenizer.AddCustomDict(i.Config.ClassName.String(), updated.TokenizerUserDict)
	if err != nil {
		return errors.Wrap(err, "updating inverted index config")
	}

	return nil
}

func (i *Index) asyncReplicationGloballyDisabled() bool {
	return i.globalreplicationConfig.AsyncReplicationDisabled.Get()
}

func (i *Index) updateReplicationConfig(ctx context.Context, cfg *models.ReplicationConfig) error {
	i.replicationConfigLock.Lock()
	defer i.replicationConfigLock.Unlock()

	i.Config.ReplicationFactor = cfg.Factor
	i.Config.DeletionStrategy = cfg.DeletionStrategy
	i.Config.AsyncReplicationEnabled = cfg.AsyncEnabled

	config, err := asyncReplicationConfigFromModel(multitenancy.IsMultiTenant(i.getClass().MultiTenancyConfig), cfg.AsyncConfig)
	if err != nil {
		return err
	}
	i.Config.AsyncReplicationConfig = config

	// unloaded shards will fetch the latest config when they are loaded
	err = i.ForEachLoadedShard(func(name string, shard ShardLike) error {
		if i.Config.AsyncReplicationEnabled && cfg.AsyncConfig != nil {
			// if async replication is being enabled, first disable it to reset any previous config
			if err := shard.SetAsyncReplicationState(ctx, AsyncReplicationConfig{}, false); err != nil {
				return fmt.Errorf("updating async replication on shard %q: %w", name, err)
			}
		}

		if err := shard.SetAsyncReplicationState(ctx, i.Config.AsyncReplicationConfig, i.asyncReplicationEnabled()); err != nil {
			return fmt.Errorf("updating async replication on shard %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (i *Index) ReplicationFactor() int64 {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor
}

func (i *Index) DeletionStrategy() string {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.DeletionStrategy
}

type IndexConfig struct {
	RootPath                            string
	ClassName                           schema.ClassName
	QueryMaximumResults                 int64
	QueryHybridMaximumResults           int64
	QueryNestedRefLimit                 int64
	ResourceUsage                       config.ResourceUsage
	LazySegmentsDisabled                bool
	SegmentInfoIntoFileNameEnabled      bool
	WriteMetadataFilesEnabled           bool
	MemtablesFlushDirtyAfter            int
	MemtablesInitialSizeMB              int
	MemtablesMaxSizeMB                  int
	MemtablesMinActiveSeconds           int
	MemtablesMaxActiveSeconds           int
	MinMMapSize                         int64
	MaxReuseWalSize                     int64
	SegmentsCleanupIntervalSeconds      int
	SeparateObjectsCompactions          bool
	CycleManagerRoutinesFactor          int
	IndexRangeableInMemory              bool
	MaxSegmentSize                      int64
	ReplicationFactor                   int64
	DeletionStrategy                    string
	AsyncReplicationEnabled             bool
	AsyncReplicationConfig              AsyncReplicationConfig
	AsyncReplicationWorkersLimiter      *dynsemaphore.DynamicWeighted
	AvoidMMap                           bool
	DisableLazyLoadShards               bool
	ForceFullReplicasSearch             bool
	TransferInactivityTimeout           time.Duration
	LSMEnableSegmentsChecksumValidation bool
	TrackVectorDimensions               bool
	TrackVectorDimensionsInterval       time.Duration
	UsageEnabled                        bool
	ShardLoadLimiter                    *loadlimiter.LoadLimiter
	BucketLoadLimiter                   *loadlimiter.LoadLimiter
	ObjectsTTLBatchSize                 *configRuntime.DynamicValue[int]
	ObjectsTTLPauseEveryNoBatches       *configRuntime.DynamicValue[int]
	ObjectsTTLPauseDuration             *configRuntime.DynamicValue[time.Duration]

	HNSWMaxLogSize                               int64
	HNSWDisableSnapshots                         bool
	HNSWSnapshotIntervalSeconds                  int
	HNSWSnapshotOnStartup                        bool
	HNSWSnapshotMinDeltaCommitlogsNumber         int
	HNSWSnapshotMinDeltaCommitlogsSizePercentage int
	HNSWWaitForCachePrefill                      bool
	HNSWFlatSearchConcurrency                    int
	HNSWAcornFilterRatio                         float64
	HNSWGeoIndexEF                               int
	VisitedListPoolMaxSize                       int

	QuerySlowLogEnabled    *configRuntime.DynamicValue[bool]
	QuerySlowLogThreshold  *configRuntime.DynamicValue[time.Duration]
	InvertedSorterDisabled *configRuntime.DynamicValue[bool]
	MaintenanceModeEnabled func() bool

	HFreshEnabled bool
}

// indexID生成索引的唯一标识符
// class: 类名
// 返回值: 小写的类名作为索引ID
func indexID(class schema.ClassName) string {
	return strings.ToLower(string(class))
}

// putObject向索引中插入单个对象
// ctx: 上下文
// object: 待插入的对象
// replProps: 复制属性配置
// tenantName: 租户名称
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) putObject(ctx context.Context, object *storobj.Object,
	replProps *additional.ReplicationProperties, tenantName string, schemaVersion uint64,
) error {
	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	targetShard, err := i.shardResolver.ResolveShard(ctx, object)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}
	if replProps == nil {
		replProps = defaultConsistency()
	}
	if i.shardHasMultipleReplicasWrite(tenantName, targetShard.Shard) {
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.PutObject(ctx, targetShard.Shard, object, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate insertion: shard=%q: %w", targetShard.Shard, err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, object.Object.Tenant, targetShard.Shard, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.PutObject(ctx, targetShard.Shard, object, schemaVersion); err != nil {
			return fmt.Errorf("put remote object: shard=%q: %w", targetShard.Shard, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(targetShard.Shard)
	defer i.backupLock.RUnlock(targetShard.Shard)

	err = shard.PutObject(ctx, object)
	if err != nil {
		return fmt.Errorf("put local object: shard=%q: %w", targetShard.Shard, err)
	}

	return nil
}

// IncomingPutObject处理来自其他节点的对象插入请求
// ctx: 上下文
// shardName: 分片名称
// object: 待插入的对象
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) IncomingPutObject(ctx context.Context, shardName string,
	object *storobj.Object, schemaVersion uint64,
) error {
	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	if err := i.parseDateFieldsInProps(object.Object.Properties); err != nil {
		return err
	}

	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.PutObject(ctx, object)
}

// replicationEnabled检查是否启用了复制功能
// 返回值: 如果复制因子大于1则返回true
func (i *Index) replicationEnabled() bool {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor > 1
}

// shardHasMultipleReplicasWrite检查分片是否有多个副本用于写入
// tenantName: 租户名称
// shardName: 分片名称
// 返回值: 如果有多个写副本则返回true
func (i *Index) shardHasMultipleReplicasWrite(tenantName, shardName string) bool {
	// if replication is enabled, we always have multiple replicas
	if i.replicationEnabled() {
		return true
	}
	// if the router is nil, preserve previous behavior by returning false
	if i.router == nil {
		return false
	}
	ws, err := i.router.GetWriteReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
	if err != nil {
		return false
	}
	// we're including additional replicas here to make sure we at least try to push the write
	// to them if they exist
	allReplicas := append(ws.NodeNames(), ws.AdditionalNodeNames()...)
	return len(allReplicas) > 1
}

// shardHasMultipleReplicasRead检查分片是否有多个副本用于读取
// tenantName: 租户名称
// shardName: 分片名称
// 返回值: 如果有多个读副本则返回true
func (i *Index) shardHasMultipleReplicasRead(tenantName, shardName string) bool {
	// if replication is enabled, we always have multiple replicas
	if i.replicationEnabled() {
		return true
	}
	// if the router is nil, preserve previous behavior by returning false
	if i.router == nil {
		return false
	}
	replicas, err := i.router.GetReadReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
	if err != nil {
		return false
	}
	return len(replicas.NodeNames()) > 1
}

// anyShardHasMultipleReplicasRead返回是否任何分片有多个读副本
// tenantName: 租户名称
// shardNames: 分片名称数组
// 返回值: 如果任何分片有多个读副本则返回true
func (i *Index) anyShardHasMultipleReplicasRead(tenantName string, shardNames []string) bool {
	if i.replicationEnabled() {
		return true
	}
	for _, shardName := range shardNames {
		if i.shardHasMultipleReplicasRead(tenantName, shardName) {
			return true
		}
	}
	return false
}

type localShardOperation string

const (
	localShardOperationWrite localShardOperation = "write"
	localShardOperationRead  localShardOperation = "read"
)

// getShardForDirectLocalOperation尝试获取用于本地读写操作的分片
// 它将返回找到的分片以及用于释放分片的函数。
// 如果未找到分片或不应使用本地分片，则分片为nil。
// 调用方应始终调用release函数。
// ctx: 上下文
// tenantName: 租户名称
// shardName: 分片名称
// operation: 操作类型（读或写）
// 返回值: 分片对象、释放函数和可能的错误
func (i *Index) getShardForDirectLocalOperation(ctx context.Context, tenantName string, shardName string, operation localShardOperation) (ShardLike, func(), error) {
	shard, release, err := i.GetShard(ctx, shardName)
	// NOTE release should always be ok to call, even if there is an error or the shard is nil,
	// see Index.getOptInitLocalShard for more details.
	if err != nil {
		return nil, release, err
	}

	// if the router is nil, just use the default behavior
	if i.router == nil {
		return shard, release, nil
	}

	// get the replicas for the shard
	var rs routerTypes.ReadReplicaSet
	var ws routerTypes.WriteReplicaSet
	switch operation {
	case localShardOperationWrite:
		ws, err = i.router.GetWriteReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
		if err != nil {
			return shard, release, nil
		}
		// if the local node is not in the list of replicas, don't return the shard (but still allow the caller to release)
		// we should not read/write from the local shard if the local node is not in the list of replicas (eg we should use the remote)
		if !slices.Contains(ws.NodeNames(), i.replicator.LocalNodeName()) {
			return nil, release, nil
		}
	case localShardOperationRead:
		rs, err = i.router.GetReadReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
		if err != nil {
			return shard, release, nil
		}
		// if the local node is not in the list of replicas, don't return the shard (but still allow the caller to release)
		// we should not read/write from the local shard if the local node is not in the list of replicas (eg we should use the remote)
		if !slices.Contains(rs.NodeNames(), i.replicator.LocalNodeName()) {
			return nil, release, nil
		}
	default:
		return nil, func() {}, fmt.Errorf("invalid local shard operation: %s", operation)
	}

	return shard, release, nil
}

// AsyncReplicationEnabled返回异步复制是否已启用
// 返回值: 如果异步复制已启用则返回true
func (i *Index) AsyncReplicationEnabled() bool {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.asyncReplicationEnabled()
}

// asyncReplicationEnabled检查异步复制是否启用的内部方法
// 返回值: 如果复制因子大于1且异步复制启用且未全局禁用则返回true
func (i *Index) asyncReplicationEnabled() bool {
	return i.Config.ReplicationFactor > 1 && i.Config.AsyncReplicationEnabled && !i.asyncReplicationGloballyDisabled()
}

// AsyncReplicationConfig返回异步复制配置
// 返回值: 异步复制配置对象
func (i *Index) AsyncReplicationConfig() AsyncReplicationConfig {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.AsyncReplicationConfig
}

// asyncReplicationWorkerAcquire获取异步复制工作者资源
// ctx: 上下文
// 返回值: 可能的错误
func (i *Index) asyncReplicationWorkerAcquire(ctx context.Context) error {
	return i.asyncReplicationWorkersLimiter.Acquire(ctx, 1)
}

// asyncReplicationWorkerRelease释放异步复制工作者资源
func (i *Index) asyncReplicationWorkerRelease() {
	i.asyncReplicationWorkersLimiter.Release(1)
}

// parseDateFieldsInProps检查当前类的schema以确定哪些字段是日期字段，
// 然后解析这些字段（如果已设置）。支持date和date[]两种类型。
// props: 属性对象
// 返回值: 可能的错误
func (i *Index) parseDateFieldsInProps(props interface{}) error {
	if props == nil {
		return nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		// don't know what to do with this
		return nil
	}

	c := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())
	if c == nil {
		return fmt.Errorf("class %s not found in schema", i.Config.ClassName)
	}

	for _, prop := range c.Properties {
		if prop.DataType[0] == string(schema.DataTypeDate) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			parsed, err := parseAsStringToTime(raw)
			if err != nil {
				return errors.Wrapf(err, "time prop %q", prop.Name)
			}

			propMap[prop.Name] = parsed
		}

		if prop.DataType[0] == string(schema.DataTypeDateArray) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			asSlice, ok := raw.([]string)
			if !ok {
				return errors.Errorf("parse as time array, expected []interface{} got %T",
					raw)
			}
			parsedSlice := make([]interface{}, len(asSlice))
			for j := range asSlice {
				parsed, err := parseAsStringToTime(interface{}(asSlice[j]))
				if err != nil {
					return errors.Wrapf(err, "time array prop %q at pos %d", prop.Name, j)
				}

				parsedSlice[j] = parsed
			}
			propMap[prop.Name] = parsedSlice

		}
	}

	return nil
}

// parseAsStringToTime将字符串解析为时间对象
// in: 输入接口
// 返回值: 解析后的时间对象和可能的错误
func parseAsStringToTime(in interface{}) (time.Time, error) {
	var parsed time.Time
	var err error

	asString, ok := in.(string)
	if !ok {
		return parsed, errors.Errorf("parse as time, expected string got %T", in)
	}

	parsed, err = time.Parse(time.RFC3339, asString)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

// return value []error gives the error for the index with the positions
// matching the inputs
// putObjectBatch批量插入对象到索引中，支持分片路由和复制机制
// ctx: 上下文对象
// objects: 待插入的对象数组
// replProps: 复制属性配置
// schemaVersion: 模式版本号
// 返回值: 每个对象对应的错误数组，与输入对象数组一一对应
func (i *Index) putObjectBatch(ctx context.Context, objects []*storobj.Object,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	// objsAndPos结构体用于存储分片中的对象及其原始位置
	type objsAndPos struct {
		objects []*storobj.Object  // 对象数组
		pos     []int              // 对应的原始位置索引
	}
	out := make([]error, len(objects))  // 初始化错误数组，与对象数组长度相同
	// 如果复制属性为空，则使用默认一致性级别
	if replProps == nil {
		replProps = defaultConsistency()
	}

	// 按分片对对象进行分组
	byShard := map[string]objsAndPos{}
	for pos, obj := range objects {
		// 解析对象应该存储的分片
		target, err := i.shardResolver.ResolveShard(ctx, obj)
		if err != nil {
			out[pos] = err  // 记录解析错误
			continue
		}
		// 将对象添加到对应分片的组中
		group := byShard[target.Shard]
		group.objects = append(group.objects, obj)
		group.pos = append(group.pos, pos)
		byShard[target.Shard] = group
	}

	// 使用等待组并发处理各个分片
	wg := &sync.WaitGroup{}
	for shardName, group := range byShard {
		shardName := shardName  // 避免闭包变量捕获问题
		group := group          // 避免闭包变量捕获问题
		wg.Add(1)  // 增加等待组计数
		// 定义处理函数
		f := func() {
			defer wg.Done()  // 确保等待组计数减少

			// 恐慌恢复机制
			defer func() {
				err := recover()
				if err != nil {
					// 将恐慌错误传播到所有相关位置
					for pos := range group.pos {
						out[pos] = fmt.Errorf("an unexpected error occurred: %s", err)
					}
					fmt.Fprintf(os.Stderr, "panic: %s\n", err)
					entsentry.Recover(err)  // Sentry错误追踪
					debug.PrintStack()      // 打印堆栈跟踪
				}
			}()
			// 同一分片组中的所有对象都属于同一个租户，因为在多租户系统中
			// 属于同一租户的所有对象都会存储在同一分片中。
			// 对于非多租户集合，所有对象的Object.Tenant都为空。
			// 因此，我们可以安全地使用组中任意对象的租户信息。
			tenantName := group.objects[0].Object.Tenant
			var errs []error  // 存储操作错误
			// 判断是否需要多副本写入
			if i.shardHasMultipleReplicasWrite(tenantName, shardName) {
				// 多副本写入：通过复制器进行分布式写入
				errs = i.replicator.PutObjects(ctx, shardName, group.objects,
					routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				// 单副本写入：直接在本地或远程分片上操作
				shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
				defer release()  // 确保资源释放
				if err != nil {
					errs = []error{err}  // 返回获取分片错误
				} else if shard != nil {
					// 本地分片操作
					func() {
						// 获取备份读锁防止在备份过程中写入
						i.backupLock.RLock(shardName)
						defer i.backupLock.RUnlock(shardName)
						defer release()  // 确保释放
						errs = shard.PutObjectBatch(ctx, group.objects)  // 在分片上批量插入对象
					}()
				} else {
					// 远程分片操作
					errs = i.remote.BatchPutObjects(ctx, shardName, group.objects, schemaVersion)
				}
			}

			// 将错误映射回原始对象数组的对应位置
			for i, err := range errs {
				desiredPos := group.pos[i]
				out[desiredPos] = err
			}
		}
		enterrors.GoWrapper(f, i.logger)
	}

	wg.Wait()  // 等待所有分片操作完成

	return out  // 返回错误数组
}

// return value []error gives the error for the index with the positions
// matching the inputs
// duplicateErr复制错误到指定数量的错误数组
// in: 输入错误
// count: 数组长度
// 返回值: 错误数组
func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}

	return out
}

// IncomingBatchPutObjects处理来自其他节点的批量对象插入请求
// ctx: 上下文
// shardName: 分片名称
// objects: 对象数组
// schemaVersion: 模式版本号
// 返回值: 每个对象对应的错误数组
func (i *Index) IncomingBatchPutObjects(ctx context.Context, shardName string,
	objects []*storobj.Object, schemaVersion uint64,
) []error {
	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	for j := range objects {
		if err := i.parseDateFieldsInProps(objects[j].Object.Properties); err != nil {
			return duplicateErr(err, len(objects))
		}
	}

	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return duplicateErr(err, len(objects))
	}
	defer release()

	return shard.PutObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
// AddReferencesBatch批量添加引用到索引中
// ctx: 上下文
// refs: 批量引用数组
// replProps: 复制属性配置
// schemaVersion: 模式版本号
// 返回值: 每个引用对应的错误数组
func (i *Index) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	type refsAndPos struct {
		refs objects.BatchReferences
		pos  []int
	}
	if replProps == nil {
		replProps = defaultConsistency()
	}

	byShard := map[string]refsAndPos{}
	out := make([]error, len(refs))

	for pos, ref := range refs {
		shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, ref.From.TargetID, ref.Tenant)
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.refs = append(group.refs, ref)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	for shardName, group := range byShard {
		// All references in the same shard group have the same tenant since in multi-tenant
		// systems all objects belonging to a tenant end up in the same shard.
		// For non-multi-tenant collections, ref.Tenant is empty for all references.
		// Therefore, we can safely use the tenant from any reference in the group.
		tenantName := group.refs[0].Tenant
		var errs []error
		if i.shardHasMultipleReplicasWrite(tenantName, shardName) {
			errs = i.replicator.AddReferences(ctx, shardName, group.refs, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
		} else {
			shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
			if err != nil {
				errs = duplicateErr(err, len(group.refs))
			} else if shard != nil {
				func() {
					i.backupLock.RLock(shardName)
					defer i.backupLock.RUnlock(shardName)
					defer release()
					errs = shard.AddReferencesBatch(ctx, group.refs)
				}()
			} else {
				errs = i.remote.BatchAddReferences(ctx, shardName, group.refs, schemaVersion)
			}
		}

		for i, err := range errs {
			desiredPos := group.pos[i]
			out[desiredPos] = err
		}
	}

	return out
}

// IncomingBatchAddReferences处理来自其他节点的批量添加引用请求
// ctx: 上下文
// shardName: 分片名称
// refs: 批量引用数组
// schemaVersion: 模式版本号
// 返回值: 每个引用对应的错误数组
func (i *Index) IncomingBatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences, schemaVersion uint64,
) []error {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return duplicateErr(err, len(refs))
	}
	defer release()

	return shard.AddReferencesBatch(ctx, refs)
}

// objectByID根据ID获取单个对象
// ctx: 上下文
// id: 对象UUID
// props: 要选择的属性
// addl: 附加属性
// replProps: 复制属性配置
// tenant: 租户名称
// 返回值: 对象指针和可能的错误
func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	replProps *additional.ReplicationProperties, tenant string,
) (*storobj.Object, error) {
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return nil, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return nil, fmt.Errorf("determine shard: %w", err)
		default:
			return nil, objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	var obj *storobj.Object

	if i.shardHasMultipleReplicasRead(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		if replProps.NodeName != "" {
			obj, err = i.replicator.NodeObject(ctx, replProps.NodeName, shardName, id, props, addl)
		} else {
			obj, err = i.replicator.GetOne(ctx, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), shardName, id, props, addl)
		}
		return obj, err
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
	defer release()
	if err != nil {
		return obj, err
	}
	defer release()

	if shard != nil {
		if obj, err = shard.ObjectByID(ctx, id, props, addl); err != nil {
			return obj, fmt.Errorf("get local object: shard=%s: %w", shardName, err)
		}
	} else {
		if obj, err = i.remote.GetObject(ctx, shardName, id, props, addl); err != nil {
			return obj, fmt.Errorf("get remote object: shard=%s: %w", shardName, err)
		}
	}

	return obj, nil
}

// IncomingGetObject处理来自其他节点的获取对象请求
// ctx: 上下文
// shardName: 分片名称
// id: 对象UUID
// props: 要选择的属性
// additional: 附加属性
// 返回值: 对象指针和可能的错误
func (i *Index) IncomingGetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.ObjectByID(ctx, id, props, additional)
}

// IncomingMultiGetObjects处理来自其他节点的批量获取对象请求
// ctx: 上下文
// shardName: 分片名称
// ids: 对象UUID数组
// 返回值: 对象数组和可能的错误
func (i *Index) IncomingMultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.MultiObjectByID(ctx, wrapIDsInMulti(ids))
}

// multiObjectByID批量获取多个对象
// ctx: 上下文
// query: 标识符数组
// tenant: 租户名称
// 返回值: 对象数组和可能的错误
func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier, tenant string,
) ([]*storobj.Object, error) {
	type idsAndPos struct {
		ids []multi.Identifier
		pos []int
	}

	byShard := map[string]idsAndPos{}
	for pos, id := range query {
		shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, strfmt.UUID(id.ID), tenant)
		if err != nil {
			switch {
			case errors.As(err, &objects.ErrMultiTenancy{}):
				return nil, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
			case errors.As(err, &authzerrors.Forbidden{}):
				return nil, fmt.Errorf("determine shard: %w", err)
			default:
				return nil, objects.NewErrInvalidUserInput("determine shard: %v", err)
			}
		}

		group := byShard[shardName]
		group.ids = append(group.ids, id)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	out := make([]*storobj.Object, len(query))

	for shardName, group := range byShard {
		var objects []*storobj.Object
		var err error

		shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
		if err != nil {
			return nil, err
		} else if shard != nil {
			func() {
				defer release()
				objects, err = shard.MultiObjectByID(ctx, group.ids)
				if err != nil {
					err = errors.Wrapf(err, "local shard %s", shardId(i.ID(), shardName))
				}
			}()
		} else {
			objects, err = i.remote.MultiGetObjects(ctx, shardName, extractIDsFromMulti(group.ids))
			if err != nil {
				return nil, errors.Wrapf(err, "remote shard %s", shardName)
			}
		}

		for i, obj := range objects {
			desiredPos := group.pos[i]
			out[desiredPos] = obj
		}
	}

	return out, nil
}

// extractIDsFromMulti从多标识符数组中提取UUID数组
// in: 多标识符数组
// 返回值: UUID数组
func extractIDsFromMulti(in []multi.Identifier) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))

	for i, id := range in {
		out[i] = strfmt.UUID(id.ID)
	}

	return out
}

// wrapIDsInMulti将UUID数组包装为多标识符数组
// in: UUID数组
// 返回值: 多标识符数组
func wrapIDsInMulti(in []strfmt.UUID) []multi.Identifier {
	out := make([]multi.Identifier, len(in))

	for i, id := range in {
		out[i] = multi.Identifier{ID: string(id)}
	}

	return out
}

// exists检查对象是否存在
// ctx: 上下文
// id: 对象UUID
// replProps: 复制属性配置
// tenant: 租户名称
// 返回值: 是否存在和可能的错误
func (i *Index) exists(ctx context.Context, id strfmt.UUID,
	replProps *additional.ReplicationProperties, tenant string,
) (bool, error) {
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return false, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return false, fmt.Errorf("determine shard: %w", err)
		default:
			return false, objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	var exists bool
	if i.shardHasMultipleReplicasRead(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		return i.replicator.Exists(ctx, cl, shardName, id)
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
	defer release()
	if err != nil {
		return exists, err
	}

	if shard != nil {
		exists, err = shard.Exists(ctx, id)
		if err != nil {
			err = fmt.Errorf("exists locally: shard=%q: %w", shardName, err)
		}
	} else {
		exists, err = i.remote.Exists(ctx, shardName, id)
		if err != nil {
			owner, _ := i.getSchema.ShardOwner(i.Config.ClassName.String(), shardName)
			err = fmt.Errorf("exists remotely: shard=%q owner=%q: %w", shardName, owner, err)
		}
	}

	return exists, err
}

// IncomingExists处理来自其他节点的对象存在性检查请求
// ctx: 上下文
// shardName: 分片名称
// id: 对象UUID
// 返回值: 是否存在和可能的错误
func (i *Index) IncomingExists(ctx context.Context, shardName string,
	id strfmt.UUID,
) (bool, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return false, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return false, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.Exists(ctx, id)
}

// objectSearch执行对象搜索，支持过滤、关键字排名、排序等多种查询条件
// ctx: 上下文
// limit: 返回结果的最大数量
// filters: 本地过滤条件
// keywordRanking: 关键字排名参数
// sort: 排序条件
// cursor: 游标参数
// addlProps: 附加属性
// replProps: 复制属性
// tenant: 租户标识
// autoCut: 自动截断参数
// properties: 要返回的属性列表
// 返回值: 对象列表、分数列表和可能的错误
func (i *Index) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, replProps *additional.ReplicationProperties, tenant string, autoCut int,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	cl := i.consistencyLevel(replProps, routerTypes.ConsistencyLevelOne)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, nil, err
	}

	// 如果请求是BM25F且未选择属性，则使用所有可能的属性
	if keywordRanking != nil && keywordRanking.Type == "bm25" && len(keywordRanking.Properties) == 0 {

		cl := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())
		if cl == nil {
			return nil, nil, fmt.Errorf("class %s not found in schema", i.Config.ClassName)
		}

		propHash := cl.Properties
		// 获取哈希的键
		for _, v := range propHash {
			if inverted.PropertyHasSearchableIndex(i.getSchema.ReadOnlyClass(i.Config.ClassName.String()), v.Name) {
				keywordRanking.Properties = append(keywordRanking.Properties, v.Name)
			}
		}

		// WEAVIATE-471 - 如果找不到可搜索的属性则报错
		if len(keywordRanking.Properties) == 0 {
			return nil, []float32{}, errors.New(
				"No properties provided, and no indexed properties found in class")
		}
	}

	outObjects, outScores, err := i.objectSearchByShard(ctx, limit, filters, keywordRanking, sort, cursor, addlProps, tenant, readPlan, properties)
	if err != nil {
		return nil, nil, err
	}

	if len(outObjects) == len(outScores) {
		if keywordRanking != nil && keywordRanking.Type == "bm25" {
			for ii := range outObjects {
				oo := outObjects[ii]

				if oo.AdditionalProperties() == nil {
					oo.Object.Additional = make(map[string]interface{})
				}

				// Additional score is filled in by the top level function

				// Collect all keys starting with "BM25F" and add them to the Additional
				if keywordRanking.AdditionalExplanations {
					explainScore := ""
					for k, v := range oo.Object.Additional {
						if strings.HasPrefix(k, "BM25F") {

							explainScore = fmt.Sprintf("%v, %v:%v", explainScore, k, v)
							delete(oo.Object.Additional, k)
						}
					}
					oo.Object.Additional["explainScore"] = explainScore
				}
			}
		}
	}

	if len(sort) > 0 {
		if len(readPlan.Shards()) > 1 {
			var err error
			outObjects, outScores, err = i.sort(outObjects, outScores, sort, limit)
			if err != nil {
				return nil, nil, errors.Wrap(err, "sort")
			}
		}
	} else if keywordRanking != nil {
		outObjects, outScores = i.sortKeywordRanking(outObjects, outScores)
	} else if len(readPlan.Shards()) > 1 && !addlProps.ReferenceQuery {
		// sort only for multiple shards (already sorted for single)
		// and for not reference nested query (sort is applied for root query)
		outObjects, outScores = i.sortByID(outObjects, outScores)
	}

	if autoCut > 0 {
		cutOff := autocut.Autocut(outScores, autoCut)
		outObjects = outObjects[:cutOff]
		outScores = outScores[:cutOff]
	}

	// if this search was caused by a reference property
	// search, we should not limit the number of results.
	// for example, if the query contains a where filter
	// whose operator is `And`, and one of the operands
	// contains a path to a reference prop, the Search
	// caused by such a ref prop being limited can cause
	// the `And` to return no results where results would
	// be expected. we won't know that unless we search
	// and return all referenced object properties.
	if !addlProps.ReferenceQuery && len(outObjects) > limit {
		if len(outObjects) == len(outScores) {
			outScores = outScores[:limit]
		}
		outObjects = outObjects[:limit]
	}

	if i.anyShardHasMultipleReplicasRead(tenant, readPlan.Shards()) {
		err = i.replicator.CheckConsistency(ctx, cl, outObjects)
		if err != nil {
			i.logger.WithField("action", "object_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return outObjects, outScores, nil
}

// objectSearchByShard按分片执行对象搜索
// ctx: 上下文
// limit: 结果数量限制
// filters: 过滤条件
// keywordRanking: 关键字排名
// sort: 排序条件
// cursor: 游标
// addlProps: 附加属性
// tenant: 租户名称
// readPlan: 读取路由计划
// properties: 属性列表
// 返回值: 对象数组、分数数组和可能的错误
func (i *Index) objectSearchByShard(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, tenant string, readPlan routerTypes.ReadRoutingPlan, properties []string,
) ([]*storobj.Object, []float32, error) {
	resultObjects, resultScores := objectSearchPreallocate(limit, readPlan.Shards())

	eg := enterrors.NewErrorGroupWrapper(i.logger, "filters:", filters)
	// When running in fractional CPU environments, _NUMCPU will be 1
	// Most cloud deployments of Weaviate are in HA clusters with rf=3
	// Therefore, we should set the maximum amount of concurrency to at least 3
	// so that single-tenant rf=3 queries are not serialized. For any higher value of
	// _NUMCPU, e.g. 8, the extra goroutine will not be a significant overhead (16 -> 17)
	eg.SetLimit(_NUMCPU*2 + 1)
	shardResultLock := sync.Mutex{}

	remoteSearch := func(shardName string) error {
		objs, scores, nodeName, err := i.remote.SearchShard(ctx, shardName, nil, nil, 0, limit, filters, keywordRanking, sort, cursor, nil, addlProps, nil, properties)
		if err != nil {
			return fmt.Errorf(
				"remote shard object search %s: %w", shardName, err)
		}

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			storobj.AddOwnership(objs, nodeName, shardName)
		}

		shardResultLock.Lock()
		resultObjects = append(resultObjects, objs...)
		resultScores = append(resultScores, scores...)
		shardResultLock.Unlock()

		return nil
	}
	localSeach := func(shardName string) error {
		// We need to getOrInit here because the shard might not yet be loaded due to eventual consistency on the schema update
		// triggering the shard loading in the database
		shard, release, err := i.getOrInitShard(ctx, shardName)
		defer release()
		if err != nil {
			return fmt.Errorf("error getting local shard %s: %w", shardName, err)
		}
		if shard == nil {
			// This will make the code hit other remote replicas, and usually resolve any kind of eventual consistency issues just thanks to delaying
			// the search to the other replica.
			// This is not ideal, but it works for now.
			return remoteSearch(shardName)
		}

		localCtx := helpers.InitSlowQueryDetails(ctx)
		helpers.AnnotateSlowQueryLog(localCtx, "is_coordinator", true)
		objs, scores, err := shard.ObjectSearch(localCtx, limit, filters, keywordRanking, sort, cursor, addlProps, properties)
		if err != nil {
			return fmt.Errorf(
				"local shard object search %s: %w", shard.ID(), err)
		}
		nodeName := i.getSchema.NodeName()

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			storobj.AddOwnership(objs, nodeName, shardName)
		}

		shardResultLock.Lock()
		resultObjects = append(resultObjects, objs...)
		resultScores = append(resultScores, scores...)
		shardResultLock.Unlock()

		return nil
	}
	err := executor.ExecuteForEachShard(readPlan,
		// Local Shard Search
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				return localSeach(shardName)
			}, shardName)
			return nil
		},
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				return remoteSearch(shardName)
			}, shardName)
			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing search for each shard: %w", err)
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	if len(resultObjects) == len(resultScores) {
		// Force a stable sort order by UUID
		type resultSortable struct {
			object *storobj.Object
			score  float32
		}
		objs := resultObjects
		scores := resultScores
		results := make([]resultSortable, len(objs))
		for i := range objs {
			results[i] = resultSortable{
				object: objs[i],
				score:  scores[i],
			}
		}

		golangSort.Slice(results, func(i, j int) bool {
			if results[i].score == results[j].score {
				return results[i].object.Object.ID < results[j].object.Object.ID
			}

			return results[i].score > results[j].score
		})

		finalObjs := make([]*storobj.Object, len(results))
		finalScores := make([]float32, len(results))
		for i, result := range results {
			finalObjs[i] = result.object
			finalScores[i] = result.score
		}

		return finalObjs, finalScores, nil
	}

	return resultObjects, resultScores, nil
}

// sortByID按对象ID排序
// objects: 对象数组
// scores: 分数数组
// 返回值: 排序后的对象数组和分数数组
func (i *Index) sortByID(objects []*storobj.Object, scores []float32,
) ([]*storobj.Object, []float32) {
	return newIDSorter().sort(objects, scores)
}

// sortKeywordRanking按关键字排名分数排序
// objects: 对象数组
// scores: 分数数组
// 返回值: 排序后的对象数组和分数数组
func (i *Index) sortKeywordRanking(objects []*storobj.Object,
	scores []float32,
) ([]*storobj.Object, []float32) {
	return newScoresSorter().sort(objects, scores)
}

// sort对对象进行排序
// objects: 对象数组
// scores: 分数数组
// sort: 排序条件
// limit: 结果数量限制
// 返回值: 排序后的对象数组、分数数组和可能的错误
func (i *Index) sort(objects []*storobj.Object, scores []float32,
	sort []filters.Sort, limit int,
) ([]*storobj.Object, []float32, error) {
	return sorter.NewObjectsSorter(i.getSchema.ReadOnlyClass).
		Sort(objects, scores, limit, sort)
}

// mergeGroups合并分组结果
// objects: 对象数组
// dists: 距离数组
// groupBy: 分组参数
// limit: 结果数量限制
// shardCount: 分片数量
// 返回值: 合并后的对象数组、距离数组和可能的错误
func (i *Index) mergeGroups(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy, limit, shardCount int,
) ([]*storobj.Object, []float32, error) {
	return newGroupMerger(objects, dists, groupBy).Do()
}

// singleLocalShardObjectVectorSearch在单个本地分片上执行向量搜索
// ctx: 上下文
// searchVectors: 搜索向量数组
// targetVectors: 目标向量名称数组
// dist: 距离阈值
// limit: 结果数量限制
// filters: 过滤条件
// sort: 排序条件
// groupBy: 分组参数
// additional: 附加属性
// shard: 分片对象
// targetCombination: 目标组合策略
// properties: 属性列表
// 返回值: 对象数组、距离数组和可能的错误
func (i *Index) singleLocalShardObjectVectorSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	shard ShardLike, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	ctx = helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(ctx, "is_coordinator", true)
	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shard.Name()))
	}
	res, resDists, err := shard.ObjectVectorSearch(
		ctx, searchVectors, targetVectors, dist, limit, filters, sort, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}
	return res, resDists, nil
}

// localShardSearch在本地分片上执行搜索
// ctx: 上下文
// searchVectors: 搜索向量数组
// targetVectors: 目标向量名称数组
// dist: 距离阈值
// limit: 结果数量限制
// localFilters: 本地过滤条件
// sort: 排序条件
// groupBy: 分组参数
// additionalProps: 附加属性
// targetCombination: 目标组合策略
// properties: 属性列表
// tenantName: 租户名称
// shardName: 分片名称
// 返回值: 对象数组、距离数组和可能的错误
func (i *Index) localShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additionalProps additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, tenantName string, shardName string,
) ([]*storobj.Object, []float32, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	if shard != nil {
		defer release()
	}

	localCtx := helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(localCtx, "is_coordinator", true)
	localShardResult, localShardScores, err := shard.ObjectVectorSearch(
		localCtx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}
	// Append result to out
	if i.shardHasMultipleReplicasRead(tenantName, shardName) {
		storobj.AddOwnership(localShardResult, i.getSchema.NodeName(), shardName)
	}
	return localShardResult, localShardScores, nil
}

// remoteShardSearch在远程分片上执行搜索
// ctx: 上下文
// searchVectors: 搜索向量数组
// targetVectors: 目标向量名称数组
// distance: 距离阈值
// limit: 结果数量限制
// localFilters: 本地过滤条件
// sort: 排序条件
// groupBy: 分组参数
// additional: 附加属性
// targetCombination: 目标组合策略
// properties: 属性列表
// tenantName: 租户名称
// shardName: 分片名称
// 返回值: 对象数组、距离数组和可能的错误
func (i *Index) remoteShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, distance float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, tenantName string, shardName string,
) ([]*storobj.Object, []float32, error) {
	var outObjects []*storobj.Object
	var outScores []float32

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	if shard != nil {
		defer release()
	}

	if i.Config.ForceFullReplicasSearch {
		// Force a search on all the replicas for the shard
		remoteSearchResults, err := i.remote.SearchAllReplicas(ctx,
			i.logger, shardName, searchVectors, targetVectors, distance, limit, localFilters,
			nil, sort, nil, groupBy, additional, i.getSchema.NodeName(), targetCombination, properties)
		// Only return an error if we failed to query remote shards AND we had no local shard to query
		if err != nil && shard == nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}
		// Append the result of the search to the outgoing result
		for _, remoteShardResult := range remoteSearchResults {
			if i.shardHasMultipleReplicasRead(tenantName, shardName) {
				storobj.AddOwnership(remoteShardResult.Objects, remoteShardResult.Node, shardName)
			}
			outObjects = append(outObjects, remoteShardResult.Objects...)
			outScores = append(outScores, remoteShardResult.Scores...)
		}
	} else {
		// Search only what is necessary
		remoteResult, remoteDists, nodeName, err := i.remote.SearchShard(ctx,
			shardName, searchVectors, targetVectors, distance, limit, localFilters,
			nil, sort, nil, groupBy, additional, targetCombination, properties)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}

		if i.shardHasMultipleReplicasRead(tenantName, shardName) {
			storobj.AddOwnership(remoteResult, nodeName, shardName)
		}
		outObjects = remoteResult
		outScores = remoteDists
	}
	return outObjects, outScores, nil
}

// objectVectorSearch执行向量搜索，支持多种向量搜索参数和过滤条件
// ctx: 上下文
// searchVectors: 搜索向量数组
// targetVectors: 目标向量名称数组
// dist: 距离阈值
// limit: 返回结果的最大数量
// localFilters: 本地过滤条件
// sort: 排序条件
// groupBy: 分组参数
// additionalProps: 附加属性
// replProps: 复制属性
// tenant: 租户标识
// targetCombination: 目标组合策略
// properties: 要返回的属性列表
// 返回值: 对象列表、距离分数列表和可能的错误
func (i *Index) objectVectorSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, localFilters *filters.LocalFilter, sort []filters.Sort,
	groupBy *searchparams.GroupBy, additionalProps additional.Properties,
	replProps *additional.ReplicationProperties, tenant string, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	cl := i.consistencyLevel(replProps, routerTypes.ConsistencyLevelOne)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, nil, err
	}

	// 如果只有一个分片且不需要强制全副本搜索，则直接在本地分片上搜索
	if len(readPlan.Shards()) == 1 && !i.Config.ForceFullReplicasSearch {
		shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, readPlan.Shards()[0], localShardOperationRead)
		defer release()  // 确保释放资源
		if err != nil {
			return nil, nil, err
		}
		if shard != nil {
			// 在单个本地分片上执行向量搜索
			return i.singleLocalShardObjectVectorSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters,
				sort, groupBy, additionalProps, shard, targetCombination, properties)
		}
	}

	// 限制值为-1表示按距离搜索。在这种情况下，我们需要调整输出容量的计算方式
	var shardCap int
	if limit < 0 {
		// 按距离搜索时使用默认初始限制
		shardCap = len(readPlan.Shards()) * hnsw.DefaultSearchByDistInitialLimit
	} else {
		// 按数量限制搜索
		shardCap = len(readPlan.Shards()) * limit
	}

	// 创建错误组包装器用于并发处理
	eg := enterrors.NewErrorGroupWrapper(i.logger, "tenant:", tenant)
	// 在分数CPU环境中运行时，_NUMCPU为1
	// Weaviate的大多数云部署都在rf=3的HA集群中
	// 因此，我们应该将最大并发数至少设置为3
	// 这样单租户rf=3查询就不会被序列化。对于任何更高的_NUMCPU值，
	// 例如8，额外的goroutine不会产生显著开销（16 -> 17）
	eg.SetLimit(_NUMCPU*2 + 1)
	m := &sync.Mutex{}

	out := make([]*storobj.Object, 0, shardCap)
	dists := make([]float32, 0, shardCap)
	var localSearches atomic.Int64
	var localResponses atomic.Int64
	var remoteSearches atomic.Int64
	var remoteResponses atomic.Int64

	remoteSearch := func(shardName string) error {
		// If we have no local shard or if we force the query to reach all replicas
		remoteShardObject, remoteShardScores, err2 := i.remoteShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, tenant, shardName)
		if err2 != nil {
			return fmt.Errorf(
				"remote shard object search %s: %w", shardName, err2)
		}
		m.Lock()
		remoteResponses.Add(1)
		out = append(out, remoteShardObject...)
		dists = append(dists, remoteShardScores...)
		m.Unlock()
		return nil
	}
	localSearch := func(shardName string) error {
		shard, release, err := i.GetShard(ctx, shardName)
		defer release()
		if err != nil {
			return err
		}
		if shard != nil {
			localShardResult, localShardScores, err1 := i.localShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, tenant, shardName)
			if err1 != nil {
				return fmt.Errorf(
					"local shard object search %s: %w", shard.ID(), err1)
			}

			m.Lock()
			localResponses.Add(1)
			out = append(out, localShardResult...)
			dists = append(dists, localShardScores...)
			m.Unlock()
		} else {
			return remoteSearch(shardName)
		}

		return nil
	}

	err = executor.ExecuteForEachShard(readPlan,
		// Local Shard Search
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				localSearches.Add(1)
				return localSearch(shardName)
			}, shardName)
			return nil
		},
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				remoteSearches.Add(1)
				return remoteSearch(shardName)
			}, shardName)
			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing search for each shard: %w", err)
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	// If we are force querying all replicas, we need to run deduplication on the result.
	if i.Config.ForceFullReplicasSearch {
		if localSearches.Load() != localResponses.Load() {
			i.logger.Warnf("(in full replica search) local search count does not match local response count: searches=%d responses=%d", localSearches.Load(), localResponses.Load())
		}
		if remoteSearches.Load() != remoteResponses.Load() {
			i.logger.Warnf("(in full replica search) remote search count does not match remote response count: searches=%d responses=%d", remoteSearches.Load(), remoteResponses.Load())
		}
		out, dists, err = searchResultDedup(out, dists)
		if err != nil {
			return nil, nil, fmt.Errorf("could not deduplicate result after full replicas search: %w", err)
		}
	}

	if len(readPlan.Shards()) == 1 {
		return out, dists, nil
	}

	if len(readPlan.Shards()) > 1 && groupBy != nil {
		return i.mergeGroups(out, dists, groupBy, limit, len(readPlan.Shards()))
	}

	if len(readPlan.Shards()) > 1 && len(sort) > 0 {
		return i.sort(out, dists, sort, limit)
	}

	out, dists = newDistancesSorter().sort(out, dists)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		dists = dists[:limit]
	}

	if i.anyShardHasMultipleReplicasRead(tenant, readPlan.Shards()) {
		err = i.replicator.CheckConsistency(ctx, cl, out)
		if err != nil {
			i.logger.WithField("action", "object_vector_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return out, dists, nil
}

// IncomingSearch处理来自其他节点的搜索请求
// ctx: 上下文
// shardName: 分片名称
// searchVectors: 搜索向量数组
// targetVectors: 目标向量名称数组
// distance: 距离阈值
// limit: 结果数量限制
// filters: 过滤条件
// keywordRanking: 关键字排名
// sort: 排序条件
// cursor: 游标
// groupBy: 分组参数
// additional: 附加属性
// targetCombination: 目标组合策略
// properties: 属性列表
// 返回值: 对象数组、距离数组和可能的错误
func (i *Index) IncomingSearch(ctx context.Context, shardName string,
	searchVectors []models.Vector, targetVectors []string, distance float32, limit int,
	filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort, cursor *filters.Cursor, groupBy *searchparams.GroupBy,
	additional additional.Properties, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	ctx = helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(ctx, "is_coordinator", false)

	// Hacky fix here
	// shard.GetStatus() will force a lazy shard to load and we have usecases that rely on that behaviour that a search
	// will force a lazy loaded shard to load
	// However we also have cases (related to FORCE_FULL_REPLICAS_SEARCH) where we want to avoid waiting for a shard to
	// load, therefore we only call GetStatusNoLoad if replication is enabled -> another replica will be able to answer
	// the request and we want to exit early
	if i.replicationEnabled() && shard.GetStatus() == storagestate.StatusLoading {
		return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	} else {
		if shard.GetStatus() == storagestate.StatusLoading {
			// This effectively never happens with lazy loaded shard as GetStatus will wait for the lazy shard to load
			// and then status will never be "StatusLoading"
			return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
		}
	}

	if len(searchVectors) == 0 {
		res, scores, err := shard.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, additional, properties)
		if err != nil {
			return nil, nil, err
		}

		return res, scores, nil
	}

	res, resDists, err := shard.ObjectVectorSearch(
		ctx, searchVectors, targetVectors, distance, limit, filters, sort, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, resDists, nil
}

// deleteObject删除单个对象
// ctx: 上下文
// id: 对象UUID
// deletionTime: 删除时间
// replProps: 复制属性配置
// tenant: 租户名称
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID,
	deletionTime time.Time, replProps *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	if i.shardHasMultipleReplicasWrite(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.DeleteObject(ctx, shardName, id, deletionTime, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate deletion: shard=%q %w", shardName, err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.DeleteObject(ctx, shardName, id, deletionTime, schemaVersion); err != nil {
			return fmt.Errorf("delete remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)
	if err = shard.DeleteObject(ctx, id, deletionTime); err != nil {
		return fmt.Errorf("delete local object: shard=%q: %w", shardName, err)
	}
	return nil
}

// IncomingDeleteObject处理来自其他节点的删除对象请求
// ctx: 上下文
// shardName: 分片名称
// id: 对象UUID
// deletionTime: 删除时间
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) IncomingDeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.DeleteObject(ctx, id, deletionTime)
}

// IncomingDeleteObjectsExpired处理来自其他节点的过期对象删除请求
// eg: 错误组包装器
// ec: 错误合成器
// deleteOnPropName: 用于判断过期的属性名称
// ttlThreshold: TTL阈值时间
// deletionTime: 删除时间
// countDeleted: 删除计数回调函数
// schemaVersion: 模式版本号
func (i *Index) IncomingDeleteObjectsExpired(eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	// use closing context to stop long-running TTL deletions in case index is closed
	i.incomingDeleteObjectsExpired(i.closingCtx, eg, ec, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, schemaVersion)
}

// incomingDeleteObjectsExpired删除过期对象的内部实现
// ctx: 上下文
// eg: 错误组包装器
// ec: 错误合成器
// deleteOnPropName: 用于判断过期的属性名称
// ttlThreshold: TTL阈值时间
// deletionTime: 删除时间
// countDeleted: 删除计数回调函数
// schemaVersion: 模式版本号
func (i *Index) incomingDeleteObjectsExpired(ctx context.Context, eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	class := i.getClass()
	if err := ctx.Err(); err != nil {
		ec.AddGroups(err, class.Class)
		return
	}

	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLessThanEqual,
		Value: &filters.Value{
			Value: ttlThreshold,
			Type:  schema.DataTypeDate,
		},
		On: &filters.Path{
			Class:    schema.ClassName(class.Class),
			Property: schema.PropertyName(deleteOnPropName),
		},
	}}

	// the replication properties determine how aggressive the errors are returned and does not change anything about
	// the server's behaviour. Therefore, we set it to QUORUM to be able to log errors in case the deletion does not
	// succeed on too many nodes. In the case of errors a node might retain the object past its TTL. However, when the
	// deletion process happens to run on that node again, the object will be deleted then.
	replProps := defaultConsistency()

	if multitenancy.IsMultiTenant(class.MultiTenancyConfig) {
		tenants, err := i.schemaReader.Shards(class.Class)
		if err != nil {
			ec.AddGroups(fmt.Errorf("get tenants: %w", err), class.Class)
			return
		}

		for _, tenant := range tenants {
			eg.Go(func() error {
				processedBatches := 0
				// find uuids up to limit -> delete -> find uuids up to limit -> delete -> ... until no uuids left
				for {
					pauseEveryNoBatches := i.Config.ObjectsTTLPauseEveryNoBatches.Get()
					pauseDuration := i.Config.ObjectsTTLPauseDuration.Get()
					if pauseDuration > 0 && pauseEveryNoBatches > 0 && processedBatches >= pauseEveryNoBatches {
						timer := time.NewTimer(pauseDuration)
						t1 := time.Now()
						select {
						case t2 := <-timer.C:
							i.logger.WithFields(logrus.Fields{
								"action":     "objects_ttl_deletion",
								"collection": class.Class,
								"shard":      tenant,
							}).Debugf("paused for %s after processing %d batches", t2.Sub(t1), processedBatches)
							processedBatches = 0
						case <-ctx.Done():
							timer.Stop()
							ec.AddGroups(ctx.Err(), class.Class, tenant)
							return nil
						}
					}

					if err := ctx.Err(); err != nil {
						ec.AddGroups(err, class.Class, tenant)
						return nil
					}

					perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
					tenants2uuids, err := i.findUUIDs(ctx, filter, tenant, replProps, perShardLimit)
					if err != nil {
						// skip inactive tenants
						if !errors.Is(err, enterrors.ErrTenantNotActive) {
							ec.AddGroups(fmt.Errorf("find uuids: %w", err), class.Class, tenant)
						}
						return nil
					}

					if len(tenants2uuids[tenant]) == 0 {
						return nil
					}

					i.incomingDeleteObjectsExpiredUuids(ctx, ec, deletionTime, "", tenant,
						tenants2uuids[tenant], countDeleted, replProps, schemaVersion)

					processedBatches++
				}
			})
			if ctx.Err() != nil {
				break
			}
		}
		return
	}

	eg.Go(func() error {
		processedBatches := 0
		// find uuids up to limit -> delete -> find uuids up to limit -> delete -> ... until no uuids left
		for {
			pauseEveryNoBatches := i.Config.ObjectsTTLPauseEveryNoBatches.Get()
			pauseDuration := i.Config.ObjectsTTLPauseDuration.Get()
			if pauseDuration > 0 && pauseEveryNoBatches > 0 && processedBatches >= pauseEveryNoBatches {
				timer := time.NewTimer(pauseDuration)
				t1 := time.Now()
				select {
				case t2 := <-timer.C:
					i.logger.WithFields(logrus.Fields{
						"action":     "objects_ttl_deletion",
						"collection": class.Class,
					}).Debugf("paused for %s after processing %d batches", t2.Sub(t1), processedBatches)
					processedBatches = 0
				case <-ctx.Done():
					timer.Stop()
					ec.AddGroups(ctx.Err(), class.Class)
					return nil
				}
			}

			if err := ctx.Err(); err != nil {
				ec.AddGroups(err, class.Class)
				return nil
			}

			perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
			shards2uuids, err := i.findUUIDs(ctx, filter, "", replProps, perShardLimit)
			if err != nil {
				ec.AddGroups(fmt.Errorf("find uuids: %w", err), class.Class)
				return nil
			}

			shardIdx := len(shards2uuids) - 1
			anyUuidsFound := false
			wg := new(sync.WaitGroup)
			f := func(shard string, uuids []strfmt.UUID) {
				defer wg.Done()
				i.incomingDeleteObjectsExpiredUuids(ctx, ec, deletionTime, shard, "",
					uuids, countDeleted, replProps, schemaVersion)
			}

			for shard, uuids := range shards2uuids {
				shardIdx--
				if len(uuids) == 0 {
					continue
				}

				anyUuidsFound = true
				wg.Add(1)
				isLastShard := shardIdx == 0
				// if possible run in separate routine, if not run in current one
				// always run last in current one (not to start other routine,
				// while current one have to wait for the results anyway)
				if isLastShard || !eg.TryGo(func() error {
					f(shard, uuids)
					return nil
				}) {
					f(shard, uuids)
				}
				if ctx.Err() != nil {
					return nil
				}
			}
			wg.Wait()

			if !anyUuidsFound {
				return nil
			}

			processedBatches++
		}
	})
}

// incomingDeleteObjectsExpiredUuids删除指定UUID的过期对象
// ctx: 上下文
// ec: 错误合成器
// deletionTime: 删除时间
// shard: 分片名称
// tenant: 租户名称
// uuids: 待删除的UUID数组
// countDeleted: 删除计数回调函数
// replProps: 复制属性配置
// schemaVersion: 模式版本号
func (i *Index) incomingDeleteObjectsExpiredUuids(ctx context.Context, ec errorcompounder.ErrorCompounder,
	deletionTime time.Time, shard, tenant string, uuids []strfmt.UUID, countDeleted func(int32),
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) {
	if len(uuids) == 0 {
		return
	}

	inputKey := shard
	if tenant != "" {
		inputKey = tenant
	}
	collection := i.Config.ClassName.String()
	maxErrors := 3

	logger := i.logger.WithFields(logrus.Fields{
		"action":     "objects_ttl_deletion",
		"collection": collection,
		"shard":      inputKey,
	})

	f := func() (err error) {
		started := time.Now()
		deleted := int32(0)

		logger.WithFields(logrus.Fields{
			"size": len(uuids),
		}).Debug("batch delete started")
		defer func() {
			logger := logger.WithFields(logrus.Fields{
				"took":    time.Since(started),
				"deleted": deleted,
				"failed":  int32(len(uuids)) - deleted,
			})
			if err != nil {
				// as debug for each batch, combined error of all batches is logged as error anyway
				logger.WithError(err).Debug("batch delete failed")
				return
			}
			logger.Debug("batch delete finished")
		}()

		input := map[string][]strfmt.UUID{inputKey: uuids}
		resp, err := i.batchDeleteObjects(ctx, input, deletionTime, false, replProps, schemaVersion, tenant)
		if err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}

		errsCount := 0
		ecBatch := errorcompounder.New()
		for i := range resp {
			if err := resp[i].Err; err != nil {
				// limit number of returned errors to [maxErrors]
				if errsCount < maxErrors {
					errsCount++
					ecBatch.Add(fmt.Errorf("%s: %w", resp[i].UUID, err))
				}
			} else {
				deleted++
			}
		}
		countDeleted(deleted)

		if err := ecBatch.ToError(); err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}
		return nil
	}

	ec.AddGroups(f(), collection, inputKey)
}

// getClass获取当前索引对应的类定义
// 返回值: 类模型指针
func (i *Index) getClass() *models.Class {
	className := i.Config.ClassName.String()
	return i.getSchema.ReadOnlyClass(className)
}

// Intended to run on "receiver" nodes, where local shard
// is expected to exist and be active
// Method first tries to get shard from Index::shards map,
// or inits shard and adds it to the map if shard was not found
// initLocalShard初始化本地分片
// 用于"接收者"节点，期望本地分片存在且处于活跃状态
// 该方法首先尝试从Index::shards映射中获取分片，
// 如果未找到分片，则初始化并添加到映射中
// ctx: 上下文
// shardName: 分片名称
// 返回值: 可能的错误
func (i *Index) initLocalShard(ctx context.Context, shardName string) error {
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, false, false)
}

// LoadLocalShard加载本地分片
// ctx: 上下文
// shardName: 分片名称
// implicitShardLoading: 是否隐式加载分片
// 返回值: 可能的错误
func (i *Index) LoadLocalShard(ctx context.Context, shardName string, implicitShardLoading bool) error {
	// TODO: implicitShardLoading needs to be double checked if needed at all
	// consalidate mustLoad and implicitShardLoading
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, true, implicitShardLoading)
}

// initLocalShardWithForcedLoading强制加载方式初始化本地分片
// ctx: 上下文
// class: 类模型
// shardName: 分片名称
// mustLoad: 是否必须加载
// implicitShardLoading: 是否隐式加载
// 返回值: 可能的错误
func (i *Index) initLocalShardWithForcedLoading(ctx context.Context, class *models.Class, shardName string, mustLoad bool, implicitShardLoading bool) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	// make sure same shard is not inited in parallel
	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	// check if created in the meantime by concurrent call
	if shard := i.shards.Load(shardName); shard != nil {
		if mustLoad {
			lazyShard, ok := shard.(*LazyLoadShard)
			if ok {
				return lazyShard.Load(ctx)
			}
		}

		return nil
	}

	disableLazyLoad := mustLoad || i.Config.DisableLazyLoadShards

	shard, err := i.initShard(ctx, shardName, class, i.metrics.baseMetrics, disableLazyLoad, implicitShardLoading)
	if err != nil {
		return err
	}

	i.shards.Store(shardName, shard)

	return nil
}

// UnloadLocalShard卸载本地分片
// ctx: 上下文
// shardName: 分片名称
// 返回值: 可能的错误
func (i *Index) UnloadLocalShard(ctx context.Context, shardName string) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	shardLike, ok := i.shards.LoadAndDelete(shardName)
	if !ok {
		return nil // shard was not found, nothing to unload
	}

	if err := shardLike.Shutdown(ctx); err != nil {
		if !errors.Is(err, errAlreadyShutdown) {
			return errors.Wrapf(err, "shutdown shard %q", shardName)
		}
		return errors.Wrapf(errAlreadyShutdown, "shutdown shard %q", shardName)
	}

	return nil
}

// GetShard获取指定的分片（不会强制初始化）
// ctx: 上下文
// shardName: 分片名称
// 返回值: 分片对象、释放函数和可能的错误
func (i *Index) GetShard(ctx context.Context, shardName string) (
	shard ShardLike, release func(), err error,
) {
	return i.getOptInitLocalShard(ctx, shardName, false)
}

// getOrInitShard获取或初始化指定的分片
// ctx: 上下文
// shardName: 分片名称
// 返回值: 分片对象、释放函数和可能的错误
func (i *Index) getOrInitShard(ctx context.Context, shardName string) (
	shard ShardLike, release func(), err error,
) {
	return i.getOptInitLocalShard(ctx, shardName, true)
}

// getOptInitLocalShard返回指定名称的本地分片
// 如果ensureInit设置为true，则确保返回的实例是完全加载的分片
// 如果分片尚未初始化，返回的分片可能是懒加载分片实例或nil
// 在调用release之前，返回的分片不能被关闭
// ctx: 上下文
// shardName: 分片名称
// ensureInit: 是否确保初始化
// 返回值: 分片对象、释放函数和可能的错误
func (i *Index) getOptInitLocalShard(ctx context.Context, shardName string, ensureInit bool) (
	shard ShardLike, release func(), err error,
) {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return nil, func() {}, errAlreadyShutdown
	}

	// make sure same shard is not inited in parallel. In case it is not loaded yet, switch to a RW lock and initialize
	// the shard
	i.shardCreateLocks.RLock(shardName)

	// check if created in the meantime by concurrent call
	shard = i.shards.Load(shardName)
	if shard == nil {
		// If the shard is not yet loaded, we need to upgrade to a write lock to ensure only one goroutine initializes
		// the shard
		i.shardCreateLocks.RUnlock(shardName)
		if !ensureInit {
			return nil, func() {}, nil
		}

		className := i.Config.ClassName.String()
		class := i.getSchema.ReadOnlyClass(className)
		if class == nil {
			return nil, func() {}, fmt.Errorf("class %s not found in schema", className)
		}

		i.shardCreateLocks.Lock(shardName)
		defer i.shardCreateLocks.Unlock(shardName)

		// double check if loaded in the meantime by concurrent call, if not load it
		shard = i.shards.Load(shardName)
		if shard == nil {
			shard, err = i.initShard(ctx, shardName, class, i.metrics.baseMetrics, true, false)
			if err != nil {
				return nil, func() {}, err
			}
			i.shards.Store(shardName, shard)
		}
	} else {
		// shard already loaded, so we can defer the Runlock
		defer i.shardCreateLocks.RUnlock(shardName)
	}

	release, err = shard.preventShutdown()
	if err != nil {
		return nil, func() {}, fmt.Errorf("get/init local shard %q, no shutdown: %w", shardName, err)
	}

	return shard, release, nil
}

// mergeObject合并更新对象
// ctx: 上下文
// merge: 合并文档
// replProps: 复制属性配置
// tenant: 租户名称
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument,
	replProps *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, merge.ID, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	if i.shardHasMultipleReplicasWrite(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.MergeObject(ctx, shardName, &merge, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate single update: %w", err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.MergeObject(ctx, shardName, merge, schemaVersion); err != nil {
			return fmt.Errorf("update remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)
	if err = shard.MergeObject(ctx, merge); err != nil {
		return fmt.Errorf("update local object: shard=%q: %w", shardName, err)
	}

	return nil
}

// IncomingMergeObject处理来自其他节点的合并对象请求
// ctx: 上下文
// shardName: 分片名称
// mergeDoc: 合并文档
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) IncomingMergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.MergeObject(ctx, mergeDoc)
}

// aggregate执行聚合操作
// ctx: 上下文
// replProps: 复制属性配置
// params: 聚合参数
// modules: 模块提供者
// tenant: 租户名称
// 返回值: 聚合结果和可能的错误
func (i *Index) aggregate(ctx context.Context, replProps *additional.ReplicationProperties,
	params aggregation.Params, modules *modules.Provider, tenant string,
) (*aggregation.Result, error) {
	cl := i.consistencyLevel(replProps)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, err
	}

	results := make([]*aggregation.Result, len(readPlan.Shards()))
	for j, shardName := range readPlan.Shards() {
		var err error
		var res *aggregation.Result

		var shard ShardLike
		var release func()
		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					res, err = shard.Aggregate(ctx, params, modules)
				} else {
					res, err = i.remote.Aggregate(ctx, shardName, params)
				}
			}
		}()

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		results[j] = res
	}

	return aggregator.NewShardCombiner().Do(results), nil
}

// IncomingAggregate处理来自其他节点的聚合请求
// ctx: 上下文
// shardName: 分片名称
// params: 聚合参数
// mods: 模块提供者接口
// 返回值: 聚合结果和可能的错误
func (i *Index) IncomingAggregate(ctx context.Context, shardName string,
	params aggregation.Params, mods interface{},
) (*aggregation.Result, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	// 在分片上执行聚合操作
	return shard.Aggregate(ctx, params, mods.(*modules.Provider))
}

// drop删除整个索引及其所有分片
// 返回值: 可能的错误
func (i *Index) drop() error {
	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	// 检查是否有备份正在进行。在这种情况下不要删除文件，以便备份过程可以成功完成
	// 文件将在备份完成后删除，或者在下次启动时发生崩溃的情况下删除。
	lastBackup := i.lastBackup.Load()
	keepFiles := lastBackup != nil  // 如果有备份则保留文件

	// 创建错误组用于并发删除分片
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)  // 设置并发限制
	fields := logrus.Fields{"action": "drop_shard", "class": i.Config.ClassName}  // 日志字段
	// 定义删除分片的函数
	dropShard := func(shardName string, _ ShardLike) error {
		eg.Go(func() error {
			// 获取备份读锁以防止在备份过程中删除
			i.backupLock.RLock(shardName)
			defer i.backupLock.RUnlock(shardName)

			// 获取分片创建写锁以防止并发修改
			i.shardCreateLocks.Lock(shardName)
			defer i.shardCreateLocks.Unlock(shardName)

			// 加载并删除分片
			shard, ok := i.shards.LoadAndDelete(shardName)
			if !ok {
				return nil // 分片已不存在
			}
			if err := shard.drop(keepFiles); err != nil {
				logrus.WithFields(fields).WithField("id", shard.ID()).Error(err)
			}

			return nil
		})
		return nil
	}

	i.shards.Range(dropShard)
	if err := eg.Wait(); err != nil {
		return err
	}

	// Dropping the shards only unregisters the shards callbacks, but we still
	// need to stop the cycle managers that those shards used to register with.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	i.logger.WithFields(logrus.Fields{
		"action":   "drop_index",
		"duration": 60 * time.Second,
	}).Debug("context.WithTimeout")

	if err := i.stopCycleManagers(ctx, "drop"); err != nil {
		return err
	}

	if !keepFiles {
		return os.RemoveAll(i.path())
	} else {
		return os.Rename(i.path(), filepath.Join(i.Config.RootPath, backup.DeleteMarkerAdd(i.ID())))
	}
}

// dropShards删除多个分片
// names: 分片名称数组
// 返回值: 可能的错误
func (i *Index) dropShards(names []string) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	ec := errorcompounder.New()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range names {
		name := name
		eg.Go(func() error {
			i.backupLock.RLock(name)
			defer i.backupLock.RUnlock(name)
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			shard, ok := i.shards.LoadAndDelete(name)
			if !ok {
				// Ensure that if the shard is not loaded we delete any reference on disk for any data.
				// This ensures that we also delete inactive shards/tenants
				if err := os.RemoveAll(shardPath(i.path(), name)); err != nil {
					ec.Add(err)
					i.logger.WithField("action", "drop_shard").WithField("shard", shard.ID()).Error(err)
				}
			} else {
				// If shard is loaded use the native primitive to drop it
				if err := shard.drop(false); err != nil {
					ec.Add(err)
					i.logger.WithField("action", "drop_shard").WithField("shard", shard.ID()).Error(err)
				}
			}

			return nil
		})
	}

	eg.Wait()
	return ec.ToError()
}

// dropCloudShards删除云端分片
// ctx: 上下文
// cloud: 云端卸载接口
// names: 分片名称数组
// nodeId: 节点ID
// 返回值: 可能的错误
func (i *Index) dropCloudShards(ctx context.Context, cloud modulecapabilities.OffloadCloud, names []string, nodeId string) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	ec := errorcompounder.New()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range names {
		eg.Go(func() error {
			i.backupLock.RLock(name)
			defer i.backupLock.RUnlock(name)
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			if err := cloud.Delete(ctx, i.ID(), name, nodeId); err != nil {
				ec.Add(err)
				i.logger.WithField("action", "cloud_drop_shard").
					WithField("shard", name).Error(err)
			}
			return nil
		})
	}

	eg.Wait()
	return ec.ToError()
}

// Shutdown关闭索引及其所有分片
// ctx: 上下文
// 返回值: 可能的错误
func (i *Index) Shutdown(ctx context.Context) error {
	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	// TODO allow every resource cleanup to run, before returning early with error
	if err := i.shards.RangeConcurrently(i.logger, func(name string, shard ShardLike) error {
		i.backupLock.RLock(name)
		defer i.backupLock.RUnlock(name)

		if err := shard.Shutdown(ctx); err != nil {
			if !errors.Is(err, errAlreadyShutdown) {
				return errors.Wrapf(err, "shutdown shard %q", name)
			}
			i.logger.WithField("shard", shard.Name()).Debug("was already shut or dropped")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := i.stopCycleManagers(ctx, "shutdown"); err != nil {
		return err
	}

	return nil
}

// stopCycleManagers停止所有周期管理器
// ctx: 上下文
// usecase: 用例描述（如"drop"或"shutdown"）
// 返回值: 可能的错误
func (i *Index) stopCycleManagers(ctx context.Context, usecase string) error {
	if err := i.cycleCallbacks.compactionCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop objects compaction cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.compactionAuxCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop non objects compaction cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.flushCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop flush cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.vectorCommitLoggerCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop vector commit logger cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.vectorTombstoneCleanupCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop vector tombstone cleanup cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.geoPropsCommitLoggerCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop geo props commit logger cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.geoPropsTombstoneCleanupCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop geo props tombstone cleanup cycle: %w", usecase, err)
	}
	return nil
}

// getShardsQueueSize获取分片队列大小
// ctx: 上下文
// tenant: 租户名称
// 返回值: 分片名称到队列大小的映射和可能的错误
func (i *Index) getShardsQueueSize(ctx context.Context, tenant string) (map[string]int64, error) {
	className := i.Config.ClassName.String()
	shardNames, err := i.schemaReader.Shards(className)
	if err != nil {
		return nil, err
	}

	shardsQueueSize := make(map[string]int64)
	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var size int64
		var shard ShardLike
		var release func()

		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
						size += queue.Size()
						return nil
					})
				} else {
					size, err = i.remote.GetShardQueueSize(ctx, shardName)
				}
			}
		}()

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		shardsQueueSize[shardName] = size
	}

	return shardsQueueSize, nil
}

// IncomingGetShardQueueSize处理来自其他节点的获取分片队列大小请求
// ctx: 上下文
// shardName: 分片名称
// 返回值: 队列大小和可能的错误
func (i *Index) IncomingGetShardQueueSize(ctx context.Context, shardName string) (int64, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return 0, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return 0, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}
	var size int64
	_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
		size += queue.Size()
		return nil
	})
	return size, nil
}

// getShardsStatus获取所有分片的状态
// ctx: 上下文
// tenant: 租户名称
// 返回值: 分片名称到状态的映射和可能的错误
func (i *Index) getShardsStatus(ctx context.Context, tenant string) (map[string]string, error) {
	className := i.Config.ClassName.String()
	shardNames, err := i.schemaReader.Shards(className)
	if err != nil {
		return nil, err
	}

	shardsStatus := make(map[string]string)

	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var status string
		var shard ShardLike
		var release func()

		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					status = shard.GetStatus().String()
				} else {
					status, err = i.remote.GetShardStatus(ctx, shardName)
				}
			}
		}()

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		shardsStatus[shardName] = status
	}

	return shardsStatus, nil
}

// IncomingGetShardStatus处理来自其他节点的获取分片状态请求
// ctx: 上下文
// shardName: 分片名称
// 返回值: 分片状态字符串和可能的错误
func (i *Index) IncomingGetShardStatus(ctx context.Context, shardName string) (string, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return "", err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return "", enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}
	return shard.GetStatus().String(), nil
}

// updateShardStatus更新分片状态
// ctx: 上下文
// tenantName: 租户名称
// shardName: 分片名称
// targetStatus: 目标状态
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) updateShardStatus(ctx context.Context, tenantName, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
	if err != nil {
		return err
	}
	if shard == nil {
		return i.remote.UpdateShardStatus(ctx, shardName, targetStatus, schemaVersion)
	}
	defer release()
	return shard.UpdateStatus(targetStatus, "manually set by user")
}

// IncomingUpdateShardStatus处理来自其他节点的更新分片状态请求
// ctx: 上下文
// shardName: 分片名称
// targetStatus: 目标状态
// schemaVersion: 模式版本号
// 返回值: 可能的错误
func (i *Index) IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.UpdateStatus(targetStatus, "manually set by user")
}

// findUUIDs根据过滤条件查找UUID
// ctx: 上下文
// filters: 本地过滤条件
// tenant: 租户名称
// repl: 复制属性配置
// perShardLimit: 每个分片的限制数量
// 返回值: 分片名称到UUID数组的映射和可能的错误
func (i *Index) findUUIDs(ctx context.Context,
	filters *filters.LocalFilter, tenant string, repl *additional.ReplicationProperties,
	perShardLimit int,
) (map[string][]strfmt.UUID, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "filter_total")
	cl := i.consistencyLevel(repl)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, err
	}
	className := i.Config.ClassName.String()

	results := make(map[string][]strfmt.UUID)
	for _, shardName := range readPlan.Shards() {
		var shard ShardLike
		var release func()
		var err error

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			results[shardName], err = i.replicator.FindUUIDs(ctx, className, shardName, filters, cl, perShardLimit)
		} else {
			// anonymous func is here to ensure release is executed after each loop iteration
			func() {
				shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
				defer release()
				if err == nil {
					if shard != nil {
						results[shardName], err = shard.FindUUIDs(ctx, filters, perShardLimit)
					} else {
						results[shardName], err = i.remote.FindUUIDs(ctx, shardName, filters, perShardLimit)
					}
				}
			}()
		}

		if err != nil {
			return nil, fmt.Errorf("find matching doc ids in shard %q: %w", shardName, err)
		}
	}

	return results, nil
}

// consistencyLevel returns the consistency level for the given replication properties.
// If repl is not nil, the consistency level is returned from repl.
// If repl is nil and a default override is provided, the default override is returned.
// If repl is nil and no default override is provided, the default consistency level
// is returned (QUORUM).
// consistencyLevel返回给定复制属性的一致性级别
// 如果repl不为nil，则从repl返回一致性级别
// 如果repl为nil且提供了默认覆盖，则返回默认覆盖
// 如果repl为nil且未提供默认覆盖，则返回默认一致性级别（QUORUM）
// repl: 复制属性配置
// defaultOverride: 可选的默认一致性级别覆盖
// 返回值: 一致性级别
func (i *Index) consistencyLevel(
	repl *additional.ReplicationProperties,
	defaultOverride ...routerTypes.ConsistencyLevel,
) routerTypes.ConsistencyLevel {
	if repl == nil {
		repl = defaultConsistency(defaultOverride...)
	}
	return routerTypes.ConsistencyLevel(repl.ConsistencyLevel)
}

// IncomingFindUUIDs处理来自其他节点的查找UUID请求
// ctx: 上下文
// shardName: 分片名称
// filters: 本地过滤条件
// limit: 限制数量
// 返回值: UUID数组和可能的错误
func (i *Index) IncomingFindUUIDs(ctx context.Context, shardName string,
	filters *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.FindUUIDs(ctx, filters, limit)
}

// batchDeleteObjects批量删除对象
// ctx: 上下文
// shardUUIDs: 分片名称到UUID数组的映射
// deletionTime: 删除时间
// dryRun: 是否为模拟运行
// replProps: 复制属性配置
// schemaVersion: 模式版本号
// tenant: 租户名称
// 返回值: 批量简单对象和可能的错误
func (i *Index) batchDeleteObjects(ctx context.Context, shardUUIDs map[string][]strfmt.UUID,
	deletionTime time.Time, dryRun bool, replProps *additional.ReplicationProperties, schemaVersion uint64,
	tenant string,
) (objects.BatchSimpleObjects, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "delete_from_shards_total")

	type result struct {
		objs objects.BatchSimpleObjects
	}

	if replProps == nil {
		replProps = defaultConsistency()
	}

	wg := &sync.WaitGroup{}
	ch := make(chan result, len(shardUUIDs))
	for shardName, uuids := range shardUUIDs {
		uuids := uuids
		shardName := shardName
		wg.Add(1)
		f := func() {
			defer wg.Done()

			var objs objects.BatchSimpleObjects
			if i.shardHasMultipleReplicasWrite(tenant, shardName) {
				objs = i.replicator.DeleteObjects(ctx, shardName, uuids, deletionTime,
					dryRun, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
				defer release()
				if err != nil {
					objs = objects.BatchSimpleObjects{
						objects.BatchSimpleObject{Err: err},
					}
				} else if shard != nil {
					func() {
						i.backupLock.RLock(shardName)
						defer i.backupLock.RUnlock(shardName)
						defer release()
						objs = shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
					}()
				} else {
					objs = i.remote.DeleteObjectBatch(ctx, shardName, uuids, deletionTime, dryRun, schemaVersion)
				}
			}

			ch <- result{objs}
		}
		enterrors.GoWrapper(f, i.logger)
	}

	wg.Wait()
	close(ch)

	var out objects.BatchSimpleObjects
	for res := range ch {
		out = append(out, res.objs...)
	}

	return out, nil
}

// IncomingDeleteObjectBatch处理来自其他节点的批量删除对象请求
// ctx: 上下文
// shardName: 分片名称
// uuids: UUID数组
// deletionTime: 删除时间
// dryRun: 是否为模拟运行
// schemaVersion: 模式版本号
// 返回值: 批量简单对象
func (i *Index) IncomingDeleteObjectBatch(ctx context.Context, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	defer release()

	return shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
}

// defaultConsistency创建默认一致性级别配置
// defaultOverride: 可选的默认一致性级别覆盖
// 返回值: 复制属性配置
func defaultConsistency(defaultOverride ...routerTypes.ConsistencyLevel) *additional.ReplicationProperties {
	rp := &additional.ReplicationProperties{}
	if len(defaultOverride) != 0 {
		rp.ConsistencyLevel = string(defaultOverride[0])
	} else {
		rp.ConsistencyLevel = string(routerTypes.ConsistencyLevelQuorum)
	}
	return rp
}

// objectSearchPreallocate为对象搜索预分配内存
// limit: 结果数量限制
// shards: 分片名称数组
// 返回值: 预分配的对象数组和分数数组
func objectSearchPreallocate(limit int, shards []string) ([]*storobj.Object, []float32) {
	perShardLimit := config.DefaultQueryMaximumResults
	if perShardLimit > int64(limit) {
		perShardLimit = int64(limit)
	}
	capacity := perShardLimit * int64(len(shards))
	objects := make([]*storobj.Object, 0, capacity)
	scores := make([]float32, 0, capacity)

	return objects, scores
}

// GetVectorIndexConfig returns a vector index configuration associated with targetVector.
// In case targetVector is empty string, legacy vector configuration is returned.
// Method expects that configuration associated with targetVector is present.
// GetVectorIndexConfig返回与targetVector关联的向量索引配置
// 如果targetVector为空字符串，则返回传统向量配置
// 该方法期望与targetVector关联的配置存在
// targetVector: 目标向量名称
// 返回值: 向量索引配置
func (i *Index) GetVectorIndexConfig(targetVector string) schemaConfig.VectorIndexConfig {
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	if targetVector == "" {
		return i.vectorIndexUserConfig
	}

	return i.vectorIndexUserConfigs[targetVector]
}

// GetVectorIndexConfigs returns a map of vector index configurations.
// If present, legacy vector is return under the key of empty string.
// GetVectorIndexConfigs返回向量索引配置的映射
// 如果存在，传统向量在空字符串键下返回
// 返回值: 向量名称到向量索引配置的映射
func (i *Index) GetVectorIndexConfigs() map[string]schemaConfig.VectorIndexConfig {
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	configs := make(map[string]schemaConfig.VectorIndexConfig, len(i.vectorIndexUserConfigs)+1)
	for k, v := range i.vectorIndexUserConfigs {
		configs[k] = v
	}

	if i.vectorIndexUserConfig != nil {
		configs[""] = i.vectorIndexUserConfig
	}

	return configs
}

// convertToVectorIndexConfig将接口转换为向量索引配置
// config: 配置接口
// 返回值: 向量索引配置或nil
func convertToVectorIndexConfig(config interface{}) schemaConfig.VectorIndexConfig {
	if config == nil {
		return nil
	}
	// in case legacy vector config was set as an empty map/object instead of nil
	if empty, ok := config.(map[string]interface{}); ok && len(empty) == 0 {
		return nil
	}
	// Safe type assertion
	if vectorIndexConfig, ok := config.(schemaConfig.VectorIndexConfig); ok {
		return vectorIndexConfig
	}
	return nil
}

// convertToVectorIndexConfigs将向量配置映射转换为向量索引配置映射
// configs: 向量配置映射
// 返回值: 向量索引配置映射或nil
func convertToVectorIndexConfigs(configs map[string]models.VectorConfig) map[string]schemaConfig.VectorIndexConfig {
	if len(configs) > 0 {
		vectorIndexConfigs := make(map[string]schemaConfig.VectorIndexConfig)
		for targetVector, vectorConfig := range configs {
			if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
				vectorIndexConfigs[targetVector] = vectorIndexConfig
			}
		}
		return vectorIndexConfigs
	}
	return nil
}

// IMPORTANT:
// DebugResetVectorIndex is intended to be used for debugging purposes only.
// It drops the selected vector index, creates a new one, then reindexes it in the background.
// This function assumes the node is not receiving any traffic besides the
// debug endpoints and that async indexing is enabled.
// DebugResetVectorIndex用于调试目的，删除选定的向量索引，创建新索引，然后在后台重新索引
// 此函数假设节点除了调试端点外不接收任何流量，并且启用了异步索引
// ctx: 上下文
// shardName: 分片名称
// targetVector: 目标向量名称
// 返回值: 可能的错误
func (i *Index) DebugResetVectorIndex(ctx context.Context, shardName, targetVector string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return errors.New("shard not found")
	}
	defer release()

	// Get the vector index
	vidx, ok := shard.GetVectorIndex(targetVector)
	if !ok {
		return errors.New("vector index not found")
	}

	if !hnsw.IsHNSWIndex(vidx) {
		return errors.New("vector index is not hnsw")
	}

	// Reset the vector index
	err = shard.DebugResetVectorIndex(ctx, targetVector)
	if err != nil {
		return errors.Wrap(err, "failed to reset vector index")
	}

	// Reindex in the background
	enterrors.GoWrapper(func() {
		err = shard.FillQueue(targetVector, 0)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to reindex vector index")
			return
		}
	}, i.logger)

	return nil
}

// DebugRepairIndex修复向量索引（用于调试）
// ctx: 上下文
// shardName: 分片名称
// targetVector: 目标向量名称
// 返回值: 可能的错误
func (i *Index) DebugRepairIndex(ctx context.Context, shardName, targetVector string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return errors.New("shard not found")
	}
	defer release()

	// Repair in the background
	enterrors.GoWrapper(func() {
		err := shard.RepairIndex(context.Background(), targetVector)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to repair vector index")
			return
		}
	}, i.logger)

	return nil
}

// tenantDirExists检查租户目录是否存在
// tenantName: 租户名称
// 返回值: 是否存在和可能的错误
func (i *Index) tenantDirExists(tenantName string) (bool, error) {
	tenantPath := shardPath(i.path(), tenantName)
	if _, err := os.Stat(tenantPath); err != nil {
		// when inactive tenant is not populated, its directory does not exist yet
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// buildReadRoutingPlan构建读取路由计划
// cl: 一致性级别
// tenantName: 租户名称
// 返回值: 读取路由计划和可能的错误
func (i *Index) buildReadRoutingPlan(cl routerTypes.ConsistencyLevel, tenantName string) (routerTypes.ReadRoutingPlan, error) {
	planOptions := routerTypes.RoutingPlanBuildOptions{
		Tenant:           tenantName,
		ConsistencyLevel: cl,
	}
	readPlan, err := i.router.BuildReadRoutingPlan(planOptions)
	if err != nil {
		return routerTypes.ReadRoutingPlan{}, err
	}

	return readPlan, nil
}

// DebugRequantizeIndex重新量化向量索引（用于调试）
// ctx: 上下文
// shardName: 分片名称
// targetVector: 目标向量名称
// 返回值: 可能的错误
func (i *Index) DebugRequantizeIndex(ctx context.Context, shardName, targetVector string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return errors.New("shard not found")
	}
	defer release()

	// Repair in the background
	enterrors.GoWrapper(func() {
		err := shard.RequantizeIndex(context.Background(), targetVector)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to requantize vector index")
			return
		}
	}, i.logger)

	return nil
}
